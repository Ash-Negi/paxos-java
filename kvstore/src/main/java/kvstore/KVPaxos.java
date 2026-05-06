package kvstore;

import paxos.Paxos;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

// Replicated key/value store, layered on top of paxos-core.
//
// Java port of src/kvpaxos/server.go (MIT 6.824). Each KVPaxos instance owns
// one paxos.Paxos peer plus its own TCP listener on a separate port that
// clients (Clerks) talk to. The set of kv addresses and the set of paxos
// addresses are independent, but conventionally have the same length and the
// same `me` index.
//
// Concurrency model: Get/Put grab `mu` and hold it through the entire paxos
// agreement + apply. Concurrent client RPCs to the same server are therefore
// serialized — simple and correct, mirrors the Go reference.
public class KVPaxos {

    // ---- State (guarded by mu unless marked volatile) -----------------------

    private final ReentrantLock mu = new ReentrantLock();
    private final int me;
    private volatile boolean dead = false;
    private volatile boolean unreliable = false;

    private final Paxos px;

    // Highest paxos seq we've already applied to `data`.
    private int globalSeq = 0;
    private final Map<String, String> data            = new HashMap<>();
    // For Put idempotency: requestID -> previous value (the one returned to
    // the client). Get is naturally idempotent and isn't tracked here.
    private final Map<Long, String>   visitedRequests = new HashMap<>();

    // ---- Networking ---------------------------------------------------------

    private final ServerSocket    listener;
    private final Thread          listenerThread;
    private final ExecutorService rpcPool = Executors.newCachedThreadPool();
    private final Random          rand    = new Random();

    public KVPaxos(String[] kvAddrs, String[] paxosAddrs, int me) throws IOException {
        this.me = me;
        this.px = new Paxos(paxosAddrs, me);

        int port = portOf(kvAddrs[me]);
        this.listener = new ServerSocket(port);

        this.listenerThread = new Thread(this::runListener, "kvpaxos-listener-" + me);
        this.listenerThread.setDaemon(true);
        this.listenerThread.start();
    }

    // -----------------------------------------------------------------------
    // RPC handlers — mirror server.go Get / Put
    // -----------------------------------------------------------------------

    public Messages.GetReply get(Messages.GetArgs args) {
        mu.lock();
        try {
            Op op = new Op("Get", args.key, "", args.requestID, false);
            String[] r = makeAgreementAndApplyChange(op);
            Messages.GetReply reply = new Messages.GetReply();
            reply.err   = r[0];
            reply.value = r[1];
            return reply;
        } finally {
            mu.unlock();
        }
    }

    public Messages.PutReply put(Messages.PutArgs args) {
        mu.lock();
        try {
            Op op = new Op("Put", args.key, args.value, args.requestID, args.doHash);
            String[] r = makeAgreementAndApplyChange(op);
            Messages.PutReply reply = new Messages.PutReply();
            reply.err           = r[0];
            reply.previousValue = r[1];
            return reply;
        } finally {
            mu.unlock();
        }
    }

    // -----------------------------------------------------------------------
    // Core: get our op into the paxos log, catch up, then apply
    // -----------------------------------------------------------------------

    // Returns {err, value}. Caller must hold mu.
    private String[] makeAgreementAndApplyChange(Op op) {
        int seq = globalSeq + 1;

        // Keep proposing at increasing seq numbers until paxos agrees on OUR op.
        while (!dead) {
            px.start(seq, op);
            Object agreedV = waitForAgreement(seq);
            if (agreedV == null) return new String[]{"", ""}; // killed
            if (!op.equals(agreedV)) {
                seq++;
                continue;
            }
            break;
        }

        // Catch up: apply any decisions in (globalSeq, seq) that landed while
        // we were retrying. Those slots were filled by other clients/servers.
        for (int idx = globalSeq + 1; idx < seq; idx++) {
            Object v = waitForAgreement(idx);
            if (v == null) return new String[]{"", ""};
            applyChange((Op) v);
        }

        String[] result = applyChange(op);
        globalSeq = seq;
        px.done(seq);
        return result;
    }

    // Poll paxos.status() until seq is decided. Returns null if killed.
    private Object waitForAgreement(int seq) {
        while (!dead) {
            Paxos.StatusResult r = px.status(seq);
            if (r.decided()) return r.value();
            try { Thread.sleep(20); } catch (InterruptedException e) { return null; }
        }
        return null;
    }

    // Apply op to local state. Caller must hold mu.
    // Returns {err, value-to-return-to-client}.
    private String[] applyChange(Op op) {
        if ("Put".equals(op.operation())) {
            // Idempotency: if we've already applied this requestID, return the
            // value we returned the first time. This matches what the client
            // would have seen had no retry been necessary.
            String prev = visitedRequests.get(op.requestID());
            if (prev != null) {
                return new String[]{Messages.OK, prev};
            }

            String oldValue = data.getOrDefault(op.key(), "");
            String newValue = op.doHash()
                    ? Long.toString(Messages.fnv1a32(oldValue + op.value()))
                    : op.value();
            data.put(op.key(), newValue);

            // Only PutHash needs the old value; for plain Put discard it to
            // save memory in the dedup map.
            String stored = op.doHash() ? oldValue : "";
            visitedRequests.put(op.requestID(), stored);
            return new String[]{Messages.OK, stored};

        } else if ("Get".equals(op.operation())) {
            String value = data.get(op.key());
            if (value != null) return new String[]{Messages.OK, value};
            return new String[]{Messages.ERR_NO_KEY, ""};
        }
        return new String[]{"", ""};
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    public void kill() {
        dead = true;
        try { listener.close(); } catch (IOException ignored) {}
        rpcPool.shutdownNow();
        px.kill();
    }

    public boolean isDead() { return dead; }

    // Set unreliability on both this server's RPC listener AND the underlying
    // paxos peer — matches the Go reference where they share one socket.
    public void setUnreliable(boolean v) {
        unreliable = v;
        px.setUnreliable(v);
    }

    // -----------------------------------------------------------------------
    // RPC plumbing — TCP + Java object serialization
    // (same wire format as paxos-core's rpcCall/serveConn)
    // -----------------------------------------------------------------------

    private void runListener() {
        while (!dead) {
            try {
                Socket conn = listener.accept();
                if (dead) { conn.close(); return; }
                if (unreliable && rand.nextInt(1000) < 100) {
                    // Drop the request entirely.
                    conn.close();
                    continue;
                }
                final boolean discardReply = unreliable && rand.nextInt(1000) < 200;
                rpcPool.submit(() -> serveConn(conn, discardReply));
            } catch (SocketException e) {
                return;
            } catch (IOException e) {
                if (!dead) System.err.println("KVPaxos(" + me + ") accept: " + e.getMessage());
                return;
            }
        }
    }

    private void serveConn(Socket conn, boolean discardReply) {
        try (Socket c = conn;
             ObjectInputStream  in  = new ObjectInputStream(c.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(c.getOutputStream())) {
            String method = (String) in.readObject();
            Object args   = in.readObject();
            Object reply = switch (method) {
                case "Get" -> get((Messages.GetArgs) args);
                case "Put" -> put((Messages.PutArgs) args);
                default    -> null;
            };
            if (discardReply) return;
            out.writeObject(reply);
            out.flush();
        } catch (Exception ignored) {
            // Peer hung up, deserialization failed, etc. Mirror Go: swallow.
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static int portOf(String addr) {
        return Integer.parseInt(addr.substring(addr.indexOf(':') + 1));
    }
}
