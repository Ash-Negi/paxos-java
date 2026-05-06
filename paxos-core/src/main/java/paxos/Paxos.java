package paxos;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

// Paxos library, Java port of the Go reference implementation.
//
// Each peer is BOTH a proposer and an acceptor. Peers talk to each other
// over plain TCP using Java object serialization (the closest equivalent of
// Go's net/rpc). The peer addresses look like "localhost:9000".
public class Paxos {

    // Acceptor / decided state for one Paxos instance (one seq number).
    public static class Instance implements Serializable {
        int highestAcN = -1;          // Na
        Object highestAcV = null;     // Va
        int highestSeen = -1;         // Np
        boolean decided = false;
        Object decidedV = null;
    }

    // ---- RPC message types --------------------------------------------------

    public static class PrepareArgs implements Serializable {
        int seq, n, sender, done;
    }
    public static class PrepareReply implements Serializable {
        boolean ok; int na; Object va; int done;
    }
    public static class AcceptArgs implements Serializable {
        int seq, n; Object v;
    }
    public static class AcceptReply implements Serializable {
        boolean ok; int n;
    }
    public static class DecideArgs implements Serializable {
        int seq; Object v;
    }
    public static class DecideReply implements Serializable {
        boolean ok;
    }

    // ---- Constants ----------------------------------------------------------

    private static final int PEER_ID_BITS = 8;

    // ---- Mutable state (guarded by `mu` unless marked volatile) -------------

    private final ReentrantLock mu = new ReentrantLock();
    private final String[] peers;
    private final int me;
    private volatile boolean dead = false;
    private volatile boolean unreliable = false;
    private volatile boolean disabled = false; // simulates "deaf" peer
    private final AtomicInteger rpcCount = new AtomicInteger();

    private final Map<Integer, Instance> instances = new HashMap<>();
    private final Map<Integer, Instance> acceptorIns = new HashMap<>();
    private int highestSeqSeen = -1;
    private final Map<Integer, Integer> peerDone = new HashMap<>();
    private int doneFreed = -1;

    // ---- Networking ---------------------------------------------------------

    private final ServerSocket listener;
    private final Thread listenerThread;
    private final ExecutorService rpcPool   = Executors.newCachedThreadPool();
    private final ExecutorService proposerPool = Executors.newCachedThreadPool();
    private final Random rand = new Random();

    public Paxos(String[] peers, int me) throws IOException {
        this.peers = peers;
        this.me = me;
        for (int i = 0; i < peers.length; i++) peerDone.put(i, -1);

        int port = portOf(peers[me]);
        this.listener = new ServerSocket(port);

        this.listenerThread = new Thread(this::runListener, "paxos-listener-" + me);
        this.listenerThread.setDaemon(true);
        this.listenerThread.start();
    }

    // -----------------------------------------------------------------------
    // Public API (mirrors paxos.go: Start, Done, Min, Max, Status, Kill)
    // -----------------------------------------------------------------------

    // Start agreement on instance seq, with proposed value v. Returns immediately;
    // the actual Paxos rounds run in a background thread.
    public void start(int seq, Object v) {
        if (seq < min()) return;

        mu.lock();
        try {
            if (seq > highestSeqSeen) highestSeqSeen = seq;
            Instance inst = instances.get(seq);
            if (inst != null && inst.decided) return;
        } finally {
            mu.unlock();
        }

        proposerPool.submit(() -> propose(seq, v));
    }

    // The application says "I'm done with everything <= seq, you can forget it."
    public void done(int seq) {
        mu.lock();
        try {
            if (seq > peerDone.get(me)) peerDone.put(me, seq);
        } finally {
            mu.unlock();
        }
    }

    // Highest seq this peer has ever seen.
    public int max() {
        mu.lock();
        try { return highestSeqSeen; } finally { mu.unlock(); }
    }

    // One more than min(peerDone[i] for all i). Below this, instances are forgotten.
    public int min() {
        mu.lock();
        int ret;
        try { ret = getMin(); } finally { mu.unlock(); }
        collectGarbage();
        return ret;
    }

    // (decided?, value) for this seq. Local-only — does not contact other peers.
    public record StatusResult(boolean decided, Object value) {}
    public StatusResult status(int seq) {
        mu.lock();
        try {
            Instance inst = instances.get(seq);
            if (inst != null && inst.decided) return new StatusResult(true, inst.decidedV);
            return new StatusResult(false, null);
        } finally {
            mu.unlock();
        }
    }

    // Shut the peer down. Future calls become no-ops; the listener thread exits.
    public void kill() {
        dead = true;
        try { listener.close(); } catch (IOException ignored) {}
        rpcPool.shutdownNow();
        proposerPool.shutdownNow();
    }

    // Test hooks
    public void setUnreliable(boolean v) { unreliable = v; }
    public void setDisabled(boolean v)   { disabled = v; }   // simulates removed socket
    public boolean isDead()              { return dead; }
    public int rpcCount()                { return rpcCount.get(); }

    // -----------------------------------------------------------------------
    // Proposer — runs in a background thread for each Start()
    // -----------------------------------------------------------------------

    private void propose(int seq, Object v) {
        boolean firstCall = true;
        int penaltySleep = 10;

        while (!dead) {
            collectGarbage();

            if (!firstCall) {
                penaltySleep = (int) (penaltySleep * 1.5f);
                if (penaltySleep > 50) penaltySleep = 50;
                int sleepMs = (rand.nextInt(penaltySleep)) + penaltySleep;
                try { Thread.sleep(sleepMs); } catch (InterruptedException e) { return; }
            }
            firstCall = false;

            // Already decided in the meantime?
            int highestSeen, myDone, peerCount, majority;
            mu.lock();
            try {
                Instance inst = instances.get(seq);
                if (inst != null && inst.decided) return;
                Instance acc = acceptorIns.get(seq);
                highestSeen = (acc == null) ? -1 : acc.highestSeen;
                myDone = peerDone.get(me);
                peerCount = peers.length;
                majority = peerCount / 2 + 1;
            } finally {
                mu.unlock();
            }

            int n = generateUniqueN(highestSeen, me);

            // ---- Phase 1: Prepare -------------------------------------------------
            int[] highestNAccepted = { -1 };
            Object[] nextPhaseV    = { v };
            int prepareOK = 0;

            PrepareArgs pArgs = new PrepareArgs();
            pArgs.seq = seq; pArgs.n = n; pArgs.sender = me; pArgs.done = myDone;

            // Self
            PrepareReply selfPrep = rpcPrepare(pArgs);
            if (selfPrep.ok) {
                if (selfPrep.na > highestNAccepted[0]) {
                    highestNAccepted[0] = selfPrep.na;
                    nextPhaseV[0] = selfPrep.va;
                }
                prepareOK++;
            }

            BlockingQueue<Boolean> pChan = new ArrayBlockingQueue<>(peerCount);
            for (int id = 0; id < peers.length; id++) {
                if (id == me) continue;
                final int peerID = id;
                final String peer = peers[id];
                rpcPool.submit(() -> {
                    PrepareReply rep = rpcCall(peer, "Prepare", pArgs, PrepareReply.class);
                    boolean ok = (rep != null) && rep.ok;
                    if (rep != null) {
                        mu.lock();
                        try {
                            if (rep.ok && rep.na > highestNAccepted[0]) {
                                highestNAccepted[0] = rep.na;
                                nextPhaseV[0] = rep.va;
                            }
                            if (rep.done > peerDone.get(peerID)) peerDone.put(peerID, rep.done);
                        } finally { mu.unlock(); }
                    }
                    pChan.offer(ok);
                });
            }

            int allResp = 1; // self
            while (true) {
                Boolean ok;
                try { ok = pChan.poll(2, TimeUnit.SECONDS); }
                catch (InterruptedException e) { return; }
                if (ok == null) break; // safety: don't wait forever
                if (ok) prepareOK++;
                allResp++;
                if (prepareOK >= majority) break;
                if (allResp >= peerCount) break;
            }
            if (prepareOK < majority) continue;

            // ---- Phase 2: Accept --------------------------------------------------
            Object actualV;
            mu.lock();
            try { actualV = nextPhaseV[0]; } finally { mu.unlock(); }

            int acceptOK = 0;
            AcceptArgs aArgs = new AcceptArgs();
            aArgs.seq = seq; aArgs.n = n; aArgs.v = actualV;

            AcceptReply selfAcc = rpcAccept(aArgs);
            if (selfAcc.ok) acceptOK++;

            BlockingQueue<Boolean> aChan = new ArrayBlockingQueue<>(peerCount);
            for (int id = 0; id < peers.length; id++) {
                if (id == me) continue;
                final String peer = peers[id];
                rpcPool.submit(() -> {
                    AcceptReply rep = rpcCall(peer, "Accept", aArgs, AcceptReply.class);
                    aChan.offer(rep != null && rep.ok);
                });
            }

            int allAccResp = 1;
            while (true) {
                Boolean ok;
                try { ok = aChan.poll(2, TimeUnit.SECONDS); }
                catch (InterruptedException e) { return; }
                if (ok == null) break;
                if (ok) acceptOK++;
                allAccResp++;
                if (acceptOK >= majority) break;
                if (allAccResp >= peerCount) break;
            }
            if (acceptOK < majority) continue;

            // ---- Phase 3: Decide --------------------------------------------------
            DecideArgs dArgs = new DecideArgs();
            dArgs.seq = seq; dArgs.v = actualV;
            rpcDecide(dArgs);
            for (int id = 0; id < peers.length; id++) {
                if (id == me) continue;
                final String peer = peers[id];
                rpcPool.submit(() -> rpcCall(peer, "Decide", dArgs, DecideReply.class));
            }

            collectGarbage();
            return;
        }
    }

    // -----------------------------------------------------------------------
    // RPC handlers (called both directly by self and via the listener)
    // -----------------------------------------------------------------------

    public PrepareReply rpcPrepare(PrepareArgs args) {
        PrepareReply reply = new PrepareReply();
        mu.lock();
        try {
            Instance acc = acceptorIns.get(args.seq);
            if (acc == null) {
                acc = new Instance();
                acc.highestSeen = -1; acc.highestAcN = -1;
            }

            if (args.n > acc.highestSeen) {
                acc.highestSeen = args.n;
                reply.ok = true;
                reply.na = acc.highestAcN;
                reply.va = acc.highestAcV;
                acceptorIns.put(args.seq, acc);
            } else {
                reply.ok = false;
            }

            reply.done = peerDone.get(me);
            //Check to see if you need to update self copy of peerDone Map for sender done value
            if (args.done > peerDone.get(args.sender)) peerDone.put(args.sender, args.done);
        } finally {
            mu.unlock();
        }
        collectGarbage();
        return reply;
    }

    public AcceptReply rpcAccept(AcceptArgs args) {
        AcceptReply reply = new AcceptReply();
        mu.lock();
        try {
            Instance acc = acceptorIns.get(args.seq);
            if (acc == null) {
                acc = new Instance();
                acc.highestSeen = -1; acc.highestAcN = -1;
            }
            if (args.n >= acc.highestSeen) {
                acc.highestSeen = args.n;
                acc.highestAcN  = args.n;
                acc.highestAcV  = args.v;
                acceptorIns.put(args.seq, acc);
                reply.ok = true; reply.n = args.n;
            } else {
                reply.ok = false;
            }
        } finally {
            mu.unlock();
        }
        return reply;
    }

    public DecideReply rpcDecide(DecideArgs args) {
        DecideReply reply = new DecideReply();
        mu.lock();
        try {
            Instance inst = instances.get(args.seq);
            if (inst == null) inst = new Instance();
            inst.decided = true;
            inst.decidedV = args.v;
            instances.put(args.seq, inst);
            reply.ok = true;
            if (args.seq > highestSeqSeen) highestSeqSeen = args.seq;
        } finally {
            mu.unlock();
        }
        return reply;
    }

    // -----------------------------------------------------------------------
    // RPC plumbing — TCP + Java object serialization
    // -----------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private <Rep> Rep rpcCall(String addr, String method, Object args, Class<Rep> replyClass) {
        try (Socket s = new Socket()) {
            s.connect(new java.net.InetSocketAddress(host(addr), portOf(addr)), 500);
            s.setSoTimeout(1000);
            try (ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                 ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {
                out.writeObject(method);
                out.writeObject(args);
                out.flush();
                return (Rep) in.readObject();
            }
        } catch (Exception e) {
            return null; // analogous to Go's call() returning false
        }
    }

    private void runListener() {
        while (!dead) {
            try {
                Socket conn = listener.accept();
                if (dead) { conn.close(); return; }
                if (disabled) { conn.close(); continue; }
                if (unreliable && rand.nextInt(1000) < 100) {
                    conn.close();
                    continue;
                }
                rpcCount.incrementAndGet();
                final boolean discardReply = unreliable && rand.nextInt(1000) < 200;
                rpcPool.submit(() -> serveConn(conn, discardReply));
            } catch (SocketException e) {
                if (!dead) { /* shutting down */ }
                return;
            } catch (IOException e) {
                if (!dead) System.err.println("accept: " + e.getMessage());
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
                case "Prepare" -> rpcPrepare((PrepareArgs) args);
                case "Accept"  -> rpcAccept((AcceptArgs) args);
                case "Decide"  -> rpcDecide((DecideArgs) args);
                default -> null;
            };
            if (discardReply) return;
            out.writeObject(reply);
            out.flush();
        } catch (Exception ignored) {
            // Peer hung up, deserialization failed, etc. Mirror Go: swallow.
        }
    }

    // -----------------------------------------------------------------------
    // Memory management — collectGarbage / getMin
    // -----------------------------------------------------------------------

    private void collectGarbage() {
        mu.lock();
        try {
            int currentMin = getMin();
            if (currentMin > doneFreed) {
                instances.keySet().removeIf(i -> i < currentMin);
                acceptorIns.keySet().removeIf(i -> i < currentMin);
                doneFreed = currentMin;
            }
        } finally {
            mu.unlock();
        }
    }

    // Caller holds mu.
    private int getMin() {
        int doneByAll = Integer.MAX_VALUE;
        for (int v : peerDone.values()) if (v < doneByAll) doneByAll = v;
        return doneByAll + 1;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static int generateUniqueN(int highestNSeen, int peerID) {
        int newID = (highestNSeen >> PEER_ID_BITS) + 1;
        return (newID << PEER_ID_BITS) | peerID;
    }

    private static int portOf(String addr) {
        return Integer.parseInt(addr.substring(addr.indexOf(':') + 1));
    }
    private static String host(String addr) {
        return addr.substring(0, addr.indexOf(':'));
    }
}
