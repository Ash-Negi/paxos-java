package kvstore;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.SecureRandom;

// Client for KVPaxos. Java port of src/kvpaxos/client.go.
//
// On each call: pick a server, send the RPC, retry on failure (cycling through
// the configured server list). Each call generates a single requestID and
// reuses it across retries — the server's dedup map handles the "Put applied
// but reply was lost" case.
public class Clerk {

    private final String[] servers;
    private final SecureRandom rng = new SecureRandom();

    public Clerk(String[] servers) {
        this.servers = servers;
    }

    public String get(String key) {
        Messages.GetArgs args = new Messages.GetArgs();
        args.key       = key;
        args.requestID = nrand();

        while (true) {
            for (String server : servers) {
                Messages.GetReply reply = call(server, "Get", args, Messages.GetReply.class);
                if (reply != null) {
                    return reply.value == null ? "" : reply.value;
                }
            }
            try { Thread.sleep(50); } catch (InterruptedException e) { return ""; }
        }
    }

    public String putExt(String key, String value, boolean dohash) {
        Messages.PutArgs args = new Messages.PutArgs();
        args.key       = key;
        args.value     = value;
        args.doHash    = dohash;
        args.requestID = nrand();

        while (true) {
            for (String server : servers) {
                Messages.PutReply reply = call(server, "Put", args, Messages.PutReply.class);
                if (reply != null) {
                    return reply.previousValue == null ? "" : reply.previousValue;
                }
            }
            try { Thread.sleep(50); } catch (InterruptedException e) { return ""; }
        }
    }

    public void put(String key, String value) {
        putExt(key, value, false);
    }

    public String putHash(String key, String value) {
        return putExt(key, value, true);
    }

    // 62-bit positive random, matching Go client's nrand().
    private long nrand() {
        return rng.nextLong() & ((1L << 62) - 1);
    }

    @SuppressWarnings("unchecked")
    private <Rep> Rep call(String addr, String method, Object args, Class<Rep> replyClass) {
        try (Socket s = new Socket()) {
            String host = addr.substring(0, addr.indexOf(':'));
            int    port = Integer.parseInt(addr.substring(addr.indexOf(':') + 1));
            s.connect(new InetSocketAddress(host, port), 1000);
            // Generous read timeout: server holds its mutex through the entire
            // paxos round, so under contention or unreliable networks the
            // reply can take many seconds.
            s.setSoTimeout(30000);
            try (ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                 ObjectInputStream  in  = new ObjectInputStream(s.getInputStream())) {
                out.writeObject(method);
                out.writeObject(args);
                out.flush();
                return (Rep) in.readObject();
            }
        } catch (Exception e) {
            return null;
        }
    }
}
