package server;

import paxos.Paxos;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

// HTTP API in front of a Paxos cluster.
//
// Two run modes:
//   1. Local (no env vars): spawns N_PEERS peers in this JVM on localhost.
//      Behaves the same as before — useful for `mvn exec` style local dev.
//   2. Distributed (PEERS + PEER_INDEX env vars set): runs as a single peer
//      that talks to the others over the network. This is what Kubernetes
//      uses — one pod per peer.
public class Main {

    static final int DEFAULT_LOCAL_PEERS = 3;
    static final int DEFAULT_LOCAL_BASE_PORT = 9100;
    static final int DEFAULT_HTTP_PORT = 8080;

    static Paxos[] peers;       // length 1 in distributed mode, N in local mode
    static int peerIndex;       // index into peers[] for HTTP handlers
    static int peerCount;       // logical cluster size (for /cluster, validation)
    static boolean distributed; // true when running as a single peer

    public static void main(String[] args) throws IOException {
        String peersEnv = System.getenv("PEERS");
        String peerIndexEnv = System.getenv("PEER_INDEX");
        int httpPort = parseInt(System.getenv("HTTP_PORT"), DEFAULT_HTTP_PORT);

        if (peersEnv != null && peerIndexEnv != null) {
            startDistributed(peersEnv, Integer.parseInt(peerIndexEnv), httpPort);
        } else {
            startLocal(httpPort);
        }
    }

    // Single-peer mode: this JVM is one node in a larger cluster.
    // PEERS is a comma-separated list of host:port for *all* peers in cluster order.
    static void startDistributed(String peersCsv, int me, int httpPort) throws IOException {
        String[] addrs = peersCsv.split(",");
        for (int i = 0; i < addrs.length; i++) addrs[i] = addrs[i].trim();
        if (me < 0 || me >= addrs.length) {
            throw new IllegalArgumentException("PEER_INDEX " + me + " out of range for " + addrs.length + " peers");
        }
        peerCount = addrs.length;
        peerIndex = me;
        distributed = true;

        peers = new Paxos[]{ new Paxos(addrs, me) };

        Runtime.getRuntime().addShutdownHook(new Thread(() -> peers[0].kill()));

        startHttp(httpPort);
        System.out.printf("Paxos peer %d/%d listening on %s (HTTP %d)%n",
                me, peerCount, addrs[me], httpPort);
        System.out.printf("Cluster: %s%n", peersCsv);
    }

    // All-peers-in-one-JVM mode (original behavior, used for local dev/tests).
    static void startLocal(int httpPort) throws IOException {
        int n = DEFAULT_LOCAL_PEERS;
        int basePort = DEFAULT_LOCAL_BASE_PORT;
        peerCount = n;
        peerIndex = 0;
        distributed = false;

        String[] addrs = new String[n];
        for (int i = 0; i < n; i++) addrs[i] = "localhost:" + (basePort + i);

        peers = new Paxos[n];
        for (int i = 0; i < n; i++) peers[i] = new Paxos(addrs, i);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Paxos p : peers) p.kill();
        }));

        startHttp(httpPort);
        System.out.printf("Paxos cluster: %d peers (local mode)%n", n);
        System.out.printf("Listening on http://localhost:%d%n", httpPort);
    }

    static void startHttp(int httpPort) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(httpPort), 0);
        server.createContext("/propose", new ProposeHandler());
        server.createContext("/status",  new StatusHandler());
        server.createContext("/done",    new DoneHandler());
        server.createContext("/min",     new MinHandler());
        server.createContext("/max",     new MaxHandler());
        server.createContext("/cluster", new ClusterHandler());
        server.createContext("/health",  new HealthHandler());
        server.start();
    }

    // Resolve the peer index a handler should use. In distributed mode every
    // request is served by the local peer regardless of the `peer` parameter;
    // in local mode the caller picks which peer to talk to.
    static int resolvePeer(int requested) {
        if (distributed) return 0;
        return requested;
    }

    static boolean validRequestedPeer(int p) {
        if (distributed) return p == peerIndex || p == -1; // -1 = unspecified, default to self
        return p >= 0 && p < peerCount;
    }

    // ---- Handlers --------------------------------------------------------

    static class ProposeHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            if (!"POST".equals(ex.getRequestMethod())) { sendErr(ex, 405, "POST only"); return; }
            Map<String, String> body = parseJson(readBody(ex));
            int seq  = parseInt(body.get("seq"), -1);
            int peer = parseInt(body.get("peer"), distributed ? peerIndex : -1);
            String value = body.getOrDefault("value", "");
            if (!validRequestedPeer(peer)) { sendErr(ex, 400, peerErrMsg()); return; }
            int idx = resolvePeer(peer);
            peers[idx].start(seq, value);
            send(ex, 200, "{\"started\":true,\"seq\":" + seq + ",\"peer\":" + peer +
                    ",\"value\":" + jsonStr(value) +
                    ",\"note\":\"poll /status to see when decided\"}");
        }
    }

    static class StatusHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            Map<String, String> q = queryParams(ex.getRequestURI());
            int seq  = parseInt(q.get("seq"), -1);
            int peer = parseInt(q.get("peer"), distributed ? peerIndex : -1);
            if (!validRequestedPeer(peer)) { sendErr(ex, 400, peerErrMsg()); return; }
            int idx = resolvePeer(peer);

            Paxos.StatusResult st = peers[idx].status(seq);
            long deadline = System.currentTimeMillis() + 500;
            while (!st.decided() && System.currentTimeMillis() < deadline) {
                try { Thread.sleep(25); } catch (InterruptedException e) { break; }
                st = peers[idx].status(seq);
            }

            String value = st.value() == null ? "null" : jsonStr(st.value().toString());
            send(ex, 200, "{\"peer\":" + peer + ",\"seq\":" + seq +
                    ",\"decided\":" + st.decided() + ",\"value\":" + value + "}");
        }
    }

    static class DoneHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            if (!"POST".equals(ex.getRequestMethod())) { sendErr(ex, 405, "POST only"); return; }
            Map<String, String> body = parseJson(readBody(ex));
            int seq  = parseInt(body.get("seq"), -1);
            int peer = parseInt(body.get("peer"), distributed ? peerIndex : -1);
            if (!validRequestedPeer(peer)) { sendErr(ex, 400, peerErrMsg()); return; }
            int idx = resolvePeer(peer);
            peers[idx].done(seq);
            send(ex, 200, "{\"peer\":" + peer + ",\"seq\":" + seq +
                    ",\"new_min\":" + peers[idx].min() + "}");
        }
    }

    static class MinHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            int peer = parseInt(queryParams(ex.getRequestURI()).get("peer"), distributed ? peerIndex : -1);
            if (!validRequestedPeer(peer)) { sendErr(ex, 400, peerErrMsg()); return; }
            int idx = resolvePeer(peer);
            send(ex, 200, "{\"peer\":" + peer + ",\"min\":" + peers[idx].min() + "}");
        }
    }

    static class MaxHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            int peer = parseInt(queryParams(ex.getRequestURI()).get("peer"), distributed ? peerIndex : -1);
            if (!validRequestedPeer(peer)) { sendErr(ex, 400, peerErrMsg()); return; }
            int idx = resolvePeer(peer);
            send(ex, 200, "{\"peer\":" + peer + ",\"max\":" + peers[idx].max() + "}");
        }
    }

    // /cluster in distributed mode only reports the local peer; clients can hit
    // each pod individually (or the headless service) for a full picture.
    static class ClusterHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            StringBuilder sb = new StringBuilder("[");
            if (distributed) {
                sb.append("{\"peer\":").append(peerIndex)
                  .append(",\"min\":").append(peers[0].min())
                  .append(",\"max\":").append(peers[0].max())
                  .append("}");
            } else {
                for (int i = 0; i < peerCount; i++) {
                    if (i > 0) sb.append(",");
                    sb.append("{\"peer\":").append(i)
                      .append(",\"min\":").append(peers[i].min())
                      .append(",\"max\":").append(peers[i].max())
                      .append("}");
                }
            }
            sb.append("]");
            send(ex, 200, sb.toString());
        }
    }

    static class HealthHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            send(ex, 200, "{\"ok\":true,\"peer\":" + peerIndex + "}");
        }
    }

    // ---- Tiny helpers ----------------------------------------------------

    static String peerErrMsg() {
        return distributed
                ? "this pod serves peer " + peerIndex + " only"
                : "peer must be 0-" + (peerCount - 1);
    }

    static int parseInt(String s, int dflt) {
        try { return Integer.parseInt(s); } catch (Exception e) { return dflt; }
    }

    static String readBody(HttpExchange ex) throws IOException {
        try (BufferedReader r = new BufferedReader(
                new InputStreamReader(ex.getRequestBody(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = r.readLine()) != null) sb.append(line);
            return sb.toString();
        }
    }

    static Map<String, String> queryParams(URI uri) {
        Map<String, String> m = new HashMap<>();
        String q = uri.getRawQuery();
        if (q == null) return m;
        for (String pair : q.split("&")) {
            int i = pair.indexOf('=');
            if (i > 0) m.put(pair.substring(0, i), pair.substring(i + 1));
        }
        return m;
    }

    static Map<String, String> parseJson(String body) {
        Map<String, String> m = new HashMap<>();
        if (body == null) return m;
        body = body.trim();
        if (body.startsWith("{")) body = body.substring(1);
        if (body.endsWith("}"))   body = body.substring(0, body.length() - 1);
        boolean inString = false;
        StringBuilder cur = new StringBuilder();
        java.util.List<String> parts = new java.util.ArrayList<>();
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '"') inString = !inString;
            if (c == ',' && !inString) { parts.add(cur.toString()); cur.setLength(0); }
            else cur.append(c);
        }
        if (cur.length() > 0) parts.add(cur.toString());
        for (String part : parts) {
            int colon = part.indexOf(':');
            if (colon < 0) continue;
            String k = part.substring(0, colon).trim().replace("\"", "");
            String v = part.substring(colon + 1).trim();
            if (v.startsWith("\"") && v.endsWith("\"")) v = v.substring(1, v.length() - 1);
            m.put(k, v);
        }
        return m;
    }

    static String jsonStr(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    static void send(HttpExchange ex, int code, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }

    static void sendErr(HttpExchange ex, int code, String msg) throws IOException {
        send(ex, code, "{\"error\":" + jsonStr(msg) + "}");
    }
}
