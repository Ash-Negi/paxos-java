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

// HTTP API in front of a 3-peer Paxos cluster running inside this JVM.
// Mirrors cmd/main.go from the Go version.
public class Main {

    static final int N_PEERS    = 3;
    static final int BASE_PORT  = 9100;   // peer i listens on BASE_PORT + i
    static final int HTTP_PORT  = 8080;

    static Paxos[] peers;

    public static void main(String[] args) throws IOException {
        // Build the peer address list. Each peer gets the full list so it knows
        // how to reach the others.
        String[] addrs = new String[N_PEERS];
        for (int i = 0; i < N_PEERS; i++) addrs[i] = "localhost:" + (BASE_PORT + i);

        peers = new Paxos[N_PEERS];
        for (int i = 0; i < N_PEERS; i++) peers[i] = new Paxos(addrs, i);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Paxos p : peers) p.kill();
        }));

        HttpServer server = HttpServer.create(new InetSocketAddress(HTTP_PORT), 0);
        server.createContext("/propose", new ProposeHandler());
        server.createContext("/status",  new StatusHandler());
        server.createContext("/done",    new DoneHandler());
        server.createContext("/min",     new MinHandler());
        server.createContext("/max",     new MaxHandler());
        server.createContext("/cluster", new ClusterHandler());
        server.start();

        System.out.printf("Paxos cluster: %d peers%n%n", N_PEERS);
        System.out.println("Endpoints:");
        System.out.println("  POST /propose   {\"seq\":0, \"value\":\"hello\", \"peer\":0}");
        System.out.println("  GET  /status    ?seq=0&peer=0");
        System.out.println("  POST /done      {\"seq\":0, \"peer\":0}");
        System.out.println("  GET  /min       ?peer=0");
        System.out.println("  GET  /max       ?peer=0");
        System.out.println("  GET  /cluster");
        System.out.printf("%nListening on http://localhost:%d%n", HTTP_PORT);
    }

    // ---- Handlers --------------------------------------------------------

    static class ProposeHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            if (!"POST".equals(ex.getRequestMethod())) { sendErr(ex, 405, "POST only"); return; }
            Map<String, String> body = parseJson(readBody(ex));
            int seq  = parseInt(body.get("seq"), -1);
            int peer = parseInt(body.get("peer"), -1);
            String value = body.getOrDefault("value", "");
            if (!validPeer(peer)) { sendErr(ex, 400, "peer must be 0-" + (N_PEERS - 1)); return; }
            peers[peer].start(seq, value);
            send(ex, 200, "{\"started\":true,\"seq\":" + seq + ",\"peer\":" + peer +
                    ",\"value\":" + jsonStr(value) +
                    ",\"note\":\"poll /status to see when decided\"}");
        }
    }

    static class StatusHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            Map<String, String> q = queryParams(ex.getRequestURI());
            int seq  = parseInt(q.get("seq"), -1);
            int peer = parseInt(q.get("peer"), -1);
            if (!validPeer(peer)) { sendErr(ex, 400, "peer must be 0-" + (N_PEERS - 1)); return; }

            // Briefly poll because Paxos runs async after start().
            Paxos.StatusResult st = peers[peer].status(seq);
            long deadline = System.currentTimeMillis() + 500;
            while (!st.decided() && System.currentTimeMillis() < deadline) {
                try { Thread.sleep(25); } catch (InterruptedException e) { break; }
                st = peers[peer].status(seq);
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
            int peer = parseInt(body.get("peer"), -1);
            if (!validPeer(peer)) { sendErr(ex, 400, "peer must be 0-" + (N_PEERS - 1)); return; }
            peers[peer].done(seq);
            send(ex, 200, "{\"peer\":" + peer + ",\"seq\":" + seq +
                    ",\"new_min\":" + peers[peer].min() + "}");
        }
    }

    static class MinHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            int peer = parseInt(queryParams(ex.getRequestURI()).get("peer"), -1);
            if (!validPeer(peer)) { sendErr(ex, 400, "peer must be 0-" + (N_PEERS - 1)); return; }
            send(ex, 200, "{\"peer\":" + peer + ",\"min\":" + peers[peer].min() + "}");
        }
    }

    static class MaxHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            int peer = parseInt(queryParams(ex.getRequestURI()).get("peer"), -1);
            if (!validPeer(peer)) { sendErr(ex, 400, "peer must be 0-" + (N_PEERS - 1)); return; }
            send(ex, 200, "{\"peer\":" + peer + ",\"max\":" + peers[peer].max() + "}");
        }
    }

    static class ClusterHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < N_PEERS; i++) {
                if (i > 0) sb.append(",");
                sb.append("{\"peer\":").append(i)
                  .append(",\"min\":").append(peers[i].min())
                  .append(",\"max\":").append(peers[i].max())
                  .append("}");
            }
            sb.append("]");
            send(ex, 200, sb.toString());
        }
    }

    // ---- Tiny helpers ----------------------------------------------------

    static boolean validPeer(int p) { return p >= 0 && p < N_PEERS; }

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

    // Tiny JSON parser that handles {"key":"string","key":number} only.
    // Good enough for our 3-key payloads — not a general parser.
    static Map<String, String> parseJson(String body) {
        Map<String, String> m = new HashMap<>();
        if (body == null) return m;
        body = body.trim();
        if (body.startsWith("{")) body = body.substring(1);
        if (body.endsWith("}"))   body = body.substring(0, body.length() - 1);
        // Naive split on commas not inside strings — fine for our inputs.
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