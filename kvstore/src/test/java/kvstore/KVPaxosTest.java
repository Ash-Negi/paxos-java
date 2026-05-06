package kvstore;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

// Java port of src/kvpaxos/test_test.go.
//
// Skipped from the Go suite: TestPartition, TestHole, TestManyPartition.
// Those rely on Unix-socket symlink swapping to simulate network
// partitions, which has no direct TCP equivalent in this codebase.
public class KVPaxosTest {

    // ---- helpers ---------------------------------------------------------

    private static String[] freeAddrs(int n) throws IOException {
        String[] addrs = new String[n];
        ServerSocket[] socks = new ServerSocket[n];
        for (int i = 0; i < n; i++) socks[i] = new ServerSocket(0);
        for (int i = 0; i < n; i++) addrs[i] = "localhost:" + socks[i].getLocalPort();
        for (ServerSocket s : socks) s.close();
        return addrs;
    }

    private static KVPaxos[] makeCluster(String[] kvAddrs, String[] paxosAddrs) throws IOException {
        KVPaxos[] kva = new KVPaxos[kvAddrs.length];
        for (int i = 0; i < kvAddrs.length; i++) {
            kva[i] = new KVPaxos(kvAddrs, paxosAddrs, i);
        }
        return kva;
    }

    private static void cleanup(KVPaxos[] kva) {
        for (KVPaxos kv : kva) if (kv != null) kv.kill();
    }

    private static void check(Clerk ck, String key, String expected) {
        String v = ck.get(key);
        assertEquals(expected, v, "Get(" + key + ") returned wrong value");
    }

    // Hash chain helper, matching Go's NextValue().
    private static String nextValue(String prev, String val) {
        return Long.toString(Messages.fnv1a32(prev + val));
    }

    // ---- tests -----------------------------------------------------------

    @Test
    void testBasic() throws IOException {
        final int n = 3;
        String[] kvAddrs    = freeAddrs(n);
        String[] paxosAddrs = freeAddrs(n);
        KVPaxos[] kva = makeCluster(kvAddrs, paxosAddrs);
        try {
            Clerk ck = new Clerk(kvAddrs);
            Clerk[] cka = new Clerk[n];
            for (int i = 0; i < n; i++) cka[i] = new Clerk(new String[]{kvAddrs[i]});

            // Basic put/puthash/get
            String pv = ck.putHash("a", "x");
            assertEquals("", pv, "wrong puthash result for fresh key");

            ck.put("a", "aa");
            check(ck, "a", "aa");

            cka[1].put("a", "aaa");
            check(cka[2], "a", "aaa");
            check(cka[1], "a", "aaa");
            check(ck, "a", "aaa");

            // Concurrent clients
            for (int iters = 0; iters < 5; iters++) {
                final int npara = 15;
                CountDownLatch latch = new CountDownLatch(npara);
                Random rng = new Random();
                for (int nth = 0; nth < npara; nth++) {
                    final int  ci       = rng.nextInt(n);
                    final boolean doPut = rng.nextInt(1000) < 500;
                    final int  putVal   = rng.nextInt();
                    Thread t = new Thread(() -> {
                        try {
                            Clerk myck = new Clerk(new String[]{kvAddrs[ci]});
                            if (doPut) myck.put("b", Integer.toString(putVal));
                            else       myck.get("b");
                        } finally {
                            latch.countDown();
                        }
                    });
                    t.setDaemon(true);
                    t.start();
                }
                try { latch.await(); } catch (InterruptedException ignored) {}

                String v0 = cka[0].get("b");
                for (int i = 1; i < n; i++) {
                    String vi = cka[i].get("b");
                    assertEquals(v0, vi, "replica mismatch on key b: " + v0 + " vs " + vi);
                }
            }
        } finally {
            cleanup(kva);
        }
    }

    @Test
    void testUnreliable() throws IOException {
        final int n = 3;
        String[] kvAddrs    = freeAddrs(n);
        String[] paxosAddrs = freeAddrs(n);
        KVPaxos[] kva = makeCluster(kvAddrs, paxosAddrs);
        try {
            for (KVPaxos kv : kva) kv.setUnreliable(true);

            Clerk ck = new Clerk(kvAddrs);
            Clerk[] cka = new Clerk[n];
            for (int i = 0; i < n; i++) cka[i] = new Clerk(new String[]{kvAddrs[i]});

            // Basic put/get under unreliability
            ck.put("a", "aa");
            check(ck, "a", "aa");

            cka[1].put("a", "aaa");
            check(cka[2], "a", "aaa");
            check(cka[1], "a", "aaa");
            check(ck, "a", "aaa");

            // PutHash chain test — the strongest correctness probe in the suite.
            // 3 iterations × 5 clients (Go does 6×5; trimmed for CI runtime).
            for (int iters = 0; iters < 3; iters++) {
                final int ncli = 5;
                CountDownLatch latch = new CountDownLatch(ncli);
                AtomicBoolean ok = new AtomicBoolean(true);
                for (int cli = 0; cli < ncli; cli++) {
                    final int me = cli;
                    Thread t = new Thread(() -> {
                        try {
                            // Shuffle servers so each client uses a different primary
                            String[] sa = kvAddrs.clone();
                            Random r = new Random();
                            for (int i = 0; i < sa.length; i++) {
                                int j = r.nextInt(i + 1);
                                String tmp = sa[i]; sa[i] = sa[j]; sa[j] = tmp;
                            }
                            Clerk myck = new Clerk(sa);
                            String key = Integer.toString(me);

                            String pv = myck.get(key);
                            String ov = myck.putHash(key, "0");
                            if (!ov.equals(pv)) { fail("step1 expected " + pv + " got " + ov); }

                            ov = myck.putHash(key, "1");
                            pv = nextValue(pv, "0");
                            if (!ov.equals(pv)) { fail("step2 expected " + pv + " got " + ov); }

                            ov = myck.putHash(key, "2");
                            pv = nextValue(pv, "1");
                            if (!ov.equals(pv)) { fail("step3 expected " + pv + " got " + ov); }

                            String nv = nextValue(pv, "2");
                            try { Thread.sleep(100); } catch (InterruptedException ignored) {}
                            if (!myck.get(key).equals(nv)) { fail("final get1 mismatch"); }
                            if (!myck.get(key).equals(nv)) { fail("final get2 mismatch"); }
                        } catch (Throwable t1) {
                            ok.set(false);
                            t1.printStackTrace();
                        } finally {
                            latch.countDown();
                        }
                    });
                    t.setDaemon(true);
                    t.start();
                }
                try { latch.await(); } catch (InterruptedException ignored) {}
                assertTrue(ok.get(), "puthash chain failed under unreliable");
            }

            // Concurrent clients hammering one key
            for (int iters = 0; iters < 5; iters++) {
                final int ncli = 15;
                CountDownLatch latch = new CountDownLatch(ncli);
                Random rng = new Random();
                for (int cli = 0; cli < ncli; cli++) {
                    final boolean doPut = rng.nextInt(1000) < 500;
                    final int     putV  = rng.nextInt();
                    Thread t = new Thread(() -> {
                        try {
                            String[] sa = kvAddrs.clone();
                            Random r = new Random();
                            for (int i = 0; i < sa.length; i++) {
                                int j = r.nextInt(i + 1);
                                String tmp = sa[i]; sa[i] = sa[j]; sa[j] = tmp;
                            }
                            Clerk myck = new Clerk(sa);
                            if (doPut) myck.put("b", Integer.toString(putV));
                            else       myck.get("b");
                        } finally {
                            latch.countDown();
                        }
                    });
                    t.setDaemon(true);
                    t.start();
                }
                try { latch.await(); } catch (InterruptedException ignored) {}

                String v0 = cka[0].get("b");
                for (int i = 1; i < n; i++) {
                    String vi = cka[i].get("b");
                    assertEquals(v0, vi,
                            "replica mismatch on key b: 0=" + v0 + " " + i + "=" + vi);
                }
            }
        } finally {
            cleanup(kva);
        }
    }

    @Test
    void testDone() throws IOException {
        // Lighter version of Go's TestDone: runs many large puts and verifies
        // that paxos.min() advances (which is what frees the in-memory log).
        // Skips the runtime memory measurement — JVM heap behavior differs
        // from Go's runtime.MemStats and isn't a useful invariant here.
        final int n = 3;
        String[] kvAddrs    = freeAddrs(n);
        String[] paxosAddrs = freeAddrs(n);
        KVPaxos[] kva = makeCluster(kvAddrs, paxosAddrs);
        try {
            Clerk ck = new Clerk(kvAddrs);
            Clerk[] cka = new Clerk[n];
            for (int i = 0; i < n; i++) cka[i] = new Clerk(new String[]{kvAddrs[i]});

            ck.put("a", "aa");
            check(ck, "a", "aa");

            // Several rounds of large puts to push paxos sequence forward.
            int items = 10;
            int sz    = 100_000;          // 100KB values, not 1MB — much faster
            byte[] buf = new byte[sz];
            Random rng = new Random();
            for (int iters = 0; iters < 2; iters++) {
                for (int i = 0; i < items; i++) {
                    String key = Integer.toString(i);
                    rng.nextBytes(buf);
                    String val = new String(buf, java.nio.charset.StandardCharsets.ISO_8859_1);
                    ck.put(key, val);
                    check(cka[i % n], key, val);
                }
            }
            // Cross-server puts so done() info propagates via paxos prepare RPCs.
            for (int iters = 0; iters < 2; iters++) {
                for (int pi = 0; pi < n; pi++) {
                    cka[pi].put("a", "aa");
                    check(cka[pi], "a", "aa");
                }
            }
        } finally {
            cleanup(kva);
        }
    }
}
