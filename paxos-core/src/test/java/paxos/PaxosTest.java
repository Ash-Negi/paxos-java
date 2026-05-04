package paxos;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

// Java port of test_test.go.
// Each test allocates a fresh set of TCP ports so tests can run in parallel
// or be re-run without "address already in use" errors.
public class PaxosTest {

    // ---- helpers ---------------------------------------------------------

    // Allocate `n` free TCP ports by opening then immediately closing sockets.
    private static String[] freeAddrs(int n) throws IOException {
        String[] addrs = new String[n];
        ServerSocket[] socks = new ServerSocket[n];
        for (int i = 0; i < n; i++) socks[i] = new ServerSocket(0);
        for (int i = 0; i < n; i++) addrs[i] = "localhost:" + socks[i].getLocalPort();
        for (ServerSocket s : socks) s.close();
        return addrs;
    }

    private static int ndecided(Paxos[] pxa, int seq) {
        int count = 0;
        Object v = null;
        for (Paxos px : pxa) {
            if (px == null) continue;
            Paxos.StatusResult r = px.status(seq);
            if (r.decided()) {
                if (count > 0 && !equalsNullable(v, r.value())) {
                    fail("decided values do not match; seq=" + seq + " v=" + v + " v1=" + r.value());
                }
                count++;
                v = r.value();
            }
        }
        return count;
    }

    private static boolean equalsNullable(Object a, Object b) {
        return (a == null && b == null) || (a != null && a.equals(b));
    }

    private static void waitn(Paxos[] pxa, int seq, int wanted) {
        int to = 10;
        for (int i = 0; i < 30; i++) {
            if (ndecided(pxa, seq) >= wanted) return;
            try { Thread.sleep(to); } catch (InterruptedException e) { return; }
            if (to < 1000) to *= 2;
        }
        int nd = ndecided(pxa, seq);
        if (nd < wanted) fail("too few decided; seq=" + seq + " ndecided=" + nd + " wanted=" + wanted);
    }

    private static void waitMajority(Paxos[] pxa, int seq) {
        waitn(pxa, seq, pxa.length / 2 + 1);
    }

    private static void cleanup(Paxos[] pxa) {
        for (Paxos px : pxa) if (px != null) px.kill();
    }

    private static Paxos[] makeCluster(String[] addrs) throws IOException {
        Paxos[] pxa = new Paxos[addrs.length];
        for (int i = 0; i < addrs.length; i++) pxa[i] = new Paxos(addrs, i);
        return pxa;
    }

    // ---- tests -----------------------------------------------------------

    @Test
    void testBasic() throws IOException {
        final int n = 3;
        String[] addrs = freeAddrs(n);
        Paxos[] pxa = makeCluster(addrs);
        try {
            // Single proposer
            pxa[0].start(0, "hello");
            waitn(pxa, 0, n);

            // Many proposers, same value
            for (int i = 0; i < n; i++) pxa[i].start(1, 77);
            waitn(pxa, 1, n);

            // Many proposers, different values
            pxa[0].start(2, 100);
            pxa[1].start(2, 101);
            pxa[2].start(2, 102);
            waitn(pxa, 2, n);

            // Out-of-order
            pxa[0].start(7, 700);
            pxa[0].start(6, 600);
            pxa[1].start(5, 500);
            waitn(pxa, 7, n);
            pxa[0].start(4, 400);
            pxa[1].start(3, 300);
            waitn(pxa, 6, n);
            waitn(pxa, 5, n);
            waitn(pxa, 4, n);
            waitn(pxa, 3, n);

            assertEquals(7, pxa[0].max(), "wrong max()");
        } finally {
            cleanup(pxa);
        }
    }

    @Test
    void testDeaf() throws IOException {
        final int n = 5;
        String[] addrs = freeAddrs(n);
        Paxos[] pxa = makeCluster(addrs);
        try {
            pxa[0].start(0, "hello");
            waitn(pxa, 0, n);

            // make peers 0 and 4 deaf — they ignore inbound RPCs
            pxa[0].setDisabled(true);
            pxa[n - 1].setDisabled(true);

            pxa[1].start(1, "goodbye");
            waitMajority(pxa, 1);
            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            assertEquals(n - 2, ndecided(pxa, 1),
                    "a deaf peer heard about a decision");

            pxa[0].start(1, "xxx");
            waitn(pxa, 1, n - 1);
            try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            assertEquals(n - 1, ndecided(pxa, 1),
                    "a deaf peer heard about a decision");

            pxa[n - 1].start(1, "yyy");
            waitn(pxa, 1, n);
        } finally {
            cleanup(pxa);
        }
    }

    @Test
    void testForget() throws IOException {
        final int n = 6;
        String[] addrs = freeAddrs(n);
        Paxos[] pxa = makeCluster(addrs);
        try {
            for (int i = 0; i < n; i++) {
                int m = pxa[i].min();
                if (m > 0) fail("wrong initial min() " + m);
            }

            pxa[0].start(0, "00");
            pxa[1].start(1, "11");
            pxa[2].start(2, "22");
            pxa[0].start(6, "66");
            pxa[1].start(7, "77");
            waitn(pxa, 0, n);

            for (int i = 0; i < n; i++) assertEquals(0, pxa[i].min());
            waitn(pxa, 1, n);
            for (int i = 0; i < n; i++) assertEquals(0, pxa[i].min());

            for (int i = 0; i < n; i++) pxa[i].done(0);
            for (int i = 1; i < n; i++) pxa[i].done(1);
            for (int i = 0; i < n; i++) pxa[i].start(8 + i, "xx");

            boolean allOk = false;
            for (int iters = 0; iters < 12 && !allOk; iters++) {
                allOk = true;
                for (int i = 0; i < n; i++) if (pxa[i].min() != 1) { allOk = false; break; }
                if (!allOk) try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            }
            assertTrue(allOk, "min() did not advance after done()");
        } finally {
            cleanup(pxa);
        }
    }

    @Test
    void testMany() throws IOException {
        final int n = 3, ninst = 30;
        String[] addrs = freeAddrs(n);
        Paxos[] pxa = makeCluster(addrs);
        try {
            for (int i = 0; i < n; i++) pxa[i].start(0, 0);

            for (int seq = 1; seq < ninst; seq++) {
                while (seq >= 5 && ndecided(pxa, seq - 5) < n) {
                    try { Thread.sleep(20); } catch (InterruptedException ignored) {}
                }
                for (int i = 0; i < n; i++) pxa[i].start(seq, seq * 10 + i);
            }

            for (int seq = 1; seq < ninst; seq++) waitn(pxa, seq, n);
        } finally {
            cleanup(pxa);
        }
    }

    @Test
    void testOld() throws IOException {
        final int n = 5;
        String[] addrs = freeAddrs(n);
        Paxos[] pxa = new Paxos[n];
        try {
            // Bring up only peers 1,2,3 first.
            pxa[1] = new Paxos(addrs, 1);
            pxa[2] = new Paxos(addrs, 2);
            pxa[3] = new Paxos(addrs, 3);
            pxa[1].start(1, 111);
            waitMajority(pxa, 1);

            // Now peer 0 joins late and proposes a different value.
            pxa[0] = new Paxos(addrs, 0);
            pxa[0].start(1, 222);
            waitn(pxa, 1, 4);
        } finally {
            cleanup(pxa);
        }
    }

    @Test
    void testManyUnreliable() throws IOException {
        final int n = 3, ninst = 20;
        String[] addrs = freeAddrs(n);
        Paxos[] pxa = makeCluster(addrs);
        try {
            for (int i = 0; i < n; i++) {
                pxa[i].setUnreliable(true);
                pxa[i].start(0, 0);
            }
            for (int seq = 1; seq < ninst; seq++) {
                while (seq >= 3 && ndecided(pxa, seq - 3) < n) {
                    try { Thread.sleep(20); } catch (InterruptedException ignored) {}
                }
                for (int i = 0; i < n; i++) pxa[i].start(seq, seq * 10 + i);
            }
            for (int seq = 1; seq < ninst; seq++) waitn(pxa, seq, n);
        } finally {
            cleanup(pxa);
        }
    }

    @Test
    void testRpcCount() throws IOException {
        final int n = 3, ninst1 = 5;
        String[] addrs = freeAddrs(n);
        Paxos[] pxa = makeCluster(addrs);
        try {
            int seq = 0;
            for (int i = 0; i < ninst1; i++) {
                pxa[0].start(seq, "x");
                waitn(pxa, seq, n);
                seq++;
            }
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}

            int total = 0;
            for (Paxos px : pxa) total += px.rpcCount();
            int expected = ninst1 * n * n; // 3 prepares + 3 accepts + 3 decides per agreement
            assertTrue(total <= expected,
                    "too many RPCs for serial starts; got " + total + " expected " + expected);
        } finally {
            cleanup(pxa);
        }
    }
}
