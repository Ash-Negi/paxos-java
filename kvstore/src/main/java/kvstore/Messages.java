package kvstore;

import java.io.Serializable;

// RPC payloads + shared constants/helpers for the kvstore module.
//
// Mirrors common.go from the Go reference. Args/Reply types are mutable
// POJOs (matching the style used in paxos-core), since RPC handlers fill
// reply objects in place.
public final class Messages {

    public static final String OK         = "OK";
    public static final String ERR_NO_KEY = "ErrNoKey";

    private Messages() {}

    public static class PutArgs implements Serializable {
        public String  key;
        public String  value;
        public boolean doHash;
        public long    requestID;
    }

    public static class PutReply implements Serializable {
        public String err;
        public String previousValue;
    }

    public static class GetArgs implements Serializable {
        public String key;
        public long   requestID;
    }

    public static class GetReply implements Serializable {
        public String err;
        public String value;
    }

    // FNV-1a 32-bit hash, matching Go's hash/fnv.New32a().
    // Returned as a long so the caller sees an unsigned 32-bit value.
    public static long fnv1a32(String s) {
        final long FNV_OFFSET = 0x811c9dc5L;
        final long FNV_PRIME  = 0x01000193L;
        long h = FNV_OFFSET;
        for (byte b : s.getBytes()) {
            h ^= (b & 0xffL);
            h = (h * FNV_PRIME) & 0xffffffffL;
        }
        return h;
    }
}
