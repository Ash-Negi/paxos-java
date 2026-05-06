package kvstore;

import java.io.Serializable;

// The value type proposed to Paxos for each client request.
// Records get equals()/hashCode() that compare all fields, which is exactly
// what makeAgreementAndApplyChange() relies on to tell whether the value
// decided at a given seq is "ours" or someone else's.
public record Op(
        String operation,   // "Get" or "Put"
        String key,
        String value,
        long requestID,     // unique per client call; used for Put idempotency
        boolean doHash      // true for PutHash
) implements Serializable {
}
