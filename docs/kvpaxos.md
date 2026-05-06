# kvstore

A replicated key-value store on top of `paxos-core`. Each KVPaxos server owns
a Paxos peer; the consensus log defines the order ops are applied.

For protocol-level decisions, see [paxos-protocol.md](paxos-protocol.md).

## Layout

```
Clerk ──TCP──▶ KVPaxos ──┐
                          ├──▶ Paxos peer ──┐
Clerk ──TCP──▶ KVPaxos ──┤                  │
                          │                  ├── consensus log
Clerk ──TCP──▶ KVPaxos ──┤                  │
                          └──▶ Paxos peer ──┘
```

Each KVPaxos and its Paxos peer share a JVM but listen on **separate ports**:
clients hit the kv port, paxos peers hit the paxos port. The two address
lists are passed independently to the constructor.

## Guarantees

- **Linearizability** — clients see a total order consistent with real-time
  issuance. Paxos provides the order; the dedup map handles client retries.
- **At-most-once `Put`** — even if a reply is lost and the client retries,
  the put applies once.

## Non-obvious decisions

### Client retries reuse the same `requestID`

[Clerk.java:43-58](../kvstore/src/main/java/kvstore/Clerk.java#L43-L58)

The server's dedup map is keyed on `requestID`. Reusing the ID across retries
means a put that applied but lost its reply returns the same value on retry
instead of double-applying. Fresh IDs would let a single client call apply
twice.

### Server holds `mu` through the entire Paxos round

[KVPaxos.java:70-96](../kvstore/src/main/java/kvstore/KVPaxos.java#L70-L96)

Concurrent client RPCs to one server are serialized end-to-end: lock, run
agreement, apply, unlock. Releasing the lock during agreement would let
parallel requests run, but ordering `applyChange()` and `globalSeq` updates
across them is subtle. The simple approach is correct; revisit if throughput
becomes the bottleneck.

### Each request retries at increasing seq numbers

[KVPaxos.java:103-116](../kvstore/src/main/java/kvstore/KVPaxos.java#L103-L116)

When the server proposes at `globalSeq + 1`, Paxos may decide a different
server's op at that seq. The handler checks `op.equals(agreedV)` — if Paxos
chose someone else, advance to `seq + 1` and try again. This relies on `Op`
being a `record` so `equals()` compares all fields.

### Catch-up applies every intervening seq, not just our own

[KVPaxos.java:119-128](../kvstore/src/main/java/kvstore/KVPaxos.java#L119-L128)

After winning at some seq, the server applies every op in `(globalSeq, seq]`
in order. Other servers may have decided ops while we were retrying; skipping
them would diverge local state from the consensus log.

### Idempotency only for `Put`, not `Get`

[KVPaxos.java:144-171](../kvstore/src/main/java/kvstore/KVPaxos.java#L144-L171)

Get is naturally idempotent. Put isn't — replaying double-hashes (`PutHash`)
or overwrites (`Put`). Only Put goes in the dedup map.

### Dedup map stores `""` for plain `Put`

[KVPaxos.java:160-163](../kvstore/src/main/java/kvstore/KVPaxos.java#L160-L163)

Only `PutHash` clients read the previous-value field. Plain `Put` discards
it, so the map keeps an empty string instead — saves memory in long-running
deployments without changing semantics.

## Known limits

- **Dedup map grows unboundedly.** The Go reference uses client-acknowledged
  opIDs to garbage-collect entries; not implemented here.
- **No persistence.** State is in-memory; restart loses data. A real
  deployment needs snapshots and a write-ahead log.
- **No partition tests.** The Go suite has TestPartition / TestHole /
  TestManyPartition that swap Unix-socket symlinks. TCP has no direct
  equivalent here, so they're skipped — see
  [KVPaxosTest.java:17-19](../kvstore/src/test/java/kvstore/KVPaxosTest.java#L17-L19).
