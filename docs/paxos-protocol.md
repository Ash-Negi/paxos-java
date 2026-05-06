# Paxos Protocol Notes

This document captures the non-obvious decisions and invariants in `paxos-core`.
Inline code comments cover line-level choices; this file covers anything that
spans the protocol or wouldn't fit next to a single line.

## Invariants

The implementation must preserve two safety properties for every Paxos instance
(every `seq`):

1. **Agreement** — at most one value is ever decided.
2. **Validity** — a decided value must have been proposed by some peer.

Liveness (something *eventually* gets decided) is best-effort and depends on
fewer than half the peers being faulty and the network eventually delivering
messages.

## Non-obvious decisions

> Each entry should link to the specific line(s) it describes.

### Acceptor accepts on `n >= highestSeen`, not `n > highestSeen`

See [Paxos.java:353](../paxos-core/src/main/java/paxos/Paxos.java#L353).

When a proposer wins phase 1 with proposal number `n`, the acceptor has already
recorded `highestSeen = n`. In phase 2 the same proposer sends Accept with the
same `n`. If we required strictly greater (`>`), the proposer's own Accept would
be rejected. Lamport's spec uses `>=` here for exactly this reason.

### Proposal number encoding: `(round << 8) | peerID`

See [Paxos.java:478-481](../paxos-core/src/main/java/paxos/Paxos.java#L478-L481).

Proposal numbers must be (a) globally unique across peers and (b) monotonically
increasing per proposer. Packing a per-peer round counter in the high bits and
the peer ID in the low 8 bits gives both: ties on the round are broken by peer
ID, and incrementing the round always produces a strictly higher number.

This caps the cluster at 256 peers. Fine for now; revisit if the cluster grows.

### Backoff on prepare failure

See [Paxos.java:183-188](../paxos-core/src/main/java/paxos/Paxos.java#L183-L188).

When two proposers race they can livelock — each one's Prepare invalidates the
other's, neither ever reaches Accept. Randomized backoff between retries breaks
the symmetry. The `1.5x` growth and 50ms cap are tuned for in-process tests; a
production deployment with real network latencies should raise the cap.

### Garbage collection via `Done`/`Min`

See [Paxos.java:453-472](../paxos-core/src/main/java/paxos/Paxos.java#L453-L472).

A peer can't forget instance `seq` until *every* peer has called `done(seq)` or
higher — otherwise a lagging peer could ask about it and get the wrong answer.
`min()` is `1 + min(peerDone)`, so any instance below `min()` is safe to drop.
`peerDone` is gossiped by piggybacking on Prepare RPCs.

## Layers above the protocol

- **kvstore** — replicated key-value store using `paxos-core` as the
  consensus log. See [kvpaxos.md](kvpaxos.md).
- **server** — HTTP demo driver that brings up a 3-peer cluster in one JVM.
- **checker** *(planned)* — model checker exploring schedules of Paxos
  instances to find safety violations.
