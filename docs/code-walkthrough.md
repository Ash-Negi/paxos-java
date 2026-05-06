# Code Walkthrough: How This Paxos Implementation Works

A walkthrough of `paxos-core` from the outside in: public API, then the protocol
phases, then the networking and memory management underneath.

All line references point into
[`paxos-core/src/main/java/paxos/Paxos.java`](../paxos-core/src/main/java/paxos/Paxos.java).

---

## Background: what problem does Paxos solve?

Imagine three servers that all need to agree on the same sequence of commands
(like a replicated log). Any one of them might crash or lose messages at any
time. Paxos is an algorithm that guarantees:

- **Safety** — if two servers both "decide" a value for slot N, it is the same
  value. Agreement is never violated, even under crashes and message loss.
- **Liveness** — as long as a majority of servers are reachable, something
  eventually gets decided.

This implementation runs **Multi-Paxos** (one independent Paxos round per
sequence number), where each peer is simultaneously a proposer and an acceptor.

---

## The public API

Four methods are all an application ever touches:

| Method | What it does |
|---|---|
| `start(seq, v)` | Begin trying to reach agreement on slot `seq` with proposed value `v`. Returns immediately; the round runs in the background. |
| `status(seq)` | Check whether slot `seq` has been decided locally, and if so return its value. Does not contact other peers. |
| `done(seq)` | Tell the library "my application has consumed everything up to and including `seq`; you can reclaim that memory." |
| `min()` | Returns the lowest sequence number the library still remembers. Slots below this have been freed. |

The typical application loop looks like:

```java
paxos.start(seq, myValue);
while (true) {
    StatusResult r = paxos.status(seq);
    if (r.decided()) { process(r.value()); break; }
    Thread.sleep(10);
}
paxos.done(seq);
```

---

## Key data structures

### `Instance` — per-slot state ([line 29](../paxos-core/src/main/java/paxos/Paxos.java#L29))

One `Instance` exists per sequence number. It is the memory for exactly one
round of agreement and maps directly onto the variables in Lamport's original
paper:

| Field | Protocol name | Meaning |
|---|---|---|
| `highestSeen` | Np | Highest proposal number this acceptor has promised to honour |
| `highestAcN` | Na | Proposal number of the last value this acceptor accepted |
| `highestAcV` | Va | The actual last-accepted value |
| `decided` | — | Whether consensus was reached for this slot |
| `decidedV` | — | The final decided value |

The `highestAcN`/`highestAcV` pair (Na/Va) is what makes Paxos safe: if any
acceptor reports a previously accepted value, the new proposer must carry that
value forward instead of its own. This prevents two different values from ever
being decided for the same slot.

`Instance implements Serializable` because acceptor state is exchanged over TCP.

The library keeps **two separate maps** of instances
([lines 72–73](../paxos-core/src/main/java/paxos/Paxos.java#L72)):

```java
Map<Integer, Instance> instances;    // decided values (what status() reads)
Map<Integer, Instance> acceptorIns;  // acceptor voting state (what handlers write)
```

Separating them makes `status()` cheap — it never has to look at in-progress
voting state — and makes garbage collection simpler.

### RPC message types ([lines 39–56](../paxos-core/src/main/java/paxos/Paxos.java#L39))

Each Paxos phase has a matching `Args` / `Reply` pair. They are plain Java
objects sent over a TCP socket using Java object serialization (the closest
equivalent of Go's `net/rpc`):

```
PrepareArgs / PrepareReply  — Phase 1
AcceptArgs  / AcceptReply   — Phase 2
DecideArgs  / DecideReply   — Phase 3
```

---

## The three protocol phases

Calling `start(seq, v)` submits `propose(seq, v)` to a background thread pool
([line 117](../paxos-core/src/main/java/paxos/Paxos.java#L117)). The proposer
loops until it either wins or sees that the slot was already decided by someone
else.

### Phase 1 — Prepare (lines 208–259)

The proposer picks a proposal number `n` that is higher than anything it has
seen before, then broadcasts `Prepare(seq, n)` to all peers, including itself.

**Proposal number encoding** ([lines 479–481](../paxos-core/src/main/java/paxos/Paxos.java#L479)):

```java
int newID = (highestNSeen >> PEER_ID_BITS) + 1;
return (newID << PEER_ID_BITS) | peerID;
```

A proposal number packs two things into one integer:
- The high bits are a per-proposer round counter (incremented on each retry).
- The low 8 bits are the peer ID.

This guarantees two things simultaneously: proposal numbers are unique across
all peers (different peer IDs break ties), and they are monotonically increasing
per proposer (incrementing the round always produces a strictly larger number).

**The acceptor's response** ([lines 315–343](../paxos-core/src/main/java/paxos/Paxos.java#L315)):

An acceptor promises to honour proposal `n` only if `n` is strictly greater than
the highest proposal number it has already promised (`highestSeen`). If it agrees,
it replies with `(Na, Va)`: the proposal number and value it last accepted, if
any. The proposer collects all replies and, if a majority said OK, moves to
Phase 2.

**Why does the acceptor send back its last accepted value?**  
If some earlier proposer already got a value accepted by a majority, this
mechanism forces the new proposer to carry that value forward instead of
proposing its own. This is what makes Paxos safe: once a value reaches a
majority of acceptors, no future proposer can push a different one through.

When a majority of Prepare replies come back with `ok = true`, the proposer
picks its Phase 2 value:

```java
// If any acceptor reported a previously accepted value, use the one with
// the highest Na. Otherwise, stick with the caller's original v.
if (reply.na > highestNAccepted) { highestNAccepted = reply.na; nextPhaseV = reply.va; }
```

### Phase 2 — Accept (lines 261–294)

The proposer sends `Accept(seq, n, v)` to all peers. An acceptor agrees if `n`
is greater than or equal to `highestSeen`
([line 354](../paxos-core/src/main/java/paxos/Paxos.java#L354)):

```java
if (args.n >= acc.highestSeen) { ... reply.ok = true; }
```

The `>=` (not `>`) is deliberate: the proposer already won Phase 1 with this
exact `n`, so the acceptor has `highestSeen = n`. Requiring strictly greater
would cause the proposer to reject its own Accept message.

If a majority accept, the value is now durable. No future round can overwrite it.

### Phase 3 — Decide (lines 296–308)

Once Accept succeeds on a majority, the proposer broadcasts `Decide(seq, v)`.
Every peer that receives Decide stores the value in its `instances` map and marks
the slot as decided. This is the "learn" phase: it does not change any votes, it
just propagates the result so `status()` can return it locally without needing
another round trip.

---

## Networking

### The listener ([lines 407–448](../paxos-core/src/main/java/paxos/Paxos.java#L407))

Each `Paxos` object starts a daemon thread that accepts TCP connections on its
configured port. For each accepted connection it dispatches to a thread pool,
reads the method name and args from the socket, calls the right handler
(`rpcPrepare`, `rpcAccept`, or `rpcDecide`), and writes the reply back.

### Fault injection ([lines 413–419](../paxos-core/src/main/java/paxos/Paxos.java#L413))

The listener has two test modes:

- **`unreliable`** — randomly drops 10% of incoming connections and silently
  discards 20% of replies (the reply is computed but never sent). This simulates
  a lossy network.
- **`disabled`** — closes every incoming connection immediately, simulating a
  peer that is reachable at the TCP level but not participating (a "deaf" peer).

These hooks let tests create adversarial conditions without a real network.

### `rpcCall` — outbound calls ([lines 391–405](../paxos-core/src/main/java/paxos/Paxos.java#L391))

Each outbound RPC opens a fresh TCP connection, serializes `(method, args)`,
reads back the reply, and closes the socket. There is a 500 ms connect timeout
and a 1 s read timeout. Any failure returns `null`, matching the Go reference
implementation's pattern of returning `false` on a failed call. The proposer
treats a null reply as a rejected vote and moves on.

---

## Memory management — `Done` and `Min`

A long-running Paxos log would grow forever without cleanup. The protocol
includes a distributed garbage-collection scheme:

1. The application calls `done(seq)` to mark that it has consumed everything
   through `seq`.
2. Each peer gossips its own done value by piggybacking it on Prepare RPCs
   ([lines 214, 241](../paxos-core/src/main/java/paxos/Paxos.java#L214)). This
   costs nothing extra: the proposer already sends a Prepare to every peer on
   every retry.
3. `min()` returns `1 + min(peerDone[i] for all i)`. Any slot below `min()` has
   been consumed by every peer and is safe to discard.
4. `collectGarbage()` removes those entries from both instance maps
   ([lines 454–466](../paxos-core/src/main/java/paxos/Paxos.java#L454)).

**The invariant:** a peer cannot free slot `seq` until every other peer has
called `done(seq)` or higher. If it freed early and a lagging peer asked about
it, the lagging peer would get a wrong answer. `min()` enforces this
cluster-wide lower bound.

---

## Liveness — handling contention with backoff

Two proposers racing on the same slot can livelock: A's Prepare invalidates B's
ongoing round, and B's next Prepare invalidates A's, indefinitely. The fix is
randomized exponential backoff
([lines 183–188](../paxos-core/src/main/java/paxos/Paxos.java#L183)):

```java
penaltySleep = (int)(penaltySleep * 1.5f);
if (penaltySleep > 50) penaltySleep = 50;
int sleepMs = rand.nextInt(penaltySleep) + penaltySleep;
Thread.sleep(sleepMs);
```

On each failed attempt the proposer sleeps for a random duration that grows up
to a 50 ms cap. Randomness breaks symmetry: two equally-eager proposers will
rarely retry at the same moment, so one eventually completes a full round
uncontested.
