# paxos-java

A Java implementation of Lamport's Paxos consensus algorithm, structured as a
modular library to support layers built on top of it (replicated KV store,
model checker).

## What's here

| Module | Purpose |
| --- | --- |
| [`paxos-core/`](paxos-core/) | The consensus protocol. Each peer is both proposer and acceptor; peers talk to each other over TCP. ~500 LOC, no runtime dependencies. |
| [`server/`](server/) | HTTP demo driver — brings up a 3-peer cluster in one JVM and exposes `/propose`, `/status`, `/done`, `/min`, `/max`, `/cluster`. |
| [`docs/`](docs/) | Protocol invariants and the rationale behind non-obvious decisions, with line-level links into the implementation. |

## Status

- [x] Basic Paxos: Prepare, Accept, Decide phases
- [x] Log compaction via `Done` / `Min` (peers gossip their done sequence)
- [x] Failure tests: unreliable network, deaf peers, late-joining peers, RPC-count bounds
- [ ] `kvstore/` — replicated key-value store on top of `paxos-core`
- [ ] `checker/` — model checker exploring schedules to surface safety violations

## Tests

Seven tests cover correctness under both happy-path and adversarial conditions
(see [PaxosTest.java](paxos-core/src/test/java/paxos/PaxosTest.java)):

```
mvn test
```

## Run the demo

```
mvn package
java -cp "server/target/server-1.0.0-SNAPSHOT.jar:paxos-core/target/paxos-core-1.0.0-SNAPSHOT.jar" server.Main
```

Then drive the cluster from another shell:

```
curl -X POST localhost:8080/propose -d '{"seq":0,"value":"hello","peer":0}'
curl 'localhost:8080/status?seq=0&peer=0'
```

## Design notes

The protocol-level decisions worth knowing about — proposal-number encoding,
the `>=` vs `>` case in the acceptor, randomized backoff, garbage collection —
are documented in [docs/paxos-protocol.md](docs/paxos-protocol.md) with links
to the specific lines they describe.

## Requirements

- JDK 21+
- Maven 3.9+
