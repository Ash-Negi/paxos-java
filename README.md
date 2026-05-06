# paxos-java

A Java implementation of Lamport's Paxos consensus algorithm. The protocol is a
standalone library; a KV store and model checker are planned on top.

New to the codebase? Start with the [code walkthrough](docs/code-walkthrough.md).

## What's here

| Module | Purpose |
| --- | --- |
| [`paxos-core/`](paxos-core/) | The consensus protocol. Each peer is both proposer and acceptor; peers talk to each other over TCP. ~500 LOC, no runtime dependencies. |
| [`server/`](server/) | HTTP demo driver — brings up a 3-peer cluster in one JVM and exposes `/propose`, `/status`, `/done`, `/min`, `/max`, `/cluster`. |
| [`docs/`](docs/) | Protocol invariants, non-obvious design decisions, and a full code walkthrough — all with line-level links into the implementation. |

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

[docs/code-walkthrough.md](docs/code-walkthrough.md) walks the code from the
public API down to networking. [docs/paxos-protocol.md](docs/paxos-protocol.md)
covers the trickier protocol decisions (proposal-number encoding, the `>=` vs
`>` case in the acceptor, randomized backoff, garbage collection) with links
into the specific lines.

## Requirements

- JDK 21+
- Maven 3.9+
