# paxos-java

A Java implementation of Lamport's Paxos consensus algorithm and a replicated
key-value store on top of it. A model checker is planned next.

New to the codebase? Start with the [code walkthrough](docs/code-walkthrough.md).

## What's here

| Module | Purpose |
| --- | --- |
| [`paxos-core/`](paxos-core/) | The consensus protocol. Each peer is both proposer and acceptor; peers talk over TCP. ~500 LOC, no runtime dependencies. |
| [`kvstore/`](kvstore/) | Replicated key-value store layered on `paxos-core`. Handles client retries with at-most-once semantics. |
| [`server/`](server/) | HTTP demo — brings up a 3-peer Paxos cluster in one JVM and exposes `/propose`, `/status`, `/done`, `/min`, `/max`, `/cluster`. |
| [`docs/`](docs/) | Protocol invariants, design decisions, and a code walkthrough, with line-level links into the implementation. |
| [`bench/`](bench/) | Scalability experiment: measures RPC cost and throughput at N=3–9 with and without proposer contention. Includes a Python script that generates the result charts. |

## Status

- [x] Basic Paxos: Prepare, Accept, Decide phases
- [x] Log compaction via `Done` / `Min` (peers gossip their done sequence)
- [x] Failure tests: unreliable network, deaf peers, late-joining peers, RPC-count bounds
- [x] `kvstore/`: replicated key-value store with linearizable Get/Put and at-most-once Put
- [ ] `checker/`: model checker exploring schedules to surface safety violations

## Roadmap

**In progress**
- Sharded KV store across multiple Paxos groups ([#1](https://github.com/Ash-Negi/paxos-java/issues/1))
- Model checker for exhaustive schedule exploration ([#5](https://github.com/Ash-Negi/paxos-java/issues/5))

**Durability and recovery**
- Write-ahead log for crash safety ([#2](https://github.com/Ash-Negi/paxos-java/issues/2))
- Embedded RocksDB in place of the in-memory map ([#4](https://github.com/Ash-Negi/paxos-java/issues/4))
- Log catch-up and snapshot transfer for rejoining nodes ([#3](https://github.com/Ash-Negi/paxos-java/issues/3))

**Operability**
- Prometheus metrics and Grafana dashboard ([#6](https://github.com/Ash-Negi/paxos-java/issues/6))
- REST API over the KV store ([#7](https://github.com/Ash-Negi/paxos-java/issues/7))
- Kubernetes deployment on minikube ([#8](https://github.com/Ash-Negi/paxos-java/issues/8))

## Tests

```
mvn test
```

- `paxos-core` — 9 tests covering happy-path agreement plus adversarial
  conditions: unreliable networks, deaf peers, late-joining peers, RPC-count
  bounds, and two scalability characterization tests that measure RPC cost and
  throughput at cluster sizes N=3–9 with and without proposer contention. See
  [PaxosTest.java](paxos-core/src/test/java/paxos/PaxosTest.java).
- `kvstore` — 3 tests covering basic put/get, concurrent clients, and
  unreliable-network behavior. See
  [KVPaxosTest.java](kvstore/src/test/java/kvstore/KVPaxosTest.java).

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

## Run on Kubernetes (minikube)

Brings up a 3-peer Paxos cluster as a StatefulSet, one peer per pod. Pods
discover each other via the headless `paxos` Service; clients hit any peer
through the load-balanced `paxos-client` Service. There is no persistent
storage yet — pod restarts wipe Paxos state (tracked in
[#2](https://github.com/Ash-Negi/paxos-java/issues/2) and
[#4](https://github.com/Ash-Negi/paxos-java/issues/4)).

```
# Build the image inside minikube's Docker daemon so the cluster can pull it.
minikube start
eval $(minikube docker-env)
docker build -t paxos-server:latest .

# Deploy.
kubectl apply -f k8s/

# Watch pods come up.
kubectl get pods -l app=paxos -w

# Drive the cluster.
minikube service paxos-client --url
# -> http://127.0.0.1:<port>
curl -X POST http://127.0.0.1:<port>/propose -d '{"seq":0,"value":"hello"}'
curl "http://127.0.0.1:<port>/status?seq=0"
```

In Kubernetes mode each pod serves only its own peer — the `peer` field in
requests is optional and ignored when set. The `paxos-client` service spreads
requests across pods; for inspecting a specific peer, port-forward:
`kubectl port-forward paxos-1 8080:8080`.

To scale, edit the `replicas` field in [k8s/statefulset.yaml](k8s/statefulset.yaml)
and the `PEERS` list in [k8s/configmap.yaml](k8s/configmap.yaml) to match,
then re-apply.

## Design notes

- [docs/code-walkthrough.md](docs/code-walkthrough.md) — the code from the
  public API down to networking.
- [docs/paxos-protocol.md](docs/paxos-protocol.md) — protocol-level decisions
  (proposal-number encoding, `>=` vs `>` in the acceptor, randomized backoff,
  garbage collection) with line-level links.
- [docs/kvpaxos.md](docs/kvpaxos.md) — the kvstore layer (request dedup,
  serialized agreement, catch-up, idempotency).

## Requirements

- JDK 21+
- Maven 3.9+
