# bench/ — scalability experiment

Reproduces the throughput and RPC-cost curves for `paxos-core` as cluster size
grows from N=3 to N=13, with and without proposer contention. The Java tests
produce the raw numbers; the Python script renders them as charts.

## Headline results

| Cluster              | RPCs / agreement | Throughput (agreements/sec) |
| ---                  | ---              | ---                         |
| N=3,  no contention  | 6                | 81.6                        |
| N=13, no contention  | 36               | 81.0                        |
| N=3,  full contention| 11.2             | 81.3                        |
| N=13, full contention| 201.6            | 32.2                        |

With one active proposer, throughput is flat from N=3 to N=13 — Paxos pays
its expected `3·(N−1)` RPCs per agreement and nothing more. When every peer
competes for the same slot, RPC cost grows to more than 5x the no-contention
minimum, and throughput falls ~60%.

## Setup

The cluster runs in a single JVM with peers talking over loopback TCP. That
removes the network from the picture on purpose: any slowdown you see is
protocol work, not packet loss or queueing in a switch. Same reasoning as
benching a database with the client on the same box first — establish the
ceiling before adding variables.

Each test runs once per cluster size. Re-runs on the same machine vary by
~5%, so this is a characterization, not a performance benchmark.

## Run the experiment

```
mvn test -pl paxos-core -Dtest=PaxosTest#testScalabilityByClusterSize
mvn test -pl paxos-core -Dtest=PaxosTest#testScalabilityUnderContention
```

Each test prints a small table to stdout. The first measures a single proposer
running sequential agreements (no contention — establishes a baseline). The
second has all N peers propose distinct values at the same log slot, forcing
collisions and retries.

## Generate the charts

Update the `BASELINE` and `CONTENTION` dicts in
[plot_scalability.py](plot_scalability.py) with your numbers, then:

```
python3 bench/plot_scalability.py
```

Writes two PNGs into this directory:

- `rpcs_per_agreement.png` — actual RPCs/agreement vs the minimum (no
  contention) of `3·(N−1)`. The gap widens with N, showing wasted work from
  failed Prepares and retries.
- `throughput_vs_cluster_size.png` — agreements/sec for both runs. Baseline
  is flat; contention drops ~60% by N=13.

## What the charts show

Under a single proposer, throughput is roughly constant from N=3 to N=13 —
loopback TCP is fast, so adding peers doesn't meaningfully slow each round.
RPCs/agreement sit exactly at `3·(N−1)`.

Under contention the picture inverts. Every extra peer adds another competing
proposer, so more Prepares get superseded, more retries fire, and the
acceptor mutex inside `rpcPrepare` ([Paxos.java:317](../paxos-core/src/main/java/paxos/Paxos.java#L317))
serializes the incoming burst. The overhead compounds with every node added.

This implementation is leaderless, so the choke point is each acceptor's
serialized Prepare handling rather than one leader's processing pipeline.
Aleksey Charapko's 2018 [analysis of Multi-Paxos scalability](https://charap.co/do-not-blame-only-network-for-your-paxos-scalability/)
makes the broader point that motivated the experiment: in real deployments
the bottleneck is consensus work itself, not the network. The mechanism is
different here, but the conclusion is the same — and it's one reason
production consensus systems cap at 3–5 nodes and scale out by sharding.

## Limitations

- Single trial per data point. Re-runs vary by ~5%; no error bars.
- Full contention only — every peer competes every slot. Real workloads sit
  somewhere between this and the baseline.
- In-JVM means peers share CPU; at N=13 the OS scheduler is already a factor.
- No failure injection. The unreliable-network and deaf-peer tests in
  `paxos-core` cover that case separately.

## Dependencies

- Python 3.9+
- matplotlib (`pip install matplotlib`)
