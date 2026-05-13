# bench/ — scalability experiment

Reproduces the throughput and RPC-cost curves for `paxos-core` as cluster size
grows from N=3 to N=13, with and without proposer contention. The Java tests
produce the raw numbers; the Python script renders them as charts.

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

- `rpcs_per_agreement.png` — actual RPCs/agreement vs the theoretical
  minimum of `3·(N−1)`. The gap widens with N, showing wasted work from
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
serializes the incoming burst. The overhead compounds faster than linearly.

This matches the analysis in Aleksey Charapko's 2018 work on Paxos pipeline
saturation: the bottleneck is the leader's message-processing capacity, not
the network. It's also one reason production consensus systems cap at 3–5
nodes and scale out by sharding instead.

## Dependencies

- Python 3.9+
- matplotlib (`pip install matplotlib`)
