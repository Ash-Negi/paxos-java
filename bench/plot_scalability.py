"""
Generate the scalability charts from the paxos-core scalability tests.

Update the DATA dicts below with the numbers printed by
  mvn test -pl paxos-core -Dtest=PaxosTest#testScalabilityByClusterSize
  mvn test -pl paxos-core -Dtest=PaxosTest#testScalabilityUnderContention
then run:
  python3 bench/plot_scalability.py

Outputs two PNGs in bench/:
  rpcs_per_agreement.png
  throughput_vs_cluster_size.png
"""

import os
import matplotlib.pyplot as plt

SIZES = [3, 5, 7, 9]

# From testScalabilityByClusterSize (single proposer, no contention)
BASELINE = {
    "throughput":      [72.5, 81.3, 82.6, 79.7],  # agreements/sec
    "rpcs_per_agree":  [6.0, 12.0, 18.0, 24.0],
}

# From testScalabilityUnderContention (all N peers propose per slot)
CONTENTION = {
    "throughput":      [79.4, 78.7, 78.1, 45.9],
    "rpcs_per_agree":  [10.6, 33.2, 61.8, 105.6],
}

THEORETICAL_MIN = [3 * (n - 1) for n in SIZES]

OUT_DIR = os.path.dirname(os.path.abspath(__file__))


def style():
    plt.rcParams.update({
        "font.family":        "sans-serif",
        "font.size":          12,
        "axes.spines.top":    False,
        "axes.spines.right":  False,
        "axes.grid":          True,
        "grid.alpha":         0.25,
        "grid.linestyle":     "--",
        "figure.facecolor":   "white",
        "axes.facecolor":     "white",
    })


def plot_rpcs():
    fig, ax = plt.subplots(figsize=(9, 5.5), dpi=150)
    ax.plot(SIZES, CONTENTION["rpcs_per_agree"], "o-",
            color="#c0392b", linewidth=2.5, markersize=9,
            label="Under contention (all N peers propose)")
    ax.plot(SIZES, THEORETICAL_MIN, "s--",
            color="#7f8c8d", linewidth=2, markersize=7,
            label="Theoretical minimum: 3·(N−1)")

    ax.set_xlabel("Cluster size (N)")
    ax.set_ylabel("RPCs per agreement")
    ax.set_title("RPC cost grows super-linearly under contention",
                 fontsize=14, pad=15)
    ax.set_xticks(SIZES)
    ax.legend(loc="upper left", frameon=False)

    # Annotate the gap at N=9
    ax.annotate(f"{CONTENTION['rpcs_per_agree'][-1]:.0f} actual\nvs {THEORETICAL_MIN[-1]} minimum",
                xy=(9, CONTENTION["rpcs_per_agree"][-1]),
                xytext=(7.2, 80),
                fontsize=10, color="#2c3e50",
                arrowprops=dict(arrowstyle="->", color="#7f8c8d", lw=1))

    fig.tight_layout()
    path = os.path.join(OUT_DIR, "rpcs_per_agreement.png")
    fig.savefig(path, bbox_inches="tight")
    print("wrote", path)


def plot_throughput():
    fig, ax = plt.subplots(figsize=(9, 5.5), dpi=150)
    ax.plot(SIZES, BASELINE["throughput"], "o-",
            color="#2c3e50", linewidth=2.5, markersize=9,
            label="No contention (single proposer)")
    ax.plot(SIZES, CONTENTION["throughput"], "o-",
            color="#c0392b", linewidth=2.5, markersize=9,
            label="Under contention (all N peers)")

    ax.set_xlabel("Cluster size (N)")
    ax.set_ylabel("Agreements per second")
    ax.set_title("Throughput cliff at N=9 — only under contention",
                 fontsize=14, pad=15)
    ax.set_xticks(SIZES)
    ax.set_ylim(0, max(BASELINE["throughput"]) * 1.15)
    ax.legend(loc="lower left", frameon=False)

    # Annotate the cliff
    drop_pct = (1 - CONTENTION["throughput"][-1] / CONTENTION["throughput"][0]) * 100
    ax.annotate(f"−{drop_pct:.0f}% throughput",
                xy=(9, CONTENTION["throughput"][-1]),
                xytext=(7.4, 30),
                fontsize=10, color="#c0392b",
                arrowprops=dict(arrowstyle="->", color="#c0392b", lw=1))

    fig.tight_layout()
    path = os.path.join(OUT_DIR, "throughput_vs_cluster_size.png")
    fig.savefig(path, bbox_inches="tight")
    print("wrote", path)


if __name__ == "__main__":
    style()
    plot_rpcs()
    plot_throughput()
