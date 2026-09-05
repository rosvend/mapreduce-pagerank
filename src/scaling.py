"""Measures the cost of a MapReduce pass and extrapolates to bigger graphs.

Supplementary demo for the Spark discussion (Clase 5). Produces:
  results/scaling.png   -- time per iteration, and cost vs graph size
NOT part of the graded MapReduce deliverable.

Run: uv run --extra demo python src/scaling.py
"""

import os
import time

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

from mapreduce_framework import mapreduce
from pagerank import load_graph, make_reducer, mapper

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
RESULTS = os.path.join(HERE, "..", "results")

GRAPHS = ("web_graph_sample.txt", "web_graph_medium.txt", "web_graph_large.txt")


def timed_run(graph, d=0.85, max_iter=50, epsilon=1e-6):
    """Runs the loop, returning the wall time of every individual pass."""
    N = len(graph)
    ranks = {node: 1.0 / N for node in graph}
    items = [(node, (ranks[node], graph[node])) for node in graph]
    timings = []

    for _iteration in range(max_iter):
        start = time.perf_counter()
        dangling_sum = sum(rank for _node, (rank, adjacency) in items if not adjacency)
        result = mapreduce(items, mapper, make_reducer(d, N, dangling_sum))
        new_ranks = {node: rank for node, (rank, _adjacency) in result.items()}
        l1 = sum(abs(new_ranks[node] - ranks[node]) for node in ranks)
        timings.append(time.perf_counter() - start)

        ranks = new_ranks
        items = list(result.items())
        if l1 < epsilon:
            break
    return timings


def fit_line(xs, ys):
    """Least-squares slope and intercept, without numpy."""
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    spread = sum((x - mean_x) ** 2 for x in xs)
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / spread
    return slope, mean_y - slope * mean_x


if __name__ == "__main__":
    os.makedirs(RESULTS, exist_ok=True)

    measurements = []
    for filename in GRAPHS:
        graph = load_graph(os.path.join(DATA, filename))
        timings = timed_run(graph)
        N = len(graph)
        E = sum(len(adjacency) for adjacency in graph.values())
        measurements.append((filename, N, E, timings))
        print("%-22s N=%-6d E=%-6d passes=%-3d mean %.4f s/pass  total %.2f s"
              % (filename, N, E, len(timings), sum(timings) / len(timings), sum(timings)))

    # Cost per pass is O(N + E): fit seconds against the number of shuffled pairs.
    pairs = [N + E for _f, N, E, _t in measurements]
    per_pass = [sum(t) / len(t) for _f, _N, _E, t in measurements]
    slope, intercept = fit_line(pairs, per_pass)

    print("\nfit: seconds_per_pass = %.3e * (N + E) + %.3e" % (slope, intercept))
    print("\nextrapolation (edges scale with nodes, ~6.3 per node, 16 passes):")

    _f, base_N, base_E, base_timings = measurements[-1]
    passes = len(base_timings)
    projections = []
    for factor in (1, 10, 100):
        N = base_N * factor
        E = base_E * factor
        seconds = (slope * (N + E) + intercept) * passes
        projections.append((factor, N, E, seconds))
        print("  %4dx  N=%-9d E=%-9d %8.1f s per full run (%.1f min)"
              % (factor, N, E, seconds, seconds / 60))

    figure, (left, right) = plt.subplots(1, 2, figsize=(12, 4.6))

    left.bar(range(1, passes + 1), base_timings, color="#2a6df4")
    left.set_xlabel("iteration")
    left.set_ylabel("seconds")
    left.set_title("Time per MapReduce pass\nweb_graph_large.txt (N=%d, E=%d)" % (base_N, base_E))
    left.grid(axis="y", alpha=0.3)

    right.plot([p[1] for p in projections], [p[3] for p in projections], "o-", color="#e4572e")
    for factor, N, _E, seconds in projections:
        right.annotate("%dx: %.0f s" % (factor, seconds), (N, seconds),
                       textcoords="offset points", xytext=(8, -12), fontsize=9)
    right.set_xscale("log")
    right.set_yscale("log")
    right.set_xlabel("nodes (log)")
    right.set_ylabel("seconds for a full %d-pass run (log)" % passes)
    right.set_title("Projected cost: every pass re-materialises\nall N+E pairs, nothing is cached")
    right.grid(alpha=0.3, which="both")

    figure.tight_layout()
    destination = os.path.join(RESULTS, "scaling.png")
    figure.savefig(destination, dpi=130)
    print("\nSaved %s" % os.path.normpath(destination))
