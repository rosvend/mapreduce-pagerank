"""
An item is (node, (rank, adjacency)). The mapper emits two kinds of messages:
  STRUCT -> keyed to the node itself, carries its adjacency list through the pass
  RANK   -> keyed to each out-neighbour, carries an equal share of the node's rank
The reducer sums the RANK messages, re-attaches the adjacency from the STRUCT
message, and returns (new_rank, adjacency): the same shape as an input value, so
one pass can feed the next.
"""

import os
import sys
import time
from collections import Counter

from mapreduce_framework import mapreduce


def load_graph(path):
    """Reads 'node: neighbour1 neighbour2' lines into {node: [out-neighbours]}."""
    graph = {}
    with open(path) as lines:
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            node, _colon, neighbours = line.partition(":")
            graph[node.strip()] = neighbours.split()

    # A node that is linked to but never declared would reach the reducer without a
    # STRUCT message, so declare it here as a dangling node.
    for adjacency in list(graph.values()):
        for neighbour in adjacency:
            graph.setdefault(neighbour, [])
    return graph


def mapper(item):
    """Emits one STRUCT message for the node and one RANK message per out-link."""
    node, (rank, adjacency) = item

    yield (node, ("STRUCT", adjacency))

    # A dangling node has no out-links: it emits no RANK messages and never divides by zero.
    if adjacency:
        share = rank / len(adjacency)
        for neighbour in adjacency:
            yield (neighbour, ("RANK", share))


def make_reducer(d, N, dangling_sum):
    """Builds the reducer, closing over the three values that are global to the iteration.

    d and N are constants; dangling_sum is the rank sitting on dangling nodes this
    pass, which the outer loop computes because neither mapper nor reducer can see
    the whole vector. The closure keeps mapreduce() untouched.
    """

    def reducer(node, values):
        adjacency = None
        rank_sum = 0.0
        for tag, payload in values:
            if tag == "STRUCT":
                adjacency = payload
            elif tag == "RANK":
                rank_sum += payload

        # teleport + uniform share of the leaked dangling rank + incoming contributions
        new_rank = (1 - d) / N + d * (dangling_sum / N) + d * rank_sum
        return (new_rank, adjacency)

    return reducer


def run_pagerank(graph, d=0.85, max_iter=50, epsilon=1e-6):
    """Iterates mapreduce() until the ranks stop moving.

    The loop lives here, outside the framework: mapreduce() is one pass by
    definition. Returns (ranks, iterations_used, converged).
    """
    N = len(graph)
    ranks = {node: 1.0 / N for node in graph}
    items = [(node, (ranks[node], graph[node])) for node in graph]

    for iteration in range(max_iter):
        # Rank stuck on dangling nodes: only the loop can see the whole vector.
        dangling_sum = sum(rank for _node, (rank, adjacency) in items if not adjacency)

        result = mapreduce(items, mapper, make_reducer(d, N, dangling_sum))

        new_ranks = {node: rank for node, (rank, _adjacency) in result.items()}
        l1 = sum(abs(new_ranks[node] - ranks[node]) for node in ranks)
        ranks = new_ranks

        # The next pass reuses the adjacency the STRUCT messages just carried through.
        items = list(result.items())

        if l1 < epsilon:
            return ranks, iteration + 1, True

    return ranks, max_iter, False


def top_ranked(ranks, graph, count=15):
    """Returns the highest ranked nodes as (node, rank, in-degree) triples."""
    in_degree = Counter(neighbour for adjacency in graph.values() for neighbour in adjacency)
    ordered = sorted(ranks.items(), key=lambda pair: pair[1], reverse=True)
    return [(node, rank, in_degree[node]) for node, rank in ordered[:count]]


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    default = os.path.join(here, "..", "data", "web_graph_large.txt")
    path = sys.argv[1] if len(sys.argv) > 1 else default

    graph = load_graph(path)
    edges = sum(len(adjacency) for adjacency in graph.values())
    dangling = sum(1 for adjacency in graph.values() if not adjacency)

    start = time.time()
    ranks, iterations, converged = run_pagerank(graph)
    elapsed = time.time() - start

    lines = [
        "Graph:        %s" % os.path.basename(path),
        "Size:         %d nodes | %d edges | %d dangling" % (len(graph), edges, dangling),
        "Iterations:   %d (converged: %s)" % (iterations, converged),
        "Sum of ranks: %.12f" % sum(ranks.values()),
        "Elapsed:      %.2f s" % elapsed,
        "",
        "%-4s %-10s %-14s %s" % ("#", "node", "pagerank", "in-degree"),
    ]
    for position, (node, rank, in_degree) in enumerate(top_ranked(ranks, graph), 1):
        lines.append("%-4d %-10s %-14.8f %d" % (position, node, rank, in_degree))

    report = "\n".join(lines)
    print(report)

    results = os.path.join(here, "..", "results")
    os.makedirs(results, exist_ok=True)
    destination = os.path.join(results, "top15_" + os.path.basename(path))
    with open(destination, "w") as output:
        output.write(report + "\n")
    print("\nSaved to %s" % os.path.normpath(destination))
