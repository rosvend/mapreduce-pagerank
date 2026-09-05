"""Test cases for PageRank on MapReduce (assignment section 6.1).

Covers: a trivial chain verifiable by hand, a cycle whose ranks must be equal by
symmetry, a dangling node, and the sum-of-ranks = 1.0 invariant on every pass.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from mapreduce_framework import mapreduce
from pagerank import load_graph, make_reducer, mapper, run_pagerank

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")

# A -> B -> C, with C dangling. Small enough to compute with pencil and paper.
CHAIN = {"A": ["B"], "B": ["C"], "C": []}

# A -> B -> C -> A. Perfectly symmetric: every rank must end up at 1/3.
CYCLE = {"A": ["B"], "B": ["C"], "C": ["A"]}

# The worked example traced in DESIGN_ROYSANDOVAL.md section 2. E is dangling.
TRACE = {"A": ["B", "C"], "B": ["C"], "C": ["A"], "E": []}


def initial_items(graph):
    """Builds the iteration-0 item list: one (node, (rank, adjacency)) per node."""
    N = len(graph)
    return [(node, (1.0 / N, graph[node])) for node in graph]


def one_pass(items, d, N):
    """Runs a single MAP -> SHUFFLE -> REDUCE pass, exactly as run_pagerank does."""
    dangling_sum = sum(rank for _node, (rank, adjacency) in items if not adjacency)
    return mapreduce(items, mapper, make_reducer(d, N, dangling_sum))


class TestMapper(unittest.TestCase):
    """The mapper must emit two message types: STRUCT (structure) and RANK (contribution)."""

    def test_emits_one_struct_plus_one_rank_per_outlink(self):
        emitted = list(mapper(("A", (0.4, ["B", "C"]))))

        self.assertEqual(len(emitted), 3)
        self.assertIn(("A", ("STRUCT", ["B", "C"])), emitted)
        self.assertIn(("B", ("RANK", 0.2)), emitted)
        self.assertIn(("C", ("RANK", 0.2)), emitted)

    def test_dangling_node_emits_only_its_struct(self):
        """No out-links means no RANK messages, and therefore no division by zero."""
        emitted = list(mapper(("E", (0.25, []))))

        self.assertEqual(emitted, [("E", ("STRUCT", []))])


class TestReducer(unittest.TestCase):
    """The reducer must rebuild the adjacency from STRUCT and apply the damping formula."""

    def test_sums_rank_messages_and_recovers_adjacency(self):
        reducer = make_reducer(d=0.85, N=4, dangling_sum=0.25)

        new_rank, adjacency = reducer("C", [("STRUCT", ["A"]), ("RANK", 0.125), ("RANK", 0.25)])

        self.assertAlmostEqual(new_rank, 0.409375)
        self.assertEqual(adjacency, ["A"])

    def test_node_with_no_inlinks_still_gets_teleport_and_dangling_share(self):
        reducer = make_reducer(d=0.85, N=4, dangling_sum=0.25)

        new_rank, adjacency = reducer("E", [("STRUCT", [])])

        self.assertAlmostEqual(new_rank, 0.090625)
        self.assertEqual(adjacency, [])


class TestTrivialChain(unittest.TestCase):
    """A -> B -> C: the whole first pass is computable by hand."""

    def test_first_pass_matches_hand_computation(self):
        # dangling_sum = rank(C) = 1/3, so every node starts from
        # base = 0.15/3 + 0.85 * (1/3)/3 = 13/90 = 0.1444...
        # A receives nothing; B and C each receive 1/3, so they get base + 0.85/3 = 77/180.
        result = one_pass(initial_items(CHAIN), d=0.85, N=3)

        self.assertAlmostEqual(result["A"][0], 13.0 / 90.0)
        self.assertAlmostEqual(result["B"][0], 77.0 / 180.0)
        self.assertAlmostEqual(result["C"][0], 77.0 / 180.0)
        self.assertAlmostEqual(sum(rank for rank, _adj in result.values()), 1.0)

    def test_converged_ranks_increase_along_the_chain(self):
        """Rank flows forward, so C (the sink) must end above B, and B above A."""
        ranks, _iterations, converged = run_pagerank(CHAIN)

        self.assertTrue(converged)
        self.assertLess(ranks["A"], ranks["B"])
        self.assertLess(ranks["B"], ranks["C"])
        self.assertAlmostEqual(sum(ranks.values()), 1.0)


class TestCycle(unittest.TestCase):
    """A -> B -> C -> A: symmetry forces all three ranks to be identical."""

    def test_all_ranks_equal_one_third(self):
        ranks, iterations, converged = run_pagerank(CYCLE)

        self.assertTrue(converged)
        for node in CYCLE:
            self.assertAlmostEqual(ranks[node], 1.0 / 3.0)
        # The uniform vector is already the fixed point, so one pass changes nothing.
        self.assertEqual(iterations, 1)


class TestDanglingNode(unittest.TestCase):
    """E has no out-links: its rank must be redistributed, not leaked."""

    def test_first_pass_matches_the_design_document_trace(self):
        result = one_pass(initial_items(TRACE), d=0.85, N=4)

        self.assertAlmostEqual(result["A"][0], 0.303125)
        self.assertAlmostEqual(result["B"][0], 0.196875)
        self.assertAlmostEqual(result["C"][0], 0.409375)
        self.assertAlmostEqual(result["E"][0], 0.090625)

    def test_dangling_node_survives_as_a_key_although_nobody_links_to_it(self):
        result = one_pass(initial_items(TRACE), d=0.85, N=4)

        self.assertIn("E", result)
        self.assertEqual(len(result), 4)
        self.assertEqual(result["E"][1], [])

    def test_converged_run_keeps_every_node_and_sums_to_one(self):
        ranks, _iterations, converged = run_pagerank(TRACE)

        self.assertTrue(converged)
        self.assertEqual(set(ranks), set(TRACE))
        self.assertAlmostEqual(sum(ranks.values()), 1.0)


class TestSumInvariant(unittest.TestCase):
    """Section 6.1: the ranks must sum to 1.0 after EVERY pass, not just at the end.

    A drifting sum is the direct signal that dangling-node handling is broken.
    """

    def assert_invariant_every_pass(self, graph, d=0.85, max_iter=50, epsilon=1e-6):
        N = len(graph)
        ranks = {node: 1.0 / N for node in graph}
        items = initial_items(graph)

        for passes in range(1, max_iter + 1):
            result = one_pass(items, d, N)

            self.assertEqual(len(result), N, "a node disappeared on pass %d" % passes)
            total = sum(rank for rank, _adj in result.values())
            self.assertAlmostEqual(total, 1.0, places=9, msg="sum drifted on pass %d" % passes)

            new_ranks = {node: rank for node, (rank, _adj) in result.items()}
            l1 = sum(abs(new_ranks[node] - ranks[node]) for node in ranks)
            ranks = new_ranks
            items = list(result.items())
            if l1 < epsilon:
                return passes
        return max_iter

    def test_invariant_on_the_hand_made_graphs(self):
        for name, graph in (("chain", CHAIN), ("cycle", CYCLE), ("trace", TRACE)):
            with self.subTest(graph=name):
                self.assert_invariant_every_pass(graph)

    def test_invariant_on_the_dataset_graphs(self):
        for filename in ("web_graph_sample.txt", "web_graph_medium.txt", "web_graph_large.txt"):
            with self.subTest(graph=filename):
                graph = load_graph(os.path.join(DATA_DIR, filename))
                self.assert_invariant_every_pass(graph)


class TestGraphStructureIsPreserved(unittest.TestCase):
    """Section 3.4: without the STRUCT message the graph is destroyed after one pass."""

    def test_removing_the_struct_message_destroys_the_graph(self):
        def mapper_without_struct(item):
            _node, (rank, adjacency) = item
            if adjacency:
                for neighbour in adjacency:
                    yield (neighbour, ("RANK", rank / len(adjacency)))

        items = initial_items(TRACE)
        first = mapreduce(items, mapper_without_struct, make_reducer(0.85, 4, 0.25))

        # E has no in-links, so with no self-addressed STRUCT it vanishes from the graph.
        self.assertNotIn("E", first)

        second = mapreduce(list(first.items()), mapper_without_struct, make_reducer(0.85, 4, 0.25))

        # No adjacency survived, so the second pass has no edges left to push rank along.
        self.assertEqual(second, {})

    def test_adjacency_returned_by_the_reducer_matches_the_original_graph(self):
        result = one_pass(initial_items(TRACE), d=0.85, N=4)

        self.assertEqual({node: adjacency for node, (_rank, adjacency) in result.items()}, TRACE)


class TestLoadGraph(unittest.TestCase):
    """The loader must skip comments and turn an empty line body into a dangling node."""

    def test_reads_the_sample_graph(self):
        graph = load_graph(os.path.join(DATA_DIR, "web_graph_sample.txt"))

        self.assertEqual(len(graph), 8)
        self.assertEqual(graph["A"], ["B", "C"])
        self.assertEqual(graph["E"], [])
        self.assertEqual(sum(len(adjacency) for adjacency in graph.values()), 12)

    def test_every_referenced_node_exists_as_a_key(self):
        """A node that is linked to but never declared would reach the reducer with no STRUCT."""
        graph = load_graph(os.path.join(DATA_DIR, "web_graph_sample.txt"))

        for adjacency in graph.values():
            for neighbour in adjacency:
                self.assertIn(neighbour, graph)


if __name__ == "__main__":
    unittest.main()
