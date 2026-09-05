"""Loads the assignment graphs into Neo4j and runs its built-in GDS PageRank.

Supplementary demo: cross-checks src/pagerank.py against a production PageRank.
NOT part of the graded MapReduce deliverable.

Run: uv run --extra demo python src/neo4j_gds.py

Note: this file cannot be called neo4j.py -- that name shadows the driver package.
"""

import logging
import os

from neo4j import GraphDatabase

from pagerank import load_graph, run_pagerank

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
USER = os.environ.get("NEO4J_USER", "neo4j")
PASSWORD = os.environ.get("NEO4J_PASSWORD", "password123")

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
DATASETS = (
    ("Sample", "web_graph_sample.txt"),
    ("Medium", "web_graph_medium.txt"),
    ("Large", "web_graph_large.txt"),
)
BATCH = 5000


def batches(sequence, size=BATCH):
    """Splits a list into chunks so no single transaction gets too large."""
    for start in range(0, len(sequence), size):
        yield sequence[start:start + size]


def load_into_neo4j(session, label, graph):
    """Replaces the :<label> subgraph in Neo4j with the adjacency read from disk."""
    session.run("MATCH (n:%s) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 5000 ROWS" % label)
    session.run("CREATE INDEX %s_id IF NOT EXISTS FOR (n:%s) ON (n.id)" % (label.lower(), label))

    nodes = sorted(graph)
    for batch in batches(nodes):
        session.run("UNWIND $ids AS id CREATE (n:Page:%s {id: id})" % label, ids=batch)

    edges = [{"source": u, "target": v} for u, adjacency in graph.items() for v in adjacency]
    for batch in batches(edges):
        session.run(
            "UNWIND $edges AS e "
            "MATCH (a:%s {id: e.source}), (b:%s {id: e.target}) "
            "CREATE (a)-[:LINKS_TO]->(b)" % (label, label),
            edges=batch,
        )
    return len(nodes), len(edges)


def gds_pagerank(session, label, d=0.85, max_iter=50, tolerance=1e-6):
    """Runs Neo4j's built-in PageRank; returns (normalised scores, iterations, converged)."""
    name = "pr_" + label.lower()
    session.run("CALL gds.graph.drop($name, false)", name=name)
    session.run("CALL gds.graph.project($name, $label, 'LINKS_TO')", name=name, label=label)

    config = {"dampingFactor": d, "maxIterations": max_iter, "tolerance": tolerance}
    stats = session.run(
        "CALL gds.pageRank.stats($name, $config) YIELD ranIterations, didConverge",
        name=name, config=config,
    ).single()

    rows = session.run(
        "CALL gds.pageRank.stream($name, $config) YIELD nodeId, score "
        "RETURN gds.util.asNode(nodeId).id AS id, score",
        name=name, config=config,
    ).data()
    session.run("CALL gds.graph.drop($name, false)", name=name)

    # GDS does not normalise its scores, so divide by the total to compare with ours.
    total = sum(row["score"] for row in rows)
    scores = {row["id"]: row["score"] / total for row in rows}
    return scores, stats["ranIterations"], stats["didConverge"]


def spearman(first, second):
    """Rank correlation between two score dictionaries, without numpy or scipy."""
    nodes = list(first)
    order_first = {node: i for i, node in enumerate(sorted(nodes, key=lambda n: -first[n]))}
    order_second = {node: i for i, node in enumerate(sorted(nodes, key=lambda n: -second[n]))}
    n = len(nodes)
    mean = (n - 1) / 2.0
    numerator = sum((order_first[x] - mean) * (order_second[x] - mean) for x in nodes)
    spread = sum((order_first[x] - mean) ** 2 for x in nodes)
    return numerator / spread if spread else 1.0


if __name__ == "__main__":
    # The server nags about a deprecated field in gds.graph.drop; not our concern here.
    logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    driver.verify_connectivity()
    print("connected to %s\n" % URI)

    with driver.session() as session:
        version = session.run("RETURN gds.version() AS v").single()["v"]
        print("GDS version: %s\n" % version)

        for label, filename in DATASETS:
            graph = load_graph(os.path.join(DATA, filename))
            nodes, edges = load_into_neo4j(session, label, graph)
            gds_scores, gds_iters, gds_converged = gds_pagerank(session, label)
            mine, my_iters, _converged = run_pagerank(graph)

            count = min(15, nodes)
            top_gds = [n for n, _ in sorted(gds_scores.items(), key=lambda p: -p[1])][:count]
            top_mine = [n for n, _ in sorted(mine.items(), key=lambda p: -p[1])][:count]
            overlap = len(set(top_gds) & set(top_mine))

            print("%s (%d nodes, %d edges)" % (filename, nodes, edges))
            print("  GDS PageRank:       %2d iterations (converged: %s)" % (gds_iters, gds_converged))
            print("  MapReduce PageRank: %2d iterations" % my_iters)
            print("  top-%d overlap:      %d/%d" % (count, overlap, count))
            print("  same #1:            %s (%s vs %s)" % (top_gds[0] == top_mine[0], top_mine[0], top_gds[0]))
            print("  Spearman rank corr: %.6f" % spearman(mine, gds_scores))
            print("  max |diff| in score: %.3e\n" % max(abs(mine[n] - gds_scores[n]) for n in graph))

    driver.close()
