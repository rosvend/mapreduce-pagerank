## PageRank using MapReduce

PageRank implemented on pure python, plus a supplementary Neo4j/GDS demo.

### How to run

`src/pagerank.py` and `tests/test_pagerank.py` use the **standard library only** —
no venv, no install, nothing to sync:

```bash
python3 -m unittest discover -s tests -v   # 16 tests
python3 src/pagerank.py                    # top-15 of data/web_graph_large.txt
python3 src/pagerank.py data/web_graph_sample.txt
```

Design: `docs/Design/DESIGN_ROYSANDOVAL.md`. Results land in `results/`.

### The Neo4j demo

Dependencies live in an optional group, so the deliverable's dependency list stays empty:

```bash
uv sync --extra demo
```

Neo4j must run with the **graph-data-science plugin enabled** — the GDS jar ships
inside the image but stays inactive without `NEO4J_PLUGINS`:

```bash
docker run \
--name local-neo4j \
-p 7474:7474 -p 7687:7687 \
-d \
-v $HOME/neo4j/data:/data \
-v $HOME/neo4j/logs:/logs \
-e NEO4J_AUTH=neo4j/password123 \
-e NEO4J_PLUGINS='["graph-data-science"]' \
-e NEO4J_dbms_security_procedures_unrestricted='gds.*' \
-e NEO4J_dbms_security_procedures_allowlist='gds.*' \
neo4j:latest
```

Then, with the browser UI at http://localhost:7474 (neo4j / password123):

```bash
uv run --extra demo python src/neo4j_gds.py     # load graphs, run GDS PageRank, compare
uv run --extra demo python src/visualization.py # convergence GIF + interactive HTML
uv run --extra demo python src/scaling.py       # time per iteration + 10x/100x projection
```
