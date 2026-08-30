## PageRank using MapReduce (Neo4j)

## How to get started 🚀

```bash 
#install dependencies 
uv sync
```
```bash
#run neo4j
docker run \
--name local-neo4j \
-p 7474:7474 -p 7687:7687 \
-d \
-v $HOME/neo4j/data:/data \
-v $HOME/neo4j/logs:/logs \
-e NEO4J_AUTH=neo4j/password123 \
neo4j:latest
```