# Lab 04 – CPG Streaming Pipeline · `huggingface/lerobot`

**Course**: Nhập Môn Dữ Liệu Lớn – HCMUS  
**Team**: CAF  
**Repository chosen**: [`huggingface/lerobot`](https://github.com/huggingface/lerobot)

---

## Overview

This Jupyter Book documents our implementation of an **incremental Code Property Graph (CPG) pipeline** with real-time streaming ingestion for Lab 04.

We selected `huggingface/lerobot` — a **746-file, 8.4 MB** Python codebase for end-to-end robot learning — and built a complete pipeline that:

1. **Clones** the repository and discovers all Python source files
2. **Parses** each file incrementally to extract AST nodes, CFG/DFG/CALL edges
3. **Streams** the results through Apache Kafka (4 dedicated topics)
4. **Ingests** the graph topology into **Neo4j** via the Neo4j Kafka Connector Sink
5. *(Task 5/6 – team member's scope)*

## Pipeline Architecture

```
LeRobot repo (746 .py files)
       │  git clone --depth=1
       ▼
 Parser Service (ast module)
 ├── AST nodes  ──────────────► cpg.nodes   (459,699 msgs)
 ├── CFG edges  ──────────────► cpg.edges   (199,252 msgs)
 ├── DFG edges  ──┘
 ├── CALL edges ──┘
 ├── Metadata   ──────────────► cpg.metadata  (543 msgs)
 └── Errors     ──────────────► cpg.errors      (0 msgs)
                                      │
                    ┌─────────────────┴──────────────────┐
                    ▼                                    ▼
         Neo4j Kafka Connector               Spark Structured Streaming
         (no intermediate Spark)             (team member – Task 5/6)
                    │
                    ▼
              Neo4j Graph DB
         202,268 nodes ingested
         249,465 relationships
```

## Grading Summary

| Task | Description | Points |
|------|-------------|--------|
| Task 1 | Repository Cloning & File Discovery | 1.0 |
| Task 2 | Incremental CPG Parser Service | 1.5 |
| Task 3 | Kafka Topic Design | 1.5 |
| Task 4 | Graph Topology Ingestion into Neo4j | 2.0 |
| Task 5 | Source Metadata into MongoDB *(team member)* | 2.0 |
| Task 6 | Idempotent Replay Verification *(team member)* | 1.0 |
| Architecture Diagram | | 1.0 |
| **Total** | | **10.0** |

## How to Run

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Create Kafka topics
python src/kafka_setup.py

# 3. Register Neo4j Kafka Connectors
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8083/connectors -d @config/neo4j-sink-nodes.json
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8083/connectors -d @config/neo4j-sink-edges.json

# 4. Discover files (Task 1)
python src/file_discovery.py

# 5. Run incremental CPG parser (Task 2 + 3)
cd src && python parser_service.py
```
