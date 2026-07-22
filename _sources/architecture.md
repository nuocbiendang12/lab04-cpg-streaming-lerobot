# Architecture Diagram

This page presents the complete pipeline architecture for Lab 04 — CPG Streaming with `huggingface/lerobot`.

---

## Full Pipeline Diagram

```{mermaid}
flowchart TD
    A["🗂️ huggingface/lerobot\n(746 .py files)"] -->|"git clone --depth=1"| B

    subgraph Parser["🐍 Parser Service (src/parser_service.py)"]
        B["file_discovery.py\n543 included files"] --> C["cpg_extractor.py\n• extract_ast_nodes()\n• extract_cfg_edges()\n• extract_dfg_edges()\n• extract_call_edges()"]
    end

    subgraph Kafka["⚡ Apache Kafka (Confluent 7.6.1)"]
        C -->|"459,699 msgs"| D["cpg.nodes\n3 partitions"]
        C -->|"199,252 msgs"| E["cpg.edges\n3 partitions"]
        C -->|"543 msgs"| F["cpg.metadata\n1 partition"]
        C -->|"0 msgs"| G["cpg.errors\n1 partition"]
    end

    subgraph Neo4j["🔵 Neo4j Kafka Connector Sink (Task 4)"]
        D -->|"tasks.max=3"| H["neo4j-cpg-nodes-sink\nCypher: MERGE (n:CPGNode)"]
        E -->|"tasks.max=3"| I["neo4j-cpg-edges-sink\nCypher: FOREACH/CASE MERGE"]
    end

    subgraph MongoDB["🟢 Spark Structured Streaming (Task 5 - teammate)"]
        F --> J["SparkSession\n.readStream(kafka)\n.writeStream(mongodb)"]
    end

    H --> K[("🔵 Neo4j Graph DB\nbolt://localhost:7687\n202,268 nodes ingested\n249,465 relationships")]
    I --> K
    J --> L[("🟢 MongoDB\nport 27017\ncpg_db.cpg_metadata\n543 documents")]

    style Parser fill:#fffde7,stroke:#f9a825
    style Kafka fill:#e3f2fd,stroke:#1976d2
    style Neo4j fill:#e8f5e9,stroke:#388e3c
    style MongoDB fill:#fce4ec,stroke:#c62828
```

---

## Component Table

| Component | Technology | Version | Role |
|-----------|-----------|---------|------|
| Message Broker | Apache Kafka | Confluent 7.6.1 | Transport layer for all CPG events |
| Stream Coordination | Apache Zookeeper | Confluent 7.6.1 | Kafka cluster coordination |
| Graph Connector | Neo4j Kafka Connector Sink | 5.1.5 | Direct Kafka → Neo4j (no Spark) |
| Graph Database | Neo4j Community | 5.24 | Persistent CPG graph storage |
| Document Store | MongoDB | 7.0 | Source metadata persistence |
| Stream Processor | Apache Spark | 3.5.1 | Kafka → MongoDB (Task 5, teammate) |
| CPG Parser | Python `ast` stdlib | 3.12 | AST/CFG/DFG/CALL extraction |
| Orchestration | Docker Compose | 3.8 | Single-command infra setup |

---

## Data Flow Numbers

| Stage | Volume |
|-------|--------|
| Python files discovered | 746 total, **543 included** |
| AST nodes extracted | **459,699** |
| Edges extracted (CFG+DFG+CALL) | **199,252** |
| Metadata events | **543** (one per file) |
| Neo4j nodes ingested (ongoing) | **202,268** (44% complete) |
| Neo4j relationships ingested | **249,465** |
| MongoDB documents (Task 5) | 543 |

---

## Key Design Decisions

1. **`ast` module over Joern/tree-sitter**: Zero external dependencies, sufficient for Python-only CPG, easy per-file streaming.
2. **SHA-256 stable IDs**: `node_id = sha256(file_path:lineno:col:type)[:16]` — pure function of source position ensures idempotent `MERGE`.
3. **4 Kafka topics**: Separate topics per event category allow independent consumer scaling (Neo4j connector vs. Spark).
4. **`tasks.max = 3 = partitions`**: One connector task per partition eliminates write contention.
5. **`FOREACH/CASE` instead of APOC**: APOC calls were silently ignored by the Kafka Connector despite working via Python driver — `FOREACH/CASE` on static edge types is the correct workaround.
