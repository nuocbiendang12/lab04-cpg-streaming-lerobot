"""Kafka producer wrapper for the CPG Parser Service (Task 3 wiring).

Emits four categories of events, one topic each, matching the Kafka Topic
Design in the lab spec: node events, edge events, source metadata events,
and parser error events. Every message carries schema_version + event_time
for forward compatibility.
"""
import json
from dataclasses import asdict
from datetime import datetime, timezone

from kafka import KafkaProducer

SCHEMA_VERSION = "1.0"

TOPICS = {
    "nodes": "cpg.nodes",
    "edges": "cpg.edges",
    "metadata": "cpg.metadata",
    "errors": "cpg.errors",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _envelope(event_type: str, key: str, payload: dict) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "event_time": _now_iso(),
        "event_type": event_type,
        **payload,
        "_key": key,
    }


class CPGKafkaProducer:
    def __init__(self, bootstrap_servers: str = "localhost:29092"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            linger_ms=20,
        )

    def emit_node(self, node) -> None:
        payload = asdict(node)
        msg = _envelope("node", node.node_id, payload)
        self.producer.send(TOPICS["nodes"], key=node.node_id, value=msg)

    def emit_edge(self, edge) -> None:
        payload = asdict(edge)
        msg = _envelope("edge", edge.edge_id, payload)
        self.producer.send(TOPICS["edges"], key=edge.edge_id, value=msg)

    def emit_metadata(self, meta) -> None:
        payload = asdict(meta)
        msg = _envelope("metadata", meta.file_path, payload)
        self.producer.send(TOPICS["metadata"], key=meta.file_path, value=msg)

    def emit_error(self, file_path: str, error_message: str, error_type: str) -> None:
        msg = _envelope("error", file_path, {
            "file_path": file_path,
            "error_type": error_type,
            "error_message": error_message,
        })
        self.producer.send(TOPICS["errors"], key=file_path, value=msg)

    def flush(self) -> None:
        self.producer.flush()

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()
