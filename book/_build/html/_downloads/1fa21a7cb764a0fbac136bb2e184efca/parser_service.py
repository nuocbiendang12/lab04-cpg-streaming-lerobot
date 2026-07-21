"""Task 2: Incremental CPG Parser Service.

Processes Python source files one file at a time (bounded memory - only the
current file's AST and event lists are held in memory) and emits the
resulting node/edge/metadata events to Kafka. Syntax errors and unreadable
files are emitted to cpg.errors instead of crashing the service.

Usage:
    python parser_service.py                     # process all discovered, non-excluded files
    python parser_service.py --file <rel_path>   # process a single file (used for Task 6 replay)
    python parser_service.py --limit 50          # process only the first N files (smoke test)
"""
import argparse
import json
import time
from pathlib import Path

from cpg_extractor import parse_file
from kafka_producer import CPGKafkaProducer

REPO_ROOT = Path(__file__).resolve().parent.parent / "repos" / "lerobot"
DISCOVERED_PATH = Path(__file__).resolve().parent.parent / "data" / "discovered_files.json"
MANIFEST_PATH = Path(__file__).resolve().parent.parent / "data" / "processed_manifest.json"


def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def iter_target_files(single_file: str | None, limit: int | None):
    with open(DISCOVERED_PATH, encoding="utf-8") as f:
        data = json.load(f)
    files = data["files"]
    if single_file:
        files = [r for r in files if r["rel_path"] == single_file]
        if not files:
            raise SystemExit(f"File not found in discovered_files.json: {single_file}")
    else:
        files = [r for r in files if not r["excluded"]]
        if limit:
            files = files[:limit]
    return files


def process_one_file(producer: CPGKafkaProducer, rel_path: str) -> dict:
    abs_path = REPO_ROOT / rel_path
    kafka_path = f"lerobot/{rel_path}"  # stable logical path, independent of local clone location
    try:
        source = abs_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        producer.emit_error(kafka_path, str(e), error_type="io_error")
        return {"status": "io_error", "rel_path": rel_path}

    try:
        nodes, cfg_edges, dfg_edges, call_edges, metadata = parse_file(kafka_path, source)
    except SyntaxError as e:
        producer.emit_error(kafka_path, str(e), error_type="syntax_error")
        return {"status": "syntax_error", "rel_path": rel_path}

    for node in nodes:
        producer.emit_node(node)
    for edge in (cfg_edges + dfg_edges + call_edges):
        producer.emit_edge(edge)
    producer.emit_metadata(metadata)

    return {
        "status": "ok",
        "rel_path": rel_path,
        "file_hash": metadata.file_hash,
        "num_nodes": len(nodes),
        "num_edges": len(cfg_edges) + len(dfg_edges) + len(call_edges),
    }


def run(single_file: str | None = None, limit: int | None = None, bootstrap_servers: str = "localhost:29092"):
    targets = iter_target_files(single_file, limit)
    manifest = load_manifest()
    producer = CPGKafkaProducer(bootstrap_servers=bootstrap_servers)

    results = []
    t0 = time.time()
    for i, rec in enumerate(targets, 1):
        result = process_one_file(producer, rec["rel_path"])
        results.append(result)
        if result["status"] == "ok":
            manifest[result["rel_path"]] = {
                "file_hash": result["file_hash"],
                "num_nodes": result["num_nodes"],
                "num_edges": result["num_edges"],
                "last_processed": time.time(),
            }
        if i % 50 == 0 or i == len(targets):
            print(f"[{i}/{len(targets)}] processed ({time.time() - t0:.1f}s elapsed)")

    producer.close()
    save_manifest(manifest)

    ok = sum(1 for r in results if r["status"] == "ok")
    errors = sum(1 for r in results if r["status"] != "ok")
    print(f"\nDone. {ok} files parsed OK, {errors} errors, "
          f"{sum(r.get('num_nodes', 0) for r in results)} nodes, "
          f"{sum(r.get('num_edges', 0) for r in results)} edges emitted.")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default=None, help="Process a single rel_path (Task 6 replay)")
    parser.add_argument("--limit", type=int, default=None, help="Process only first N files")
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    args = parser.parse_args()
    run(single_file=args.file, limit=args.limit, bootstrap_servers=args.bootstrap_servers)
