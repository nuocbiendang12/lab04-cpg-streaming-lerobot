"""Task 1: Repository Cloning and File Discovery.

Enumerates all .py files under the cloned lerobot repository, optionally
excluding test/setup/auto-generated files, and writes the resulting file
list + summary stats to data/discovered_files.json for downstream tasks
(the Parser Service in Task 2 consumes this list one file at a time).
"""
import json
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent / "repos" / "lerobot"
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "discovered_files.json"

EXCLUDE_NAME_PATTERNS = ("setup.py", "conf.py")
EXCLUDE_DIR_PARTS = ("tests", "test")


def is_excluded(rel_path: Path) -> bool:
    name = rel_path.name
    if name in EXCLUDE_NAME_PATTERNS or name.startswith("test_") or name.endswith("_test.py"):
        return True
    if any(part in EXCLUDE_DIR_PARTS for part in rel_path.parts[:-1]):
        return True
    return False


def discover_files():
    all_py = sorted(REPO_ROOT.rglob("*.py"))
    all_py = [p for p in all_py if ".git" not in p.parts]

    records = []
    for path in all_py:
        rel_path = path.relative_to(REPO_ROOT)
        stat = path.stat()
        records.append({
            "rel_path": str(rel_path).replace("\\", "/"),
            "size_bytes": stat.st_size,
            "excluded": is_excluded(rel_path),
        })
    return records


def summarize(records):
    included = [r for r in records if not r["excluded"]]
    excluded = [r for r in records if r["excluded"]]

    by_top_dir = {}
    for r in records:
        top = r["rel_path"].split("/")[0]
        by_top_dir[top] = by_top_dir.get(top, 0) + 1

    return {
        "total_py_files": len(records),
        "included_files": len(included),
        "excluded_files": len(excluded),
        "total_size_bytes": sum(r["size_bytes"] for r in records),
        "by_top_level_dir": dict(sorted(by_top_dir.items(), key=lambda kv: -kv[1])),
        "largest_files": sorted(records, key=lambda r: -r["size_bytes"])[:5],
    }


def main():
    records = discover_files()
    summary = summarize(records)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump({"summary": summary, "files": records}, f, indent=2)

    print(f"Repository: {REPO_ROOT}")
    print(f"Total .py files discovered: {summary['total_py_files']}")
    print(f"  Included (non-test/setup):  {summary['included_files']}")
    print(f"  Excluded (test/setup/conf): {summary['excluded_files']}")
    print(f"Total size: {summary['total_size_bytes'] / 1024:.1f} KB")
    print("By top-level directory:")
    for k, v in summary["by_top_level_dir"].items():
        print(f"  {k:20s} {v}")
    print(f"\nWrote file list to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
