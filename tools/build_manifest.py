"""Write results/MANIFEST.json: SHA-256, size, run time and FRACTION of every committed result file.

Run at release time so the tag fixes which exact result files the report and slides were written from:
    python tools/build_manifest.py
Line endings are normalised to LF before hashing text files, so a checkout on Windows gives the same hashes.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / "results"
TEXT = {".json", ".log", ".md", ".txt", ".csv"}


def sha256(path: Path) -> str:
    data = path.read_bytes()
    if path.suffix in TEXT:
        data = data.replace(b"\r\n", b"\n")
    return hashlib.sha256(data).hexdigest()


def entry(path: Path) -> dict:
    e = {"path": path.relative_to(ROOT).as_posix(), "bytes": path.stat().st_size, "sha256": sha256(path)}
    if path.suffix == ".json":
        try:
            body = json.loads(path.read_text())
        except ValueError:
            return e
        if isinstance(body, dict):
            for k in ("created_utc", "fraction", "base_fraction"):
                if k in body:
                    e[k] = body[k]
    return e


def build(root: Path = RES) -> dict:
    files = sorted(p for p in root.rglob("*") if p.is_file() and p.name != "MANIFEST.json" and "_kaggle" not in p.parts)
    try:
        commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    except OSError:
        commit = ""
    return {"generated_utc": datetime.now(timezone.utc).isoformat(), "source_commit": commit or None,
            "files": [entry(p) for p in files]}


def main() -> int:
    body = build()
    (RES / "MANIFEST.json").write_text(json.dumps(body, indent=2) + "\n")
    print(f"wrote results/MANIFEST.json ({len(body['files'])} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
