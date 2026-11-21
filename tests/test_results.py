"""Result files: stage log helper, and every JSON key the results page reads exists in the committed real results.

The second test guards against a notebook/report reading a key the real run never wrote (a lesson from an earlier
project: a renamed key broke a Kaggle run half-way). It runs on the committed results/*.json, not on synthetic data.
"""

import json
import sys
from pathlib import Path

import pytest

from bdm import config, results

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import make_results_md as mr  # noqa: E402

# Results produced by an older notebook version and awaiting a rerun. Each entry must name why; remove it when the
# new run is merged (the test then enforces the new keys).
PENDING = {
    "06_recommender": "rerun of the pre-registered multi-model protocol (#67) still on Kaggle",
}


def test_mark_stage_appends(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "RESULTS_DIR", tmp_path)
    results.mark_stage("nb", "split", n=3)
    results.mark_stage("nb", "fit", status="FAILED", error="boom")
    rows = json.loads((tmp_path / "nb_stages.json").read_text())
    assert [r["stage"] for r in rows] == ["split", "fit"]
    assert rows[0]["status"] == "OK" and rows[0]["n"] == 3
    assert rows[1]["status"] == "FAILED" and rows[1]["error"] == "boom"


def _real(name):
    d = mr.load(name)
    if d is None:
        pytest.skip(f"results/{name}.json not committed yet")
    if name in PENDING:
        pytest.skip(f"{name}: {PENDING[name]}")
    return d


@pytest.mark.parametrize("section,name,rows", mr.ROWS, ids=[r[1] for r in mr.ROWS])
def test_results_page_keys_exist(section, name, rows):
    d = _real(name)
    missing = []
    for label, path in rows:
        if path is None:
            val, _ = mr.composite(name, label, d)
            if mr.MISSING in val:
                missing.append(label)
        elif mr.get(d, path) is None:
            missing.append(path)
    assert not missing, f"{name}: keys read by make_results_md but absent in the real run: {missing}"


@pytest.mark.parametrize("name,path,title,cols", mr.TABLES, ids=[f"{t[0]}:{t[1]}" for t in mr.TABLES])
def test_results_page_tables_exist(name, path, title, cols):
    d = _real(name)
    recs = mr.get(d, path)
    assert isinstance(recs, list) and recs, f"{name}.{path} is not a non-empty list"
    absent = [c for c in cols if c not in recs[0]]
    assert not absent, f"{name}.{path}: columns {absent} missing"


def test_manifest_hash_ignores_line_endings(tmp_path):
    import build_manifest as bm

    (tmp_path / "a.json").write_bytes(b'{"fraction": 1.0,\n "created_utc": "x"}\n')
    (tmp_path / "b.json").write_bytes(b'{"fraction": 1.0,\r\n "created_utc": "x"}\r\n')
    assert bm.sha256(tmp_path / "a.json") == bm.sha256(tmp_path / "b.json")
    (tmp_path / "c.png").write_bytes(b"\x89PNG\r\n")
    assert bm.sha256(tmp_path / "c.png") != bm.sha256(tmp_path / "a.json")
