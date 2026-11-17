"""Run-parameter injection for tools/kaggle_run.py (no network)."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import kaggle_run  # noqa: E402


def test_set_params_counts_matches_even_when_value_unchanged():
    code = 'F = float(os.environ.get("BDM_FRACTION", "0.05"))\nM = os.environ.get("BDM_SPARK_MEM", "20g")\n'
    new, n = kaggle_run.set_params(code, 0.05, "20g")
    assert n == 2 and new == code
    new, n = kaggle_run.set_params(code, 1.0, "16g")
    assert n == 2 and '"BDM_FRACTION", "1.0"' in new and '"BDM_SPARK_MEM", "16g"' in new


def test_every_notebook_has_parameters_and_valid_metadata(monkeypatch):
    seen = {}

    def fake(*args):
        d = Path(args[3])
        nb = json.loads(next(d.glob("*.ipynb")).read_text())
        seen[args] = ["".join(c["source"]) for c in nb["cells"]]
        meta = json.loads((d / "kernel-metadata.json").read_text())
        assert meta["id"].startswith("minhquangdng/bdm-") and meta["enable_internet"]
        return 0

    monkeypatch.setattr(kaggle_run, "kaggle", fake)
    for nb in kaggle_run.NOTEBOOKS:
        assert kaggle_run.push(nb, 0.25, "18g") == 0
    assert len(seen) == len(kaggle_run.NOTEBOOKS)
    for cells in seen.values():
        assert any('"BDM_FRACTION", "0.25"' in c for c in cells)
