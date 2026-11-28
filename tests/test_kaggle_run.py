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

    def fake(*args, **kw):
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


def test_kaggle_retries_on_rate_limit(monkeypatch):
    calls = []

    class R:
        def __init__(self, out, rc):
            self.stdout, self.stderr, self.returncode = out, "", rc

    outs = [R("429 Client Error: Too Many Requests", 1),
            R("Maximum batch CPU session count of 5 reached.", 0), R("ok", 0)]
    monkeypatch.setattr(kaggle_run.subprocess, "run", lambda *a, **k: (calls.append(a), outs.pop(0))[1])
    monkeypatch.setattr(kaggle_run.time, "sleep", lambda s: None)
    assert kaggle_run.kaggle("kernels", "push", "-p", "x", retries=5) == 0
    assert len(calls) == 3


def test_output_ignores_the_cloned_repo(monkeypatch, tmp_path):
    root = tmp_path / "proj"
    monkeypatch.setattr(kaggle_run, "ROOT", root)

    def fake(*args, **kw):
        dest = Path(args[args.index("-p") + 1])
        for sub, tag in (("results", "new"), ("repo/results", "stale")):
            (dest / sub / "figures").mkdir(parents=True, exist_ok=True)
            (dest / sub / "04_lsh_similarity.json").write_text(tag)
            (dest / sub / "figures" / "04_s_curve.png").write_text(tag)
        return 0

    monkeypatch.setattr(kaggle_run, "kaggle", fake)
    kaggle_run.output("04")
    assert (root / "results" / "04_lsh_similarity.json").read_text() == "new"
    assert (root / "results" / "figures" / "04_s_curve.png").read_text() == "new"


def test_seed_kernels_bake_their_seed(monkeypatch):
    texts = {}

    def fake(*args, **kw):
        d = Path(args[3])
        meta = json.loads((d / "kernel-metadata.json").read_text())
        texts[meta["id"]] = json.loads(next(d.glob("*.ipynb")).read_text())
        return 0

    monkeypatch.setattr(kaggle_run, "kaggle", fake)
    for nb, seed in (("11s42", "42"), ("11s7", "7"), ("11s2024", "2024")):
        assert kaggle_run.push(nb, 1.0, "20g") == 0
        cells = "".join("".join(c["source"]) for c in texts[f"minhquangdng/{kaggle_run.slug(nb)}"]["cells"])
        assert f'"BDM_SEED", "{seed}"' in cells
    assert len({kaggle_run.slug(nb) for nb in ("11s42", "11s7", "11s2024")}) == 3
