"""combine_seed_runs: the all-seeds rule of ADR-0002/0007 and the comparability checks."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import combine_seed_runs as c  # noqa: E402


def run(seed, als, verdict_ha, n=100, hist=0.10):
    test = [{"model": "popularity", "ndcg@10": 0.06}, {"model": "history", "ndcg@10": hist},
            {"model": "als", "ndcg@10": als}, {"model": "history_als", "ndcg@10": 0.11}]
    sig = [{"a": "history_als", "b": "als", "seed": seed, "verdict": verdict_ha},
           {"a": "history", "b": "als", "seed": seed, "verdict": "better"}]
    return {"seed": seed, "n_common_users": n, "test": test, "significance": sig, "config": {}, "cutoff": "x",
            "fraction": 1.0, "created_utc": f"2026-11-28T0{seed % 9}"}


def test_better_only_if_every_seed():
    runs = {42: run(42, .09, "better"), 7: run(7, .091, "better"), 2024: run(2024, .089, "tie")}
    out = c.combine(runs, [42, 7, 2024])
    assert out["verdicts"]["history_als vs als"] == "tie"
    assert out["verdicts"]["history vs als"] == "better"
    assert out["test_summary"]["als"]["ndcg@10_sd"] > 0


def test_missing_seed_gives_no_verdict():
    with pytest.raises(ValueError, match="missing"):
        c.combine({42: run(42, .09, "better")}, [42, 7])


def test_runs_must_be_comparable():
    with pytest.raises(ValueError, match="n_common_users"):
        c.combine({42: run(42, .09, "better"), 7: run(7, .09, "better", n=99)}, [42, 7])
    with pytest.raises(ValueError, match="deterministic"):
        c.combine({42: run(42, .09, "better"), 7: run(7, .09, "better", hist=0.2)}, [42, 7])
