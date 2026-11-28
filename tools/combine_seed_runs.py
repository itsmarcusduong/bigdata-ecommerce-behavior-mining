"""Merge the per-seed test runs of notebook 11 into results/11_recsys_test.json (ADR-0007).

A pair "A vs B" is *better* only if its paired-bootstrap 95% CI excludes 0 on the positive side for **every** seed,
*worse* if on the negative side for every seed, otherwise a *tie* (ADR-0002). A missing seed gives no verdict.
The deterministic models (popularity, history) and the evaluated user set must agree across seeds; otherwise the runs
are not comparable and nothing is written.
    python tools/combine_seed_runs.py            # seeds 42 7 2024
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / "results"
DETERMINISTIC = ("popularity", "history")


def overall(verdicts: list[str]) -> str:
    if verdicts and all(v == "better" for v in verdicts):
        return "better"
    if verdicts and all(v == "worse" for v in verdicts):
        return "worse"
    return "tie"


def combine(runs: dict[int, dict], seeds: list[int]) -> dict:
    """Combine per-seed results; raises ValueError when the runs are missing or not comparable."""
    missing = [s for s in seeds if s not in runs]
    if missing:
        raise ValueError(f"missing seeds {missing}: no verdict (ADR-0007, step 3)")
    first = runs[seeds[0]]
    for s in seeds[1:]:
        r = runs[s]
        if r["n_common_users"] != first["n_common_users"]:
            raise ValueError(f"seed {s}: n_common_users {r['n_common_users']} != {first['n_common_users']}")
        for m in DETERMINISTIC:
            a = next(x["ndcg@10"] for x in first["test"] if x["model"] == m)
            b = next(x["ndcg@10"] for x in r["test"] if x["model"] == m)
            if abs(a - b) > 1e-9:
                raise ValueError(f"seed {s}: deterministic model {m} differs ({a} vs {b})")
    pairs: dict[str, list[dict]] = {}
    for s in seeds:
        for row in runs[s]["significance"]:
            pairs.setdefault(f"{row['a']} vs {row['b']}", []).append(row)
    verdicts = {p: overall([r["verdict"] for r in rows]) for p, rows in pairs.items()}
    models = sorted({x["model"] for x in first["test"]})
    summary = {}
    for m in models:
        vals = [next(x["ndcg@10"] for x in runs[s]["test"] if x["model"] == m) for s in seeds]
        mean = sum(vals) / len(vals)
        sd = (sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)) ** 0.5 if len(vals) > 1 else 0.0
        summary[m] = {"ndcg@10_mean": mean, "ndcg@10_sd": sd, "ndcg@10_by_seed": dict(zip(map(str, seeds), vals))}
    return {"experiment": "11_recsys_test", "seeds": seeds, "config": first["config"], "cutoff": first["cutoff"],
            "fraction": first["fraction"], "n_common_users": first["n_common_users"],
            "created_utc": max(runs[s]["created_utc"] for s in seeds),
            "source_files": [f"results/11_recsys_test_seed{s}.json" for s in seeds], "test_summary": summary,
            "significance": [r for rows in pairs.values() for r in rows], "verdicts": verdicts,
            "rule": "better only if the paired-bootstrap 95% CI excludes 0 (positive) for every seed (ADR-0002/0007)"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seeds", type=int, nargs="+", default=[42, 7, 2024])
    a = ap.parse_args()
    runs = {}
    for s in a.seeds:
        p = RES / f"11_recsys_test_seed{s}.json"
        if p.exists():
            runs[s] = json.loads(p.read_text())
    try:
        body = combine(runs, a.seeds)
    except ValueError as e:
        print(f"not combined: {e}", file=sys.stderr)
        return 1
    (RES / "11_recsys_test.json").write_text(json.dumps(body, indent=2) + "\n")
    print("wrote results/11_recsys_test.json:", body["verdicts"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
