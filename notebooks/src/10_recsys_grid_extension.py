# %% [markdown]
# # 10 - Recommender grid extension (ADR-0006)
# Run 06 (ADR-0002) selected **history + ALS**, but two winning settings sat on the **edge of their grid**:
# history half-life 7 days (largest tried) and ALS rank 64 (largest) / alpha 10 (smallest). This notebook asks whether
# a fairer search for ALS and history changes the conclusion. Everything else is identical to notebook 06: data,
# dates (fit < validation cutoff <= validation < cutoff <= test), metric, fallback and selection rule.
#
# 1. **Reproduction gate**: the old winners must reproduce their validation NDCG@10 from `results/06_recommender.json`
#    before any new setting is compared (popularity, history: |diff| <= 1e-4; ALS: |diff| <= 2e-3).
# 2. **Grid**: history half-life in {7, 14, 30} days; ALS rank {64, 128} x alpha {3, 10} (seed 42 on validation).
# 3. **Selection**: highest validation NDCG@10 over popularity, history, ALS, history_als (this run) plus item-kNN and
#    recent popularity carried over from run 06 (same validation week, not on a grid edge).
# 4. **Test, once**: popularity, history, ALS x 3 seeds, history_als x 3 seeds; "A better than B" only if the
#    paired-bootstrap 95% CI excludes 0 for every seed.

# %%
import gc
import json
import time

from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import functions as F

from bdm import config, io, recsys
from bdm.results import mark_stage, save_fig, save_result
from bdm.sessions import time_split, user_item_matrix

NB = "10_recsys_grid_extension"
SEEDS = [int(s) for s in os.environ.get("BDM_SEEDS", "42,7,2024").split(",")]
HALF_LIVES = [float(x) for x in os.environ.get("BDM_HALF_LIVES", "7,14,30").split(",")]
ALS_GRID = [tuple(float(v) for v in p.split("x")) for p in os.environ.get("BDM_ALS_GRID", "64x10,64x3,128x10,128x3").split(",")]
GATE_TOL = {"popularity": 1e-4, "history": 1e-4, "als": 2e-3}
CARRIED = ("item_knn", "recent_popularity")   # not rerun (ADR-0006, step 4)

ref_path = next((p for p in (Path("/kaggle/working/repo/results/06_recommender.json"),
                             Path.cwd().parent / "results" / "06_recommender.json",
                             Path.cwd() / "results" / "06_recommender.json") if p.exists()), None)
ref = json.loads(ref_path.read_text()) if ref_path else None
print("reference run 06:", ref_path, ref and ref.get("created_utc"))

spark = config.get_spark("10-recsys-grid", driver_memory=SPARK_MEM)
events = io.load_events(spark, FRACTION)
CUTOFF = os.environ.get("BDM_CUTOFF", "2019-11-15")
VAL_CUTOFF = str(date.fromisoformat(CUTOFF) - timedelta(days=7))
train, test = time_split(events, CUTOFF)
train.cache(); test.cache()
truth, seen = recsys.truth_and_seen(train, test)
truth.cache(); seen.cache()
users = truth.select("user_id")
n_eval = truth.count()
fit, val = time_split(train, VAL_CUTOFF)
truth_v, seen_v = recsys.truth_and_seen(fit, val)
truth_v.cache(); seen_v.cache()
users_v = truth_v.select("user_id")
n_val = truth_v.count()
print(f"evaluable test users {n_eval:,}; validation users {n_val:,}")
mark_stage(NB, "split", n_eval_users=n_eval, n_val_users=n_val)
out = {"fraction": FRACTION, "cutoff": CUTOFF, "val_cutoff": VAL_CUTOFF, "n_eval_users": n_eval, "n_val_users": n_val,
       "seeds": SEEDS, "half_lives": HALF_LIVES, "als_grid": [{"rank": int(r), "alpha": a} for r, a in ALS_GRID],
       "adr": "docs/decisions/ADR-0006-recsys-grid-extension.md",
       "reference_run": {"path": str(ref_path) if ref_path else None, "created_utc": ref and ref.get("created_utc")}}

# %% [markdown]
# ## 1. Validation: reproduction gate first, then the extended grid

# %%
pop_v = recsys.popularity_recs(fit.filter(F.col("event_type") == "purchase"), users_v, 100).cache()
val_rows = []


def score_val(family, params, recs, t_s):
    m = recsys.evaluate(recsys.with_fallback(recs, pop_v), truth_v, seen_v)
    row = {"family": family, "params": params, "fit_s": round(t_s, 1), **{f"val_{k}": round(v, 5) for k, v in m.items()}}
    val_rows.append(row); print(row)
    return m["ndcg@10"]


def ref_val(family, params):
    """Validation NDCG@10 of the same setting in run 06, or None."""
    for r in (ref or {}).get("validation_grid", []):
        if r["family"] == family and all(float(r["params"].get(k, -1)) == float(v) for k, v in params.items()):
            return r["val_ndcg@10"]
    return None


t0 = time.perf_counter(); score_val("popularity", {}, pop_v, time.perf_counter() - t0)
for hl in HALF_LIVES:
    t0 = time.perf_counter(); r = recsys.history_recs(fit, users_v, VAL_CUTOFF, half_life_days=hl).cache()
    score_val("history", {"half_life_days": hl}, r, time.perf_counter() - t0); r.unpersist()

ui_fit = user_item_matrix(fit, min_user_items=3, min_item_users=5).cache()
print(f"fit interaction matrix: {ui_fit.count():,} non-zeros")
best_als_val, best_als_model = -1.0, None
for rank, alpha in ALS_GRID:
    t0 = time.perf_counter()
    model = recsys.train_als(ui_fit, rank=int(rank), reg=0.05, alpha=alpha, max_iter=8, seed=SEEDS[0])
    t_fit = time.perf_counter() - t0
    s = score_val("als", {"rank": int(rank), "alpha": alpha}, recsys.als_recs(model, users_v, 100), t_fit)
    if s > best_als_val:
        if best_als_model is not None:
            del best_als_model; gc.collect()
        best_als_val, best_als_model = s, model
    else:
        del model; gc.collect()
    if len([r for r in val_rows if r["family"] == "als"]) == 1:
        mark_stage(NB, "first_als", seconds=round(t_fit))

# Reproduction gate: compare the old winners with run 06 (only meaningful on the same full-data setting).
gate_rows = []
for fam, params in (("popularity", {}), ("history", {"half_life_days": 7.0}), ("als", {"rank": 64, "alpha": 10.0})):
    now = next((r["val_ndcg@10"] for r in val_rows if r["family"] == fam
                and all(float(r["params"].get(k, -1)) == float(v) for k, v in params.items())), None)
    old = ref_val(fam, params)
    diff = None if (now is None or old is None) else round(now - old, 6)
    gate_rows.append({"family": fam, "params": params, "now": now, "run06": old, "diff": diff, "tol": GATE_TOL[fam],
                      "ok": diff is not None and abs(diff) <= GATE_TOL[fam]})
same_setting = bool(ref) and ref.get("fraction") == FRACTION and ref.get("cutoff") == CUTOFF
gate = {"applicable": same_setting, "passed": (not same_setting) or all(g["ok"] for g in gate_rows), "rows": gate_rows}
print("reproduction gate:", gate)
mark_stage(NB, "gate", status="OK" if gate["passed"] else "FAILED", gate=gate)
if not gate["passed"]:
    out.update({"gate": gate, "validation_grid": val_rows,
                "conclusion": "reproduction gate failed: no conclusion about the extended grid (ADR-0006, step 1)"})
    print("saved", save_result(NB, out))
    raise SystemExit("reproduction gate failed")

val_df = pd.DataFrame(val_rows)
best_cfg = {fam: g.sort_values("val_ndcg@10", ascending=False).iloc[0]["params"] for fam, g in val_df.groupby("family")}
hl_best = best_cfg["history"]["half_life_days"]
t0 = time.perf_counter()
blend_v = recsys.blend_recs(recsys.history_recs(fit, users_v, VAL_CUTOFF, half_life_days=hl_best),
                            recsys.als_recs(best_als_model, users_v, 100))
score_val("history_als", {"half_life_days": hl_best, **best_cfg["als"]}, blend_v, time.perf_counter() - t0)
del best_als_model; gc.collect()
val_df = pd.DataFrame(val_rows)
best_cfg["history_als"] = {"half_life_days": hl_best, **best_cfg["als"]}
family_val = {fam: {"ndcg@10": float(g["val_ndcg@10"].max()), "source": "this run"} for fam, g in val_df.groupby("family")}
for fam in CARRIED:   # only comparable when this run uses the same data and dates as run 06
    v = (ref or {}).get("validation_family_ndcg@10", {}).get(fam, {}).get("ndcg@10") if same_setting else None
    if v is not None:
        family_val[fam] = {"ndcg@10": float(v), "source": "run 06 (not rerun, ADR-0006)"}
selected = recsys.select_best(family_val)
at_edge = {"history_half_life": hl_best == max(HALF_LIVES),
           "als_rank": best_cfg["als"]["rank"] == max(int(r) for r, _ in ALS_GRID),
           "als_alpha_low": best_cfg["als"]["alpha"] == min(a for _, a in ALS_GRID)}
print("best configuration per family:", best_cfg)
print("validation NDCG@10 per family:", {k: round(v["ndcg@10"], 5) for k, v in family_val.items()})
print("SELECTED (pre-registered rule):", selected, "| still on a grid edge:", at_edge)
mark_stage(NB, "validation", selected=selected, validation_family_ndcg10=family_val, best_config=best_cfg, at_edge=at_edge)
val_df.sort_values("val_ndcg@10", ascending=False)

# %% [markdown]
# ## 2. Test, once: the best settings refitted on all training data, 3 seeds for ALS-based models

# %%
t0 = time.perf_counter()
pop = recsys.popularity_recs(train.filter(F.col("event_type") == "purchase"), users, 100).cache()
hist_r = recsys.history_recs(train, users, CUTOFF, half_life_days=hl_best).cache()
ui = user_item_matrix(train, min_user_items=3, min_item_users=5).cache()
als_by_seed, blend_by_seed, als_seconds = {}, {}, {}
for sd in SEEDS:
    t1 = time.perf_counter()
    mdl = recsys.train_als(ui, rank=int(best_cfg["als"]["rank"]), reg=0.05, alpha=best_cfg["als"]["alpha"],
                           max_iter=8, seed=sd)
    als_by_seed[sd] = recsys.als_recs(mdl, users, 100).cache(); als_by_seed[sd].count()
    als_seconds[sd] = round(time.perf_counter() - t1, 1)
    blend_by_seed[sd] = recsys.blend_recs(hist_r, als_by_seed[sd]).cache(); blend_by_seed[sd].count()
    del mdl; gc.collect()
    mark_stage(NB, f"refit_seed_{sd}", seconds=als_seconds[sd])
common = (als_by_seed[SEEDS[0]].select("user_id").join(hist_r.select("user_id"), "user_id")
          .join(users, "user_id").cache())
n_common = common.count()
print(f"users history and ALS can both serve: {n_common:,} of {n_eval:,}; refits in {time.perf_counter() - t0:.0f}s")

test_rows, per_user, coverage = [], {}, {}
for name, r in (("popularity", pop), ("history", hist_r)):
    m = recsys.evaluate(r, truth, seen, users=common, per_user="ndcg@10")
    per_user[(name, None)] = m.pop("_per_user"); test_rows.append({"model": name, "seed": None, **m})
    coverage[name] = recsys.evaluate(recsys.with_fallback(r, pop), truth, seen)
for sd in SEEDS:
    for name, r in (("als", als_by_seed[sd]), ("history_als", blend_by_seed[sd])):
        m = recsys.evaluate(r, truth, seen, users=common, per_user="ndcg@10")
        per_user[(name, sd)] = m.pop("_per_user"); test_rows.append({"model": name, "seed": sd, **m})
        if sd == SEEDS[0]:
            coverage[name] = recsys.evaluate(recsys.with_fallback(r, pop), truth, seen)
test_df = pd.DataFrame(test_rows)
summary = (test_df.groupby("model")[["precision@10", "recall@10", "hit_rate@10", "map@10", "ndcg@10", "ndcg@20"]]
           .agg(["mean", "std"]))
summary

# %% [markdown]
# ## 3. Paired bootstrap: the selected family against every family rerun here, for every seed

# %%
rerun = ["popularity", "history", "als", "history_als"]


def pu(name, sd):
    return per_user[(name, sd if name in ("als", "history_als") else None)]


sig = []
if selected in rerun:
    for other in [f for f in rerun if f != selected]:
        for sd in SEEDS:
            b = recsys.paired_bootstrap(pu(selected, sd), pu(other, sd), seed=sd)
            sig.append({"selected": selected, "other": other, "seed": sd, **b, "verdict": recsys.verdict(b)})
# The key question of ADR-0006, asked whatever is selected: does the blend still beat tuned ALS and history?
key = []
for other in ("als", "history"):
    for sd in SEEDS:
        b = recsys.paired_bootstrap(pu("history_als", sd), pu(other, sd), seed=sd)
        key.append({"a": "history_als", "b": other, "seed": sd, **b, "verdict": recsys.verdict(b)})
for sd in SEEDS:
    b = recsys.paired_bootstrap(pu("history", None), pu("als", sd), seed=sd)
    key.append({"a": "history", "b": "als", "seed": sd, **b, "verdict": recsys.verdict(b)})
sig_df, key_df = pd.DataFrame(sig), pd.DataFrame(key)


def overall(df, col):
    return {o: ("better" if (g["verdict"] == "better").all() else "worse" if (g["verdict"] == "worse").all() else "tie")
            for o, g in df.groupby(col)} if len(df) else {}


key_df["pair"] = key_df["a"] + " vs " + key_df["b"]
verdicts = {"selected_vs_others": overall(sig_df, "other"), "key_pairs": overall(key_df, "pair")}
print("verdicts over", len(SEEDS), "seeds:", verdicts)
mark_stage(NB, "test", verdicts=verdicts)
key_df

# %%
plot = test_df.groupby("model")["ndcg@10"].agg(["mean", "std"]).reindex(rerun)
fig, ax = plt.subplots(figsize=(7, 3.6))
ax.bar(plot.index, plot["mean"], yerr=plot["std"].fillna(0), color=["#d9692a" if i == selected else "#2c6a99" for i in plot.index])
ax.set_ylabel("NDCG@10 (test, common users)"); ax.set_title(f"Extended grid - selected on validation: {selected}")
save_fig(fig, "10_grid_extension"); plt.show()

# %%
out.update({"gate": gate, "selected": selected, "best_config": best_cfg, "at_grid_edge": at_edge,
            "validation_family_ndcg@10": family_val, "validation_grid": val_df.to_dict("records"),
            "n_common_users": n_common, "test_by_model_seed": test_df.to_dict("records"),
            "test_summary": {m: {f"{c[0]}_{c[1]}": float(v) for c, v in row.items()} for m, row in summary.iterrows()},
            "models_full_coverage_with_popularity_fallback": coverage,
            "significance_selected_vs_others": sig_df.to_dict("records"),
            "significance_key_pairs": key_df.to_dict("records"), "verdicts": verdicts,
            "fit_seconds": {"als_by_seed": als_seconds}})
print("saved", save_result(NB, out))
mark_stage(NB, "saved")
