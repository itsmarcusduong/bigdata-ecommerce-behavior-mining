# %% [markdown]
# # 06 - Scalable recommendation: which model is best, under a rule fixed in advance
# Implicit-feedback recommendation with a **strict temporal split** (train: before cutoff, test: after).
# Ground truth = products a *known* user **purchased** in the test period; already-bought products are removed
# from every list before scoring.
#
# | family | idea | grid (validation) |
# |---|---|---|
# | Popularity | global top sellers in train | - |
# | Recent popularity | top sellers in the last *d* days of train | d in {3, 7, 14} |
# | History | the user's own viewed/carted products, weight x recency decay | half-life in {1, 3, 7} days |
# | Item-kNN | cosine co-occurrence from train sessions | history = cart+purchase or all events |
# | ALS (implicit) | matrix factorisation of the weighted user-item matrix | rank {16, 32, 64} x alpha {10, 40} |
# | History + ALS | the user's history first, ALS fills the rest | best history x best ALS |
#
# **Pre-registered protocol.**
# 1. Every configuration is fitted on data before the validation week and scored on that week (NDCG@10, all
#    validation users, popularity fills users a model cannot serve). The best configuration of each family is kept.
# 2. **Selection rule**: the family with the highest validation NDCG@10 is the selected model. The test period is
#    not used for any choice.
# 3. Each family's best configuration is refitted on all training data and scored **once** on test, on the users all
#    families can serve (and, separately, on all users with the popularity fallback).
# 4. ALS-based models are refitted with **3 seeds**. "A is better than B" only if the paired-bootstrap 95% CI of
#    NDCG@10(A) - NDCG@10(B) excludes 0 **for every seed**; otherwise the result is reported as a tie.

# %%
import gc
import time

from datetime import date, timedelta

from pyspark.sql import functions as F

from bdm import config, io, recsys
from bdm.graph import cooccurrence_edges
from bdm.results import save_fig, save_result
from bdm.sessions import session_baskets, time_split, user_item_matrix

SEEDS = [int(s) for s in os.environ.get("BDM_SEEDS", "42,7,2024").split(",")]
spark = config.get_spark("06-recsys", driver_memory=SPARK_MEM)
events = io.load_events(spark, FRACTION)
CUTOFF = os.environ.get("BDM_CUTOFF", "2019-11-15")
train, test = time_split(events, CUTOFF)
train.cache(); test.cache()
truth, seen = recsys.truth_and_seen(train, test)
truth.cache(); seen.cache()
users = truth.select("user_id")
n_eval = truth.count()
print(f"cutoff {CUTOFF}: train {train.count():,} / test {test.count():,} events; evaluable users: {n_eval:,}")
VAL_CUTOFF = str(date.fromisoformat(CUTOFF) - timedelta(days=7))
fit, val = time_split(train, VAL_CUTOFF)
truth_v, seen_v = recsys.truth_and_seen(fit, val)
truth_v.cache(); seen_v.cache()
users_v = truth_v.select("user_id")
n_val = truth_v.count()
print(f"validation: fit < {VAL_CUTOFF} <= val < {CUTOFF}; validation users: {n_val:,}")
out = {"fraction": FRACTION, "cutoff": CUTOFF, "val_cutoff": VAL_CUTOFF, "n_eval_users": n_eval,
       "n_val_users": n_val, "seeds": SEEDS,
       "selection_rule": "highest validation NDCG@10 (all validation users, popularity fallback); "
                         "A>B on test only if paired-bootstrap 95% CI excludes 0 for every seed"}

# %% [markdown]
# ## 1. Validation: a grid for every family (fit < validation cutoff, scored on the validation week)

# %%
def knn_recs(src, target_users, hist_types, top_n=50):
    edges = cooccurrence_edges(spark, session_baskets(src, ("view", "cart", "purchase"), 2, 30), min_weight=2)
    nbrs = recsys.item_neighbors(edges, top_n)
    hist = src.filter(F.col("event_type").isin(*hist_types)).select("user_id", "product_id").join(target_users, "user_id")
    return recsys.itemknn_recs(hist, nbrs, 100)


pop_v = recsys.popularity_recs(fit.filter(F.col("event_type") == "purchase"), users_v, 100).cache()
val_rows = []


def score_val(family, params, recs, t_s):
    m = recsys.evaluate(recsys.with_fallback(recs, pop_v), truth_v, seen_v)
    row = {"family": family, "params": params, "fit_s": round(t_s, 1), **{f"val_{k}": round(v, 5) for k, v in m.items()}}
    val_rows.append(row); print(row)
    return m["ndcg@10"]


t0 = time.perf_counter(); score_val("popularity", {}, pop_v, time.perf_counter() - t0)
for d in (3, 7, 14):
    t0 = time.perf_counter(); r = recsys.recent_popularity_recs(fit, users_v, VAL_CUTOFF, days=d)
    score_val("recent_popularity", {"days": d}, r, time.perf_counter() - t0)
for hl in (1.0, 3.0, 7.0):
    t0 = time.perf_counter(); r = recsys.history_recs(fit, users_v, VAL_CUTOFF, half_life_days=hl).cache()
    score_val("history", {"half_life_days": hl}, r, time.perf_counter() - t0); r.unpersist()
for ht in (("cart", "purchase"), ("view", "cart", "purchase")):
    t0 = time.perf_counter(); r = knn_recs(fit, users_v, ht).cache()
    score_val("item_knn", {"history": "+".join(ht)}, r, time.perf_counter() - t0); r.unpersist()

ui_fit = user_item_matrix(fit, min_user_items=3, min_item_users=5).cache()
print(f"fit interaction matrix: {ui_fit.count():,} non-zeros")
best_als_val, best_als_model = -1.0, None
for rank in (16, 32, 64):
    for alpha in (10.0, 40.0):
        t0 = time.perf_counter()
        model = recsys.train_als(ui_fit, rank=rank, reg=0.05, alpha=alpha, max_iter=8, seed=SEEDS[0])
        t_fit = time.perf_counter() - t0
        s = score_val("als", {"rank": rank, "alpha": alpha}, recsys.als_recs(model, users_v, 100), t_fit)
        if s > best_als_val:
            best_als_val, best_als_model = s, model
        else:
            del model; gc.collect()

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
family_val = {fam: {"ndcg@10": float(g["val_ndcg@10"].max())} for fam, g in val_df.groupby("family")}
selected = recsys.select_best(family_val)
print("best configuration per family:", best_cfg)
print("validation NDCG@10 per family:", {k: round(v["ndcg@10"], 4) for k, v in family_val.items()})
print("SELECTED (pre-registered rule):", selected)
val_df.sort_values("val_ndcg@10", ascending=False)

# %% [markdown]
# ## 2. Test, once: each family's best configuration refitted on all training data

# %%
t0 = time.perf_counter()
pop = recsys.popularity_recs(train.filter(F.col("event_type") == "purchase"), users, 100).cache()
recent = recsys.recent_popularity_recs(train, users, CUTOFF, days=best_cfg["recent_popularity"]["days"]).cache()
hist_r = recsys.history_recs(train, users, CUTOFF, half_life_days=hl_best).cache()
knn_t0 = time.perf_counter()
knn = knn_recs(train, users, tuple(best_cfg["item_knn"]["history"].split("+"))).cache()
knn.count(); knn_seconds = round(time.perf_counter() - knn_t0, 1)
ui = user_item_matrix(train, min_user_items=3, min_item_users=5).cache()
als_by_seed, blend_by_seed, als_seconds = {}, {}, {}
for sd in SEEDS:
    t1 = time.perf_counter()
    mdl = recsys.train_als(ui, rank=best_cfg["als"]["rank"], reg=0.05, alpha=best_cfg["als"]["alpha"], max_iter=8, seed=sd)
    als_by_seed[sd] = recsys.als_recs(mdl, users, 100).cache(); als_by_seed[sd].count()
    als_seconds[sd] = round(time.perf_counter() - t1, 1)
    blend_by_seed[sd] = recsys.blend_recs(hist_r, als_by_seed[sd]).cache(); blend_by_seed[sd].count()
    del mdl; gc.collect()
common = (als_by_seed[SEEDS[0]].select("user_id").join(knn.select("user_id"), "user_id")
          .join(hist_r.select("user_id"), "user_id").join(users, "user_id").cache())
n_common = common.count()
print(f"users every family can serve: {n_common:,} of {n_eval:,}; refits in {time.perf_counter() - t0:.0f}s")

deterministic = {"popularity": pop, "recent_popularity": recent, "history": hist_r, "item_knn": knn}
test_rows, per_user, coverage = [], {}, {}
for name, r in deterministic.items():
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
# ## 3. Is the selected model really better? Paired bootstrap against every other family, for every seed

# %%
def pu(name, sd):
    return per_user[(name, sd if name in ("als", "history_als") else None)]


families = ["popularity", "recent_popularity", "history", "item_knn", "als", "history_als"]
sig = []
for other in [f for f in families if f != selected]:
    for sd in SEEDS:
        b = recsys.paired_bootstrap(pu(selected, sd), pu(other, sd), seed=sd)
        sig.append({"selected": selected, "other": other, "seed": sd, **b, "verdict": recsys.verdict(b)})
sig_df = pd.DataFrame(sig)
overall = {o: ("better" if (g["verdict"] == "better").all() else "worse" if (g["verdict"] == "worse").all() else "tie")
           for o, g in sig_df.groupby("other")}
print(f"selected = {selected}; verdict vs each other family over {len(SEEDS)} seeds:", overall)
sig_df

# %%
plot = test_df.groupby("model")["ndcg@10"].agg(["mean", "std"]).reindex(families)
fig, ax = plt.subplots(figsize=(8, 3.8))
ax.bar(plot.index, plot["mean"], yerr=plot["std"].fillna(0), color=["#1f77b4" if i == selected else "#bbbbbb" for i in plot.index])
ax.set_ylabel("NDCG@10 (test, common users)"); ax.set_title(f"Selected on validation: {selected}")
plt.xticks(rotation=20); save_fig(fig, "06_model_comparison"); plt.show()

# %% [markdown]
# ### Ablation: which behaviour signals matter for ALS? (selected ALS setting, seed 1, common users)

# %%
abl = {}
for name, types in {"views only": ["view"], "cart+purchase only": ["cart", "purchase"],
                    "all events (weighted)": ["view", "cart", "purchase"]}.items():
    mat = user_item_matrix(train.filter(F.col("event_type").isin(*types)), 2, 5)
    mdl = recsys.train_als(mat, rank=best_cfg["als"]["rank"], alpha=best_cfg["als"]["alpha"], max_iter=8, seed=SEEDS[0])
    abl[name] = recsys.evaluate(recsys.with_fallback(recsys.als_recs(mdl, users, 100), pop), truth, seen, users=common)
    del mdl; gc.collect()
abl_df = pd.DataFrame(abl).T[["precision@10", "recall@10", "map@10", "ndcg@10", "ndcg@20", "n_users"]]
abl_df

# %% [markdown]
# ## 4. Error analysis: recall@10 by length of the user's training history (history model vs ALS)

# %%
tr_cnt = ui.groupBy("user_id").count().withColumnRenamed("count", "n_hist")
err_rows = []
for name, rdf in (("history", hist_r), ("als", als_by_seed[SEEDS[0]])):
    rows = (rdf.join(common, "user_id").join(truth, "user_id").join(seen, "user_id", "left").join(tr_cnt, "user_id", "left")
            .orderBy(F.hash("user_id")).limit(50_000).select("recs", "truth", "seen", "n_hist").collect())
    for recs, truth_, seen_, n_hist in rows:
        s = set(seen_ or []); r = [x for x in recs if x not in s]
        err_rows.append((name, n_hist or 0, recsys.recall_at_k(r, truth_, 10)))
err = pd.DataFrame(err_rows, columns=["model", "n_hist", "recall@10"])
err["history_bucket"] = pd.cut(err["n_hist"], [-1, 4, 9, 19, 49, 10**9], labels=["<5", "5-9", "10-19", "20-49", "50+"])
by_hist = err.groupby(["model", "history_bucket"], observed=True)["recall@10"].agg(["mean", "count"]).reset_index()
by_hist

# %%
out.update({"selected": selected, "best_config": best_cfg, "validation_family_ndcg@10": family_val,
            "validation_grid": val_df.to_dict("records"), "n_common_users": n_common,
            "test_by_model_seed": test_df.to_dict("records"),
            "test_summary": {m: {f"{c[0]}_{c[1]}": float(v) for c, v in row.items()} for m, row in summary.iterrows()},
            "models_full_coverage_with_popularity_fallback": coverage,
            "significance_selected_vs_others": sig_df.to_dict("records"), "verdict_selected_vs_others": overall,
            "fit_seconds": {"item_knn": knn_seconds, "als_by_seed": als_seconds},
            "ablation_events": abl_df.to_dict("index"),
            "error_by_history": by_hist.astype({"history_bucket": str}).to_dict("records")})
print("saved", save_result("06_recommender", out))
