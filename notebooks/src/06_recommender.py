# %% [markdown]
# # 06 - Scalable recommendation
# Implicit-feedback recommender with a **strict temporal split** (train: before cutoff, test: after).
# Ground truth = products a *known* user **purchased** in the test period (already-bought products are
# removed from the recommendations before scoring).
#
# | model | idea |
# |---|---|
# | Popularity | global top sellers (baseline) |
# | Item-kNN | cosine-normalised co-occurrence from **train sessions** only |
# | ALS (implicit) | matrix factorisation on the weighted user-item matrix |
#
# Metrics: Precision/Recall/HitRate/MAP/NDCG @10,@20.  Ablations: rank, alpha, event weighting.
#
# **Protocol.** Hyper-parameters are chosen on a **validation** week (the 7 days before the cutoff, model fitted
# on the data before it); the test period is used once, for the final comparison. All models are compared on the
# **same users** (those every model can serve), plus a full-coverage view where cold users get popularity.
# Differences are tested with a **paired bootstrap over users** (95% CI).

# %%
import time

from datetime import date, timedelta

from pyspark.sql import functions as F

from bdm import config, io, recsys
from bdm.graph import cooccurrence_edges
from bdm.results import save_fig, save_result
from bdm.sessions import session_baskets, time_split, user_item_matrix

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
print(f"validation: fit < {VAL_CUTOFF} <= val < {CUTOFF}; validation users: {truth_v.count():,}")
out = {"fraction": FRACTION, "cutoff": CUTOFF, "val_cutoff": VAL_CUTOFF, "n_eval_users": n_eval}
results = {}

# %% [markdown]
# ## 1. Baselines

# %%
pop = recsys.popularity_recs(train.filter(F.col("event_type") == "purchase"), users, 100)
results["popularity"] = recsys.evaluate(pop, truth, seen)

t0 = time.perf_counter()
edges = cooccurrence_edges(spark, session_baskets(train, ("view", "cart", "purchase"), 2, 30), min_weight=2).cache()
nbrs = recsys.item_neighbors(edges, 50).cache()
hist = train.filter(F.col("event_type").isin("cart", "purchase")).select("user_id", "product_id").join(users, "user_id")
knn = recsys.itemknn_recs(hist, nbrs, 100)
knn_seconds = round(time.perf_counter() - t0, 1)
results["item_knn"] = recsys.evaluate(knn, truth, seen)
pd.DataFrame(results).T   # each model on the users it can serve; the fair comparison is in section 3

# %% [markdown]
# ## 2. ALS - hyper-parameter grid (selected on the validation week)

# %%
ui = user_item_matrix(train, min_user_items=3, min_item_users=5).cache()
ui_fit = user_item_matrix(fit, min_user_items=3, min_item_users=5).cache()
print(f"interaction matrix: {ui.count():,} non-zeros (fit part: {ui_fit.count():,})")
grid, best_key, best_ndcg = [], None, -1
for rank in (16, 32, 64):
    for alpha in (10.0, 40.0):
        t0 = time.perf_counter()
        model = recsys.train_als(ui_fit, rank=rank, reg=0.05, alpha=alpha, max_iter=8)
        t_train = time.perf_counter() - t0
        m = recsys.evaluate(recsys.als_recs(model, users_v, 100), truth_v, seen_v)   # validation, not test
        row = {"rank": rank, "alpha": alpha, "train_s": round(t_train, 1),
               **{f"val_{k}": round(v, 4) for k, v in m.items()}}
        grid.append(row)
        if m["ndcg@10"] > best_ndcg:
            best_ndcg, best_key = m["ndcg@10"], (rank, alpha)
        print(row)
best_model = recsys.train_als(ui, rank=best_key[0], reg=0.05, alpha=best_key[1], max_iter=8)  # refit on all train
print("selected on validation:", best_key)
grid_df = pd.DataFrame(grid); grid_df

# %% [markdown]
# ## 3. Final comparison

# %%
als_r = recsys.als_recs(best_model, users, 100).cache()
knn = knn.cache(); pop = pop.cache()
common = als_r.select("user_id").join(knn.select("user_id"), "user_id").join(users, "user_id").cache()
n_common = common.count()
print(f"users every model can serve: {n_common:,} of {n_eval:,}")
cand = {"popularity": pop, "item_knn": knn, "als_best": als_r}
results, per_user = {}, {}
for name, r in cand.items():
    m = recsys.evaluate(r, truth, seen, users=common, per_user="ndcg@10")
    per_user[name] = m.pop("_per_user")
    results[name] = m
results["als_best"].update({"rank": best_key[0], "alpha": best_key[1]})
results["item_knn"]["train_seconds"] = knn_seconds
coverage = {name: recsys.evaluate(recsys.with_fallback(r, pop), truth, seen) for name, r in cand.items()}
significance = {f"{a}_vs_{b}": recsys.paired_bootstrap(per_user[a], per_user[b])
                for a, b in [("als_best", "popularity"), ("als_best", "item_knn"), ("item_knn", "popularity")]}
print(pd.DataFrame(significance).T)
final = pd.DataFrame(results).T[["precision@10", "recall@10", "hit_rate@10", "map@10", "ndcg@10", "ndcg@20", "n_users"]]
fig, ax = plt.subplots(figsize=(7, 3.8))
final[["recall@10", "map@10", "ndcg@10"]].plot.bar(ax=ax, rot=0); ax.set_title(f"Test metrics on {n_common:,} common users (cutoff {CUTOFF})")
save_fig(fig, "06_model_comparison"); plt.show()
final

# %% [markdown]
# ### Ablation: which behaviour signals matter? (rank/alpha fixed to the selected setting, common users)

# %%
rank, alpha = best_key
variants = {"views only": ["view"], "cart+purchase only": ["cart", "purchase"], "all events (weighted)": ["view", "cart", "purchase"]}
abl = {}
for name, types in variants.items():
    mat = user_item_matrix(train.filter(F.col("event_type").isin(*types)), 2, 5)
    mdl = recsys.train_als(mat, rank=rank, alpha=alpha, max_iter=8)
    # same users for every variant: users a variant cannot serve fall back to popularity
    abl[name] = recsys.evaluate(recsys.with_fallback(recsys.als_recs(mdl, users, 100), pop), truth, seen, users=common)
abl_df = pd.DataFrame(abl).T[["precision@10", "recall@10", "map@10", "ndcg@10", "ndcg@20", "n_users"]]
abl_df

# %% [markdown]
# ## 4. Error analysis: who benefits?
# Recall@10 of the best ALS model by the user's amount of training history.

# %%
rec_df = als_r
tr_cnt = ui.groupBy("user_id").count().withColumnRenamed("count", "n_hist")
rows = rec_df.join(truth, "user_id").join(seen, "user_id", "left").join(tr_cnt, "user_id", "left").select("recs", "truth", "seen", "n_hist").collect()
recs_rows = []
for recs, truth_, seen_, n_hist in rows:
    s = set(seen_ or []); r = [x for x in recs if x not in s]
    recs_rows.append((n_hist or 0, recsys.recall_at_k(r, truth_, 10)))
err = pd.DataFrame(recs_rows, columns=["n_hist", "recall@10"])
err["history_bucket"] = pd.cut(err["n_hist"], [-1, 4, 9, 19, 49, 10**9], labels=["<5", "5-9", "10-19", "20-49", "50+"])
by_hist = err.groupby("history_bucket", observed=True)["recall@10"].agg(["mean", "count"]); by_hist

# %%
out.update({"n_common_users": n_common, "models": results, "models_full_coverage_with_popularity_fallback": coverage,
            "significance_ndcg@10_paired_bootstrap": significance, "als_grid_validation": grid_df.to_dict("records"),
            "ablation_events": abl_df.to_dict("index"), "error_by_history": by_hist.reset_index().astype({"history_bucket": str}).to_dict("records")})
print("saved", save_result("06_recommender", out))
