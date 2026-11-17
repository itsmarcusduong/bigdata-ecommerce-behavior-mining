# %% [markdown]
# # 04 - Similar items at scale: Shingling-style sets, MinHash and LSH
# *Documents* here are **sets of product ids touched by a user** (user similarity) and **sets of users
# who touched a product** (product similarity).  We compare
# 1. **Naive** all-pairs exact Jaccard (O(n^2)),
# 2. **Own** MinHash + banding LSH (RDD) with exact verification,
# 3. **Spark MLlib** `MinHashLSH`,
#
# and quantify **recall vs candidates vs runtime**, the S-curve, and the quadratic-vs-near-linear scaling.

# %%
import time

import numpy as np
from pyspark.sql import functions as F

from bdm import config, eda, io
from bdm.lsh import (best_bands_rows, exact_similar_pairs, lsh_similar_pairs, mllib_minhash_pairs,
                     pair_recall_precision, s_curve)
from bdm.results import save_fig, save_result

spark = config.get_spark("04-lsh", driver_memory=SPARK_MEM)
events = io.load_events(spark, FRACTION).filter(F.col("event_type").isin("view", "cart", "purchase"))
THRESH = 0.3          # Jaccard similarity threshold for "similar"
out = {"fraction": FRACTION, "threshold": THRESH}

# %% [markdown]
# ## 1. Build the sets (products -> sets of users)
# Keep products seen by >= MIN_USERS users; cap set size to bound the cost of very popular products.

# %%
MIN_USERS, MAX_SET = 5, 2000
prod_sets = (events.select("product_id", "user_id").distinct().groupBy("product_id")
             .agg(F.collect_set("user_id").alias("users")).filter(F.size("users").between(MIN_USERS, MAX_SET)).cache())
n_prod = prod_sets.count()
print(f"{n_prod:,} product-sets; mean size {prod_sets.select(F.avg(F.size('users'))).first()[0]:.1f}")

# %% [markdown]
# ## 2. Ground truth on a subsample (naive O(n^2)) & recall of the LSH variants

# %%
N_EVAL = 3000
sub = prod_sets.orderBy(F.rand(1)).limit(N_EVAL).collect()
sub_sets = {r["product_id"]: set(r["users"]) for r in sub}
t0 = time.perf_counter(); truth_d = exact_similar_pairs(sub_sets, THRESH); t_naive = time.perf_counter() - t0
truth = set(truth_d)
print(f"naive on {len(sub_sets)} sets: {len(truth):,} similar pairs, {t_naive:.1f}s")

sub_rdd = spark.sparkContext.parallelize([(k, sorted(v)) for k, v in sub_sets.items()], 16)
rows = []
for n_hashes, min_prob in [(64, 0.8), (128, 0.9), (128, 0.99), (256, 0.99)]:
    b, r = best_bands_rows(n_hashes, THRESH, min_prob)
    t0 = time.perf_counter()
    pairs, info = lsh_similar_pairs(sub_rdd, THRESH, n_hashes, seed=1, min_prob=min_prob)
    found = {k for k, _ in pairs.collect()}; t = time.perf_counter() - t0
    rec, prec = pair_recall_precision(found, truth)
    rows.append({"n_hashes": n_hashes, "bands": b, "rows": r, "P(candidate | s=t)": round(s_curve(THRESH, b, r), 3),
                 "recall": round(rec, 4), "precision": round(prec, 4), "seconds": round(t, 2)})
lsh_tab = pd.DataFrame(rows)
# MLlib MinHashLSH
mdf = spark.createDataFrame([(k, sorted(v)) for k, v in sub_sets.items()], "pid long, users array<long>")
t0 = time.perf_counter()
ml = {(r.id_a, r.id_b) for r in mllib_minhash_pairs(mdf, "pid", "users", THRESH, num_tables=10).collect()}
t_ml = time.perf_counter() - t0
rec_ml, prec_ml = pair_recall_precision(ml, truth)
lsh_tab.loc[len(lsh_tab)] = {"n_hashes": "MLlib 10 tables", "bands": None, "rows": None, "P(candidate | s=t)": None,
                             "recall": round(rec_ml, 4), "precision": round(prec_ml, 4), "seconds": round(t_ml, 2)}
lsh_tab

# %% [markdown]
# ## 3. S-curve: theory vs measurement

# %%
s = np.linspace(0, 1, 200)
fig, ax = plt.subplots(figsize=(6.5, 4))
for b, r in [(best_bands_rows(128, THRESH)), best_bands_rows(128, THRESH, 0.9), best_bands_rows(128, THRESH, 0.99)]:
    ax.plot(s, [s_curve(x, b, r) for x in s], label=f"b={b}, r={r}")
ax.axvline(THRESH, color="k", ls=":"); ax.set_xlabel("Jaccard similarity"); ax.set_ylabel("P(candidate pair)")
ax.legend(); ax.set_title("LSH S-curves")
save_fig(fig, "04_s_curve"); plt.show()

# %% [markdown]
# ## 4. Scalability: naive vs LSH (on growing subsets) and LSH on *all* products

# %%
rows = []
for n in (250, 500, 1000, 2000, 3000):
    ids = list(sub_sets)[:n]; sets_n = {i: sub_sets[i] for i in ids}
    t0 = time.perf_counter(); exact_similar_pairs(sets_n, THRESH); tn = time.perf_counter() - t0
    rdd_n = spark.sparkContext.parallelize([(k, sorted(v)) for k, v in sets_n.items()], 16)
    t0 = time.perf_counter(); p, _ = lsh_similar_pairs(rdd_n, THRESH, 128, seed=1, min_prob=0.9); p.count(); tl = time.perf_counter() - t0
    rows.append({"n_sets": n, "naive_s": round(tn, 2), "lsh_s": round(tl, 2)})
scal = pd.DataFrame(rows)
coef = np.polyfit(np.log(scal["n_sets"]), np.log(scal["naive_s"].clip(lower=1e-3)), 1)[0]
print(f"empirical naive exponent ~ n^{coef:.2f}")
t0 = time.perf_counter()
all_rdd = prod_sets.select("product_id", "users").rdd.map(lambda r: (r[0], r[1]))
all_pairs, info_all = lsh_similar_pairs(all_rdd, THRESH, 128, seed=1, min_prob=0.9)
n_pairs_all = all_pairs.count(); t_all = time.perf_counter() - t0
naive_est = scal["naive_s"].iloc[-1] * (n_prod / scal["n_sets"].iloc[-1]) ** 2
print(f"LSH on all {n_prod:,} products: {n_pairs_all:,} similar pairs in {t_all:.1f}s; naive extrapolated ~ {naive_est / 3600:.1f} h")
scal

# %% [markdown]
# ## 5. Application: "customers who touched this also touched" via similar products

# %%
meta = eda.product_meta(events).cache()
m = {r["product_id"]: (r["category_code"], r["brand"]) for r in meta.collect()} if n_prod < 3_000_000 else {}
top = sorted(all_pairs.takeOrdered(10, key=lambda kv: -kv[1]))
pd.DataFrame([{"product_a": a, "product_b": b, "jaccard": round(j, 3), "cat_a": m.get(a, (None,))[0],
               "cat_b": m.get(b, (None,))[0], "same_category": m.get(a, (1,))[0] == m.get(b, (2,))[0]}
              for (a, b), j in top])

# %%
out.update({"n_product_sets": n_prod, "eval_subset": N_EVAL, "naive_seconds_on_eval": t_naive,
            "n_true_pairs_eval": len(truth), "lsh_vs_naive": lsh_tab.to_dict("records"),
            "scaling": scal.to_dict("records"), "naive_exponent": coef,
            "lsh_all": {"pairs": n_pairs_all, "seconds": t_all, "bands": info_all["bands"], "rows": info_all["rows"],
                        "naive_extrapolated_hours": naive_est / 3600}})
print("saved", save_result("04_lsh_similarity", out))
