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
                     pair_recall_precision, s_curve, wilson_ci)
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
# ## 2. Ground truth on subsamples (naive O(n^2)) & recall of the LSH variants
# Two evaluation sets, both chosen **independently of LSH**:
# * **random** products - unbiased, but on the full data it contains very few pairs above the threshold;
# * **category-stratified** - all products of the largest categories (similar pairs concentrate inside a
#   category), giving enough true pairs for a meaningful recall. Recall gets a Wilson 95% interval.

# %%
N_EVAL = int(os.environ.get("BDM_LSH_EVAL", "10000"))  # naive O(n^2) baseline; more sets -> more true pairs
N_EVAL_CAT = int(os.environ.get("BDM_LSH_EVAL_CAT", "8000"))
sub = prod_sets.orderBy(F.rand(1)).limit(N_EVAL).collect()
sub_sets = {r["product_id"]: set(r["users"]) for r in sub}

prod_cat = events.groupBy("product_id").agg(F.first("category_id", ignorenulls=True).alias("category_id"))
cat_sets = prod_sets.join(prod_cat, "product_id")
cat_size = cat_sets.groupBy("category_id").count().orderBy(F.desc("count"), "category_id").collect()
chosen, total = [], 0
for row in cat_size:                      # whole categories, largest first, until N_EVAL_CAT products
    if total + row["count"] > N_EVAL_CAT and chosen:
        continue
    chosen.append(row["category_id"]); total += row["count"]
    if total >= 0.9 * N_EVAL_CAT:
        break
cat_sub = cat_sets.filter(F.col("category_id").isin(chosen)).collect()
cat_sub_sets = {r["product_id"]: set(r["users"]) for r in cat_sub}
print(f"category-stratified subset: {len(cat_sub_sets):,} products from {len(chosen)} categories")


def evaluate_subset(sets: dict, label: str) -> tuple[pd.DataFrame, dict]:
    t0 = time.perf_counter(); truth = set(exact_similar_pairs(sets, THRESH)); t_naive = time.perf_counter() - t0
    print(f"[{label}] naive on {len(sets):,} sets: {len(truth):,} similar pairs, {t_naive:.1f}s")
    rdd = spark.sparkContext.parallelize([(k, sorted(v)) for k, v in sets.items()], 16)
    rows = []
    for n_hashes, min_prob in [(64, 0.8), (128, 0.9), (128, 0.99), (256, 0.99)]:
        b, r = best_bands_rows(n_hashes, THRESH, min_prob)
        t0 = time.perf_counter()
        pairs, _ = lsh_similar_pairs(rdd, THRESH, n_hashes, seed=1, min_prob=min_prob)
        found = {k for k, _ in pairs.collect()}; t = time.perf_counter() - t0
        rec, prec = pair_recall_precision(found, truth)
        lo, hi = wilson_ci(len(found & truth), len(truth))
        rows.append({"subset": label, "n_hashes": n_hashes, "bands": b, "rows": r,
                     "P(candidate | s=t)": round(s_curve(THRESH, b, r), 3), "recall": round(rec, 4),
                     "recall_ci95": [round(lo, 4), round(hi, 4)], "precision": round(prec, 4), "seconds": round(t, 2)})
    mdf = spark.createDataFrame([(k, sorted(v)) for k, v in sets.items()], "pid long, users array<long>")
    t0 = time.perf_counter()
    ml = {(r.id_a, r.id_b) for r in mllib_minhash_pairs(mdf, "pid", "users", THRESH, num_tables=10).collect()}
    t_ml = time.perf_counter() - t0
    rec_ml, prec_ml = pair_recall_precision(ml, truth)
    lo, hi = wilson_ci(len(ml & truth), len(truth))
    rows.append({"subset": label, "n_hashes": "MLlib 10 tables", "bands": None, "rows": None, "P(candidate | s=t)": None,
                 "recall": round(rec_ml, 4), "recall_ci95": [round(lo, 4), round(hi, 4)],
                 "precision": round(prec_ml, 4), "seconds": round(t_ml, 2)})
    return pd.DataFrame(rows), {"n_sets": len(sets), "n_true_pairs": len(truth), "naive_seconds": t_naive}


lsh_tab, info_rand = evaluate_subset(sub_sets, "random")
lsh_tab_cat, info_cat = evaluate_subset(cat_sub_sets, "category-stratified")
t_naive = info_rand["naive_seconds"]
pd.concat([lsh_tab, lsh_tab_cat], ignore_index=True)

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
            "n_true_pairs_eval": info_rand["n_true_pairs"], "lsh_vs_naive": lsh_tab.to_dict("records"),
            "eval_category_stratified": {**info_cat, "n_categories": len(chosen),
                                         "lsh_vs_naive": lsh_tab_cat.to_dict("records")},
            "scaling": scal.to_dict("records"), "naive_exponent": coef,
            "lsh_all": {"pairs": n_pairs_all, "seconds": t_all, "bands": info_all["bands"], "rows": info_all["rows"],
                        "naive_extrapolated_hours": naive_est / 3600}})
print("saved", save_result("04_lsh_similarity", out))
