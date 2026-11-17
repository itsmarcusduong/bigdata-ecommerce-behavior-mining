# %% [markdown]
# # 03 - Frequent itemsets & association rules
# * **Instacart** (3.4M orders, 32M order lines): *true* shopping baskets.
# * **REES46** sessions with `cart`/`purchase` events as baskets (noisier, bigger catalogue).
#
# Algorithms: own **A-Priori** and **PCY** (RDD, two passes) vs Spark MLlib **FP-Growth**.
# Ablation: candidate pairs counted by A-Priori vs PCY (memory saving) and runtime vs support.
#
# **Kaggle inputs:** `psparks/instacart-market-basket-analysis` (plain CSVs) + the REES46 data / notebook-01 output.

# %%
import math
import time

from pyspark.sql import functions as F

from bdm import config, eda, io
from bdm.fpm import count_pairs, fpgrowth
from bdm.results import save_fig, save_result
from bdm.sessions import instacart_baskets, session_baskets

spark = config.get_spark("03-itemsets", driver_memory=SPARK_MEM)
out = {"fraction": FRACTION}

# %% [markdown]
# ## 1. Instacart baskets

# %%
ic = io.read_instacart(spark)
prior = ic["prior"]
if FRACTION < 1.0:   # sample whole orders so baskets stay intact
    prior = prior.filter((F.abs(F.hash("order_id")) % 10000) < int(FRACTION * 10000))
baskets = instacart_baskets(prior).cache()
n_baskets = baskets.count()
names = {r["product_id"]: r["product_name"] for r in ic["products"].select("product_id", "product_name").collect()}
print(f"{n_baskets:,} baskets (>=2 items) from {prior.count():,} order lines")

# %% [markdown]
# ## 2. A-Priori vs PCY vs FP-Growth - pair counting
# Absolute support `s = rel_support * N`. PCY hashes pairs into buckets during pass 1 and only counts
# pairs whose bucket is frequent in pass 2; the result must be **identical** to A-Priori.

# %%
rdd = baskets.select("items").rdd.map(lambda r: r[0])
rows = []
for rel in (0.01, 0.005, 0.002, 0.001):
    s = max(2, math.ceil(rel * n_baskets))   # same rounding as MLlib (minCount = ceil(minSupport * N))
    t0 = time.perf_counter(); ap_pairs, ap = count_pairs(rdd, s); t_ap = time.perf_counter() - t0
    t0 = time.perf_counter(); pcy_pairs, pcy = count_pairs(rdd, s, n_buckets=4_000_037); t_pcy = time.perf_counter() - t0
    same = ap["n_frequent_pairs"] == pcy["n_frequent_pairs"]
    t0 = time.perf_counter()
    fp_freq, _ = fpgrowth(baskets, "items", rel, 0.0)
    n_fp_pairs = fp_freq.filter(F.size("items") == 2).count(); t_fp = time.perf_counter() - t0
    rows.append({"rel_support": rel, "abs_support": s, "frequent_pairs": ap["n_frequent_pairs"],
                 "apriori_candidates": ap["n_candidate_pairs_counted"],
                 "pcy_candidates": pcy["n_candidate_pairs_counted"], "same_result": same,
                 "fpgrowth_pairs": n_fp_pairs, "t_apriori_s": round(t_ap, 1), "t_pcy_s": round(t_pcy, 1),
                 "t_fpgrowth_s": round(t_fp, 1)})
    print(rows[-1])
bench = pd.DataFrame(rows); bench["pcy_candidate_reduction"] = 1 - bench["pcy_candidates"] / bench["apriori_candidates"]
bench

# %%
fig, axes = plt.subplots(1, 2, figsize=(12, 3.8))
bench.plot(x="rel_support", y=["t_apriori_s", "t_pcy_s", "t_fpgrowth_s"], logx=True, marker="o", ax=axes[0])
axes[0].invert_xaxis(); axes[0].set_ylabel("seconds"); axes[0].set_title("Runtime vs support")
bench.plot(x="rel_support", y=["apriori_candidates", "pcy_candidates", "frequent_pairs"], logx=True, logy=True,
           marker="o", ax=axes[1]); axes[1].invert_xaxis(); axes[1].set_title("Candidate pairs counted (pass 2)")
save_fig(fig, "03_apriori_pcy_fpgrowth"); plt.show()

# %% [markdown]
# ## 3. Association rules (FP-Growth, k-itemsets) with product names

# %%
REL, CONF = 0.001, 0.2
t0 = time.perf_counter(); freq, rules = fpgrowth(baskets, "items", REL, CONF); freq.cache()
n_itemsets = freq.count(); t_fp = time.perf_counter() - t0
size_hist = freq.groupBy(F.size("items").alias("k")).count().orderBy("k").toPandas()
print(f"{n_itemsets:,} frequent itemsets at support {REL} in {t_fp:.1f}s"); size_hist

# %%
def name(ids): return " + ".join(names.get(i, str(i)) for i in ids)
top_rules = (rules.filter(F.col("lift") > 2).orderBy(F.desc("lift")).limit(15).collect())
pd.DataFrame([{"antecedent": name(r["antecedent"]), "consequent": name(r["consequent"]),
               "confidence": round(r["confidence"], 3), "lift": round(r["lift"], 2),
               "support": round(r["support"], 4)} for r in top_rules])

# %% [markdown]
# ## 4. REES46 session baskets (cart + purchase)

# %%
events = io.load_events(spark, FRACTION)
meta = eda.product_meta(events).cache()
sb = session_baskets(events, ("cart", "purchase"), min_items=2, max_items=50).cache()
n_sb = sb.count()
freq2, rules2 = fpgrowth(sb, "items", max(0.0005, 5 / max(n_sb, 1)), 0.1)
cat = {r["product_id"]: f"{r['brand'] or '?'}/{(r['category_code'] or '?').split('.')[-1]}"
       for r in meta.collect()} if meta.count() < 3_000_000 else {}
top2 = rules2.orderBy(F.desc("lift")).limit(10).collect()
print(f"{n_sb:,} session baskets; {freq2.count():,} frequent itemsets; {rules2.count():,} rules")
pd.DataFrame([{"antecedent": [cat.get(i, i) for i in r["antecedent"]],
               "consequent": [cat.get(i, i) for i in r["consequent"]],
               "confidence": round(r["confidence"], 3), "lift": round(r["lift"], 1)} for r in top2])

# %%
out.update({"instacart": {"n_baskets": n_baskets, "benchmark": bench.to_dict("records"),
                          "fpgrowth_support": REL, "n_itemsets": n_itemsets, "fpgrowth_seconds": t_fp,
                          "itemset_size_hist": size_hist.to_dict("records"), "n_rules_lift_gt_2": rules.filter(F.col("lift") > 2).count()},
            "rees46_sessions": {"n_baskets": n_sb, "n_itemsets": freq2.count(), "n_rules": rules2.count()}})
print("saved", save_result("03_frequent_itemsets", out))
