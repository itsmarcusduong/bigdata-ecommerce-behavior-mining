# %% [markdown]
# # 02 - Streaming algorithms on the event stream
# CS246 stream-mining toolkit applied to the click-stream, replayed **day by day** in event-time
# order (micro-batches = days): **Count-Min Sketch + heavy hitters**, **Bloom filter**, **DGIM**,
# and **HyperLogLog** (Spark `approx_count_distinct`).  Every sketch is compared with the exact answer
# computed by Spark, reporting accuracy *and* memory.

# %%
import time

import numpy as np
from pyspark.sql import functions as F

from bdm import config, io
from bdm.results import save_fig, save_result
from bdm.sketches import DGIM, BloomFilter, CountMinSketch, HeavyHitters

spark = config.get_spark("02-streaming", driver_memory=SPARK_MEM)
events = io.load_events(spark, FRACTION).cache()
days = [r[0] for r in events.select("event_date").distinct().orderBy("event_date").collect()]
print(len(days), "daily micro-batches;", f"{events.count():,} events")
results_out = {"fraction": FRACTION, "n_days": len(days)}

# %% [markdown]
# ## A. Count-Min Sketch + heavy hitters (most viewed products)
# Stream = `view` events in time order; the sketch never stores a per-product dictionary.

# %%
TOPK, EPS, DELTA = 20, 2e-5, 0.01
cms = CountMinSketch.from_error(EPS, DELTA, seed=1)
hh = HeavyHitters(TOPK, cms)
t0 = time.perf_counter(); n_stream = 0
for d in days:
    chunk = np.array([r[0] for r in events.filter((F.col("event_date") == d) & (F.col("event_type") == "view"))
                      .select("product_id").collect()], dtype=np.int64)
    hh.update(chunk); n_stream += len(chunk)
t_cms = time.perf_counter() - t0

exact = (events.filter(F.col("event_type") == "view").groupBy("product_id").count()
         .orderBy(F.desc("count")).cache())
exact_top = exact.limit(TOPK).toPandas()
est_top = pd.DataFrame(hh.top(), columns=["product_id", "cms_estimate"])
cmp_ = exact_top.merge(est_top, on="product_id", how="left")
overlap = cmp_["cms_estimate"].notna().mean()
rel_err = ((cmp_["cms_estimate"] - cmp_["count"]).abs() / cmp_["count"]).mean()
n_distinct = exact.count()
mem_sketch = cms.table.nbytes / 1e6
print(f"stream={n_stream:,} views, distinct products={n_distinct:,}")
print(f"top-{TOPK} overlap={overlap:.2f}  mean rel. error (top-{TOPK})={rel_err:.2e}")
print(f"sketch memory={mem_sketch:.1f} MB  (eps={EPS}, delta={DELTA}, width={cms.width}, depth={cms.depth}), "
      f"time={t_cms:.1f}s")
cmp_

# %% [markdown]
# **Error on the long tail.** CMS overestimates by at most `eps*N` with probability `1-delta`; we
# verify the guarantee on a random sample of products (not only the head).

# %%
sample = exact.sample(False, min(1.0, 20000 / max(n_distinct, 1)), 7).toPandas()
sample["est"] = cms.estimate_many(sample["product_id"].values)
sample["abs_err"] = sample["est"] - sample["count"]
bound = EPS * cms.n
print(f"never underestimates: {(sample['abs_err'] >= 0).all()} | "
      f"within eps*N={bound:,.0f}: {(sample['abs_err'] <= bound).mean():.4f} (target >= {1 - DELTA})")
fig, ax = plt.subplots(figsize=(5.5, 5))
ax.loglog(sample["count"] + 1, sample["est"] + 1, ".", alpha=.3)
lim = [1, sample["count"].max() * 1.5]; ax.plot(lim, lim, "k--", lw=1)
ax.set_xlabel("exact count"); ax.set_ylabel("CMS estimate"); ax.set_title("Count-Min accuracy")
save_fig(fig, "02_cms_accuracy"); plt.show()

# %% [markdown]
# ## B. Distributed build + merge (sketches are mergeable)
# Build one sketch per Spark partition (`mapPartitions`) and merge with `reduce` - the exact
# MapReduce pattern; the result is *identical* to the sequential sketch.

# %%
def build_partition_cms(rows):
    s = CountMinSketch.from_error(EPS, DELTA, seed=1)
    ids = np.fromiter((r[0] for r in rows), dtype=np.int64)
    s.update_many(ids)
    yield s

views_rdd = events.filter(F.col("event_type") == "view").select("product_id").rdd
t0 = time.perf_counter()
merged = views_rdd.mapPartitions(build_partition_cms).reduce(lambda a, b: a.merge(b))
t_dist = time.perf_counter() - t0
print("distributed == sequential:", bool((merged.table == cms.table).all()), f"| {t_dist:.1f}s")

# %% [markdown]
# **Several hash seeds.** One seed can be lucky. Rebuild the sketch (distributed) with 3 seeds and check the
# guarantee on the same long-tail sample each time.

# %%
SEEDS = [int(x) for x in os.environ.get("BDM_SEEDS", "42,7,2024").split(",")]
cms_seed_rows = []
for sd in SEEDS:
    def build_seeded(rows, sd=sd):
        sk = CountMinSketch.from_error(EPS, DELTA, seed=sd)
        sk.update_many(np.fromiter((r[0] for r in rows), dtype=np.int64))
        yield sk
    sk = views_rdd.mapPartitions(build_seeded).reduce(lambda a, b: a.merge(b))
    est = sk.estimate_many(sample["product_id"].values)
    err = est - sample["count"].values
    cms_seed_rows.append({"seed": sd, "never_under": bool((err >= 0).all()),
                          "within_bound": float((err <= EPS * sk.n).mean()), "mean_abs_err": float(np.abs(err).mean()),
                          "max_abs_err": float(err.max()), "bound": float(EPS * sk.n)})
cms_seeds = pd.DataFrame(cms_seed_rows); cms_seeds

# %% [markdown]
# ## C. Bloom filter - "is this a returning user?"
# For each day we test every user against the filter of all *previous* days (new/returning split).

# %%
n_users_total = int(events.select(F.approx_count_distinct("user_id")).first()[0])
m, k = BloomFilter.optimal_params(max(n_users_total, 1000), 0.01)
bf, seen_exact = BloomFilter(m, k, seed=3), set()
fp = tot_new = 0; rows = []
for d in days:
    users = np.array([r[0] for r in events.filter(F.col("event_date") == d).select("user_id").distinct()
                      .collect()], dtype=np.int64)
    in_bf = bf.contains_many(users)
    truly_seen = np.fromiter((u in seen_exact for u in users), dtype=bool, count=len(users))
    fp += int((in_bf & ~truly_seen).sum()); tot_new += int((~truly_seen).sum())
    rows.append((d, len(users), int(truly_seen.sum()), int(in_bf.sum())))
    bf.add_many(users); seen_exact.update(users.tolist())
bloom_df = pd.DataFrame(rows, columns=["day", "users", "returning_exact", "returning_bloom"])
fpr = fp / max(tot_new, 1)
print(f"Bloom: {m / 8 / 1e6:.1f} MB for k={k}; empirical FPR={fpr:.4f} (target 0.01); "
      f"exact set ~ {len(seen_exact) * 28 / 1e6:.1f} MB in Python")
bloom_df.head()

# %% [markdown]
# ## D. DGIM - purchases in the last N events (sliding window)
# Bit stream: `1` if the event is a purchase.  DGIM keeps O(log^2 N) buckets, error <= 50%.

# %%
N_STREAM, WINDOW = 1_000_000, 100_000
bits = np.array([r[0] for r in events.orderBy("event_time").select((F.col("event_type") == "purchase").cast("int"))
                 .limit(N_STREAM).collect()], dtype=np.int8)
dg, errs, csum = DGIM(WINDOW), [], np.cumsum(bits)
t0 = time.perf_counter()
for t, b in enumerate(bits, start=1):
    dg.update(int(b))
    if t >= WINDOW and t % 5000 == 0:
        exact_w = csum[t - 1] - (csum[t - WINDOW - 1] if t > WINDOW else 0)
        errs.append((t, exact_w, dg.count()))
t_dgim = time.perf_counter() - t0
e = pd.DataFrame(errs, columns=["t", "exact", "dgim"]); e["rel_err"] = (e["dgim"] - e["exact"]).abs() / e["exact"].clip(lower=1)
print(f"DGIM: buckets={len(dg.buckets)} (vs window {WINDOW:,}); max rel err={e['rel_err'].max():.3f}; "
      f"mean={e['rel_err'].mean():.3f}; {t_dgim:.1f}s")

# %% [markdown]
# ## E. Distinct counting (HyperLogLog++ in Spark) vs exact

# %%
t0 = time.perf_counter(); exact_u = events.select(F.countDistinct("user_id")).first()[0]; t_ex = time.perf_counter() - t0
rows = []
for rsd in (0.2, 0.05, 0.01):
    t0 = time.perf_counter()
    est = events.select(F.approx_count_distinct("user_id", rsd)).first()[0]
    rows.append((rsd, est, abs(est - exact_u) / exact_u, time.perf_counter() - t0))
hll = pd.DataFrame(rows, columns=["rsd", "estimate", "rel_err", "seconds"])
print(f"exact distinct users = {exact_u:,} ({t_ex:.1f}s)"); hll

# %% [markdown]
# **Is one run representative?** Spark's HyperLogLog++ uses a fixed hash, so the same data always gives the same
# estimate. To see the error *distribution*, hash salted copies of the ids (`user_id || '#' || salt`, same
# distinct count) with 10 salts per rsd and compare the spread with the target relative standard deviation.

# %%
N_SALTS = int(os.environ.get("BDM_HLL_SALTS", "10"))
users_only = events.select("user_id").distinct().cache(); users_only.count()
salt_rows = []
for rsd in (0.2, 0.05, 0.01):
    for salt in range(N_SALTS):
        est = users_only.select(F.approx_count_distinct(F.concat_ws("#", F.col("user_id").cast("string"), F.lit(str(salt))),
                                                        rsd)).first()[0]
        salt_rows.append({"rsd": rsd, "salt": salt, "estimate": est, "rel_err": (est - exact_u) / exact_u})
hll_salts = pd.DataFrame(salt_rows)
hll_spread = (hll_salts.groupby("rsd")["rel_err"]
              .agg(mean_rel_err="mean", sd_rel_err="std", mean_abs_rel_err=lambda v: v.abs().mean(),
                   within_2rsd=lambda v: None).reset_index())
hll_spread["within_2rsd"] = [float((hll_salts[hll_salts["rsd"] == r]["rel_err"].abs() <= 2 * r).mean())
                             for r in hll_spread["rsd"]]
hll_spread

# %%
results_out.update({
    "cms": {"eps": EPS, "delta": DELTA, "top_k_overlap": overlap, "top_k_mean_rel_err": float(rel_err),
            "memory_mb": mem_sketch, "n_stream": int(n_stream), "n_distinct_products": int(n_distinct),
            "within_bound_fraction": float((sample["abs_err"] <= bound).mean()), "seconds": t_cms,
            "distributed_equals_sequential": bool((merged.table == cms.table).all()), "distributed_seconds": t_dist},
    "bloom": {"n_bits": m, "k": k, "empirical_fpr": fpr, "target_fpr": 0.01, "memory_mb": m / 8e6},
    "dgim": {"window": WINDOW, "n_stream": int(len(bits)), "n_buckets": len(dg.buckets),
             "max_rel_err": float(e["rel_err"].max()), "mean_rel_err": float(e["rel_err"].mean())},
    "hll": hll.to_dict("records"), "exact_distinct_users": int(exact_u),
    "cms_seeds": cms_seeds.to_dict("records"), "hll_salted": hll_spread.to_dict("records"), "hll_salts": N_SALTS,
    "seeds": SEEDS})
print("saved", save_result("02_streaming_sketches", results_out))
