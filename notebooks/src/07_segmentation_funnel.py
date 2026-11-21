# %% [markdown]
# # 07 - Customer behaviour: RFM segmentation, funnel and reorder patterns
# * **REES46**: conversion funnel per category; RFM features per buyer clustered by **three algorithms**
#   (K-Means, Bisecting K-Means, Gaussian mixture) x k in {3..6} x 3 seeds; segment profiles; hourly/weekday demand.
#
# **Pre-registered selection rule**: the (algorithm, k) with the highest mean silhouette over seeds, among settings
# where every seed uses all k clusters (a fit with fewer non-empty clusters is *degenerate* and excluded); it "beats" the runner-up only if its silhouette is higher for every seed, otherwise tie.
# Seed stability is reported as the mean pairwise adjusted Rand index (ARI) on a fixed sample of buyers.
# * **Instacart**: reorder behaviour (what fraction of items are repeat purchases; by hour/day).

# %%
import time

from pyspark.sql import functions as F

from bdm import config, io, segmentation
from bdm.results import save_fig, save_result

spark = config.get_spark("07-segments", driver_memory=SPARK_MEM)
events = io.load_events(spark, FRACTION).cache()
out = {"fraction": FRACTION}

# %% [markdown]
# ## 1. Funnel by top-level category

# %%
funnel = segmentation.funnel_by_category(events).toPandas()
funnel.head(12)

# %%
f = funnel.head(10).set_index("category")[["view_to_cart", "cart_to_purchase", "view_to_purchase"]]
fig, ax = plt.subplots(figsize=(10, 4)); f.plot.bar(ax=ax, rot=30); ax.set_title("Distinct-user funnel conversion by category")
save_fig(fig, "07_funnel"); plt.show()

# %% [markdown]
# ## 2. RFM + K-Means

# %%
SEEDS = [int(x) for x in os.environ.get("BDM_SEEDS", "42,7,2024").split(",")]
t0 = time.perf_counter()
rfm = segmentation.rfm_features(events).cache()
n_buyers = rfm.count()
x = segmentation.rfm_matrix(rfm).cache()
rows, stability = segmentation.compare_clusterers(x, ks=(3, 4, 5, 6), seeds=SEEDS)
cmp_df = pd.DataFrame(rows)
agg = (cmp_df.groupby(["algo", "k"]).agg(sil_mean=("silhouette", "mean"), sil_std=("silhouette", "std"),
                                          sil_min=("silhouette", "min"), degenerate_fits=("degenerate", "sum"), min_clusters_used=("clusters_used", "min"),
                                          fit_s=("seconds", "mean")).reset_index())
agg["ari_between_seeds"] = [stability[(a, k)] for a, k in zip(agg["algo"], agg["k"])]
ok = agg[agg["degenerate_fits"] == 0].sort_values("sil_mean", ascending=False).reset_index(drop=True)
best_algo, k = ok.loc[0, "algo"], int(ok.loc[0, "k"])
run_up = ok.loc[1] if len(ok) > 1 else None


def per_seed(algo, kk):
    return cmp_df[(cmp_df["algo"] == algo) & (cmp_df["k"] == kk)].set_index("seed")["silhouette"]


if run_up is not None:
    diff = per_seed(best_algo, k) - per_seed(run_up["algo"], int(run_up["k"]))
    vs_runner_up = {"runner_up": f"{run_up['algo']} k={int(run_up['k'])}", "diff_by_seed": diff.round(4).to_dict(),
                    "verdict": "better" if (diff > 0).all() else "tie"}
else:
    vs_runner_up = {}
best_by_algo = ok.groupby("algo").head(1).set_index("algo")
print(f"{n_buyers:,} buyers; selected {best_algo} k={k}; vs runner-up: {vs_runner_up}")
model = segmentation.fit_clusterer(best_algo, k, SEEDS[0], x)
seg = model.transform(x).withColumnRenamed("prediction", "segment").cache()
profile = segmentation.segment_profile(seg).toPandas(); t_seg = time.perf_counter() - t0
scores = {int(r.k): float(r.sil_mean) for r in agg[agg["algo"] == best_algo].itertuples()}
agg.sort_values("sil_mean", ascending=False)

# %%
profile.round(2)

# %%
sample = seg.select("recency", "frequency", "monetary", "segment").sample(False, min(1.0, 20000 / max(n_buyers, 1)), 1).toPandas()
fig, axes = plt.subplots(1, 3, figsize=(14, 3.8))
for ax, (x, y) in zip(axes, [("recency", "frequency"), ("recency", "monetary"), ("frequency", "monetary")]):
    ax.scatter(sample[x] + 1, sample[y] + 1, c=sample["segment"], s=4, cmap="tab10", alpha=.5)
    ax.set_xscale("log"); ax.set_yscale("log"); ax.set_xlabel(x); ax.set_ylabel(y)
fig.suptitle(f"RFM segments ({best_algo}, k={k})"); save_fig(fig, "07_rfm_segments"); plt.show()

# %% [markdown]
# ## 3. Demand by hour and weekday (purchases)

# %%
dem = (events.filter(F.col("event_type") == "purchase")
       .groupBy(F.hour("event_time").alias("hour"), F.dayofweek("event_date").alias("dow")).count().toPandas())
heat = dem.pivot(index="dow", columns="hour", values="count").fillna(0)
fig, ax = plt.subplots(figsize=(10, 3.5)); im = ax.imshow(heat.values, aspect="auto", cmap="viridis")
ax.set_xlabel("hour (UTC)"); ax.set_ylabel("day of week (1=Sun)"); ax.set_yticks(range(len(heat.index)), heat.index)
fig.colorbar(im); ax.set_title("Purchases by hour x weekday"); save_fig(fig, "07_demand_heatmap"); plt.show()

# %% [markdown]
# ## 4. Instacart reorder behaviour

# %%
ic = io.read_instacart(spark)
if FRACTION < 1.0:
    ic["prior"] = ic["prior"].filter((F.abs(F.hash("order_id")) % 10000) < int(FRACTION * 10000))
op = ic["prior"].join(ic["orders"].select("order_id", "order_dow", "order_hour_of_day"), "order_id")
reorder = op.agg(F.avg("reordered")).first()[0]
by_hour = op.groupBy("order_hour_of_day").agg(F.avg("reordered").alias("reorder_rate"), F.count("*").alias("lines")).orderBy("order_hour_of_day").toPandas()
top_re = (op.join(ic["products"].select("product_id", "product_name"), "product_id").groupBy("product_name")
          .agg(F.avg("reordered").alias("reorder_rate"), F.count("*").alias("n")).filter(F.col("n") > 500)
          .orderBy(F.desc("reorder_rate")).limit(10).toPandas())
print(f"overall reorder rate = {reorder:.3f}")
fig, ax = plt.subplots(figsize=(8, 3.3)); by_hour.plot(x="order_hour_of_day", y="reorder_rate", ax=ax, legend=False)
ax.set_ylabel("reorder rate"); ax.set_title("Instacart: reorder rate by hour of day"); save_fig(fig, "07_instacart_reorder"); plt.show()
top_re

# %%
out.update({"funnel_top": funnel.head(15).to_dict("records"), "rfm": {"n_buyers": n_buyers, "selected_algo": best_algo, "best_k": k, "silhouette": scores,
            "profile": profile.to_dict("records"), "seconds": t_seg, "seeds": SEEDS,
            "comparison": agg.to_dict("records"), "comparison_by_seed": rows, "vs_runner_up": vs_runner_up,
            "best_by_algo": best_by_algo.reset_index().to_dict("records"),
            "selection_rule": "highest mean silhouette over seeds among settings with no degenerate fit; better than "
                              "runner-up only if higher for every seed"},
            "instacart": {"reorder_rate": reorder, "top_reordered": top_re.to_dict("records")}})
print("saved", save_result("07_segmentation_funnel", out))
