# %% [markdown]
# # 07 - Customer behaviour: RFM segmentation, funnel and reorder patterns
# * **REES46**: conversion funnel per category, RFM features per buyer -> **K-Means** (k chosen by silhouette),
#   segment profiles, hourly/weekday demand.
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
t0 = time.perf_counter()
rfm = segmentation.rfm_features(events).cache()
n_buyers = rfm.count()
seg, k, scores = segmentation.kmeans_segments(rfm, ks=(3, 4, 5, 6))
seg.cache(); profile = segmentation.segment_profile(seg).toPandas(); t_seg = time.perf_counter() - t0
print(f"{n_buyers:,} buyers; best k={k}; silhouette per k: { {kk: round(v, 3) for kk, v in scores.items()} }; {t_seg:.1f}s")
profile.round(2)

# %%
sample = seg.select("recency", "frequency", "monetary", "segment").sample(False, min(1.0, 20000 / max(n_buyers, 1)), 1).toPandas()
fig, axes = plt.subplots(1, 3, figsize=(14, 3.8))
for ax, (x, y) in zip(axes, [("recency", "frequency"), ("recency", "monetary"), ("frequency", "monetary")]):
    ax.scatter(sample[x] + 1, sample[y] + 1, c=sample["segment"], s=4, cmap="tab10", alpha=.5)
    ax.set_xscale("log"); ax.set_yscale("log"); ax.set_xlabel(x); ax.set_ylabel(y)
fig.suptitle(f"RFM segments (k={k})"); save_fig(fig, "07_rfm_segments"); plt.show()

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
out.update({"funnel_top": funnel.head(15).to_dict("records"), "rfm": {"n_buyers": n_buyers, "best_k": k, "silhouette": scores,
            "profile": profile.to_dict("records"), "seconds": t_seg},
            "instacart": {"reorder_rate": reorder, "top_reordered": top_re.to_dict("records")}})
print("saved", save_result("07_segmentation_funnel", out))
