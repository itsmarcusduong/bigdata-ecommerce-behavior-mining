# %% [markdown]
# # 01 - Ingestion & EDA (REES46 e-commerce events, ~110M rows)
# **Goal.** Convert the two raw CSV files (2019-Oct 5.3 GB + 2019-Nov 8.4 GB) into a partitioned
# **Parquet** copy (columnar, compressed, predicate-pushdown friendly - the role HDFS/Parquet plays
# in a Hadoop stack) and profile the data at scale.
#
# **Kaggle inputs:** `mkechinov/ecommerce-behavior-data-from-multi-category-store`.
# **Output:** `/kaggle/working/events_parquet` -> save this notebook, then add its *output* as an
# input of notebooks 02-08 so they skip CSV parsing.

# %%
from bdm import config, eda, io, results
from bdm.results import Timer, save_fig, save_result

spark = config.get_spark("01-ingest-eda", driver_memory=SPARK_MEM)
timer = Timer()

# %% [markdown]
# ## 1. CSV -> Parquet
# Explicit schema (no `inferSchema` pass over 14 GB), malformed rows dropped, timestamps parsed.

# %%
raw = io.read_rees46_csv(spark, fraction=FRACTION)
out_dir = str(config.WORK_DIR / "events_parquet")
with timer("csv_to_parquet"):
    io.write_events_parquet(raw, out_dir)
events = spark.read.parquet(out_dir).cache()
n_events = events.count()
print(f"{n_events:,} events written to {out_dir} in {timer.times['csv_to_parquet']} s "
      f"({n_events / timer.times['csv_to_parquet']:,.0f} rows/s)")

# %% [markdown]
# ## 2. Dataset overview (volume, cardinality, missing values)

# %%
with timer("overview"):
    ov = eda.overview(events)
pd.Series({k: v for k, v in ov.items() if k != "event_types"})

# %%
pd.Series(ov["event_types"], name="events").to_frame()

# %% [markdown]
# ## 3. Temporal patterns (daily volume by event type)

# %%
daily = eda.daily_counts(events).toPandas().set_index("event_date")
fig, ax = plt.subplots(figsize=(11, 3.6))
daily.plot(ax=ax, logy=True)
ax.set_title("Events per day by type (log scale)"); ax.set_ylabel("events")
save_fig(fig, "01_daily_events"); plt.show()

# %% [markdown]
# ## 4. Categories, brands, sessions

# %%
top_cat = eda.top_k(events, "category_code", 15, "purchase").toPandas()
top_brand = eda.top_k(events, "brand", 15, "purchase").toPandas()
fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))
top_cat.plot.barh(x="category_code", y="count", ax=axes[0], legend=False).invert_yaxis()
axes[0].set_title("Top purchased categories")
top_brand.plot.barh(x="brand", y="count", ax=axes[1], legend=False, color="tab:orange").invert_yaxis()
axes[1].set_title("Top purchased brands")
save_fig(fig, "01_top_categories_brands"); plt.show()

# %%
sess = eda.sessions_stats(events).toPandas()
sess

# %% [markdown]
# ## 5. Data-quality notes to carry forward
# * `category_code` / `brand` have many nulls -> category analyses use `coalesce(..., 'unknown')`.
# * Sessions (`user_session`) are the natural *baskets* and *browsing paths* for later notebooks.
# * `purchase` is rare (a few % of events) -> recommender evaluation uses purchases as ground truth.

# %%
path = save_result("01_ingest_eda", {"fraction": FRACTION, "n_events": n_events, "timings_s": timer.times,
                                     "overview": ov, "session_stats": sess.to_dict("records")[0]})
print("saved", path)
