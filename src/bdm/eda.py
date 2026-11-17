"""Scalable exploratory statistics for the event log (all aggregations run in Spark)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def overview(events: DataFrame) -> dict:
    row = events.agg(
        F.count("*").alias("n_events"),
        F.approx_count_distinct("user_id").alias("n_users"),
        F.approx_count_distinct("product_id").alias("n_products"),
        F.approx_count_distinct("user_session").alias("n_sessions"),
        F.min("event_time").alias("t_min"), F.max("event_time").alias("t_max"),
        F.avg(F.col("category_code").isNull().cast("double")).alias("null_category_code"),
        F.avg(F.col("brand").isNull().cast("double")).alias("null_brand"),
        F.avg(F.col("user_session").isNull().cast("double")).alias("null_session"),
    ).first().asDict()
    row["event_types"] = {r["event_type"]: r["count"] for r in events.groupBy("event_type").count().collect()}
    row["price_quantiles_0_25_50_75_99"] = events.approxQuantile("price", [0.0, 0.25, 0.5, 0.75, 0.99], 0.01)
    return row


def daily_counts(events: DataFrame) -> DataFrame:
    return (events.groupBy("event_date").pivot("event_type").count().na.fill(0).orderBy("event_date"))


def top_k(events: DataFrame, col: str, k: int = 20, event_type: str | None = None) -> DataFrame:
    e = events if event_type is None else events.filter(F.col("event_type") == event_type)
    return e.groupBy(col).count().orderBy(F.desc("count")).limit(k)


def sessions_stats(events: DataFrame) -> DataFrame:
    s = events.filter(F.col("user_session").isNotNull()).groupBy("user_session").agg(
        F.count("*").alias("n_events"),
        (F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))).alias("duration_s"),
        F.max((F.col("event_type") == "purchase").cast("int")).alias("has_purchase"))
    return s.agg(F.avg("n_events").alias("avg_events_per_session"),
                 F.expr("percentile_approx(n_events, 0.5)").alias("median_events"),
                 F.expr("percentile_approx(duration_s, 0.5)").alias("median_duration_s"),
                 F.avg("has_purchase").alias("session_conversion"))


def product_meta(events: DataFrame) -> DataFrame:
    """One row per product: most common non-null category/brand and mean price."""
    return events.groupBy("product_id").agg(
        F.first("category_code", ignorenulls=True).alias("category_code"),
        F.first("brand", ignorenulls=True).alias("brand"),
        F.avg("price").alias("avg_price"))
