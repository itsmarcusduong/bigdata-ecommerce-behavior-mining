"""Feature building on top of the raw event log: weights, user-item matrix, baskets."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EVENT_WEIGHTS = {"view": 1.0, "cart": 3.0, "purchase": 5.0}


def with_weight(events: DataFrame) -> DataFrame:
    w = F.lit(0.0)
    for etype, val in EVENT_WEIGHTS.items():
        w = F.when(F.col("event_type") == etype, F.lit(val)).otherwise(w)
    return events.withColumn("weight", w)


def user_item_matrix(events: DataFrame, min_user_items: int = 3, min_item_users: int = 5) -> DataFrame:
    """Implicit-feedback matrix ``(user_id, product_id, rating)`` with rating = log1p(sum weight)."""
    ui = (with_weight(events).filter(F.col("weight") > 0)
          .groupBy("user_id", "product_id").agg(F.sum("weight").alias("w")))
    ui = ui.withColumn("rating", F.log1p("w")).drop("w")
    users = ui.groupBy("user_id").count().filter(F.col("count") >= min_user_items).select("user_id")
    items = ui.groupBy("product_id").count().filter(F.col("count") >= min_item_users).select("product_id")
    return ui.join(users, "user_id").join(items, "product_id")


def session_baskets(events: DataFrame, event_types=("cart", "purchase"), min_items: int = 2,
                    max_items: int = 50) -> DataFrame:
    """One basket per ``user_session`` = distinct products with the given event types."""
    b = (events.filter(F.col("event_type").isin(*event_types) & F.col("user_session").isNotNull())
         .groupBy("user_session").agg(F.collect_set("product_id").alias("items")))
    return b.filter(F.size("items").between(min_items, max_items))


def instacart_baskets(prior: DataFrame, min_items: int = 2, max_items: int = 50) -> DataFrame:
    b = prior.groupBy("order_id").agg(F.collect_set("product_id").alias("items"))
    return b.filter(F.size("items").between(min_items, max_items))


def time_split(events: DataFrame, cutoff: str) -> tuple[DataFrame, DataFrame]:
    """Temporal split: train = events strictly before ``cutoff`` (YYYY-MM-DD), test = on/after."""
    return (events.filter(F.col("event_date") < F.lit(cutoff).cast("date")),
            events.filter(F.col("event_date") >= F.lit(cutoff).cast("date")))
