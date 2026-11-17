"""Data loading: REES46 e-commerce events (CSV -> Parquet) and Instacart baskets."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from . import config

REES46_SCHEMA = StructType([
    StructField("event_time", StringType()),
    StructField("event_type", StringType()),
    StructField("product_id", LongType()),
    StructField("category_id", LongType()),
    StructField("category_code", StringType()),
    StructField("brand", StringType()),
    StructField("price", DoubleType()),
    StructField("user_id", LongType()),
    StructField("user_session", StringType()),
])

REES46_FILES = ("2019-Oct.csv", "2019-Nov.csv")


def sample_by_user(df: DataFrame, fraction: float, col: str = "user_id") -> DataFrame:
    """Deterministic user-level sample (keeps each sampled user's full history/sessions)."""
    if fraction >= 1.0:
        return df
    buckets = 10_000
    return df.filter((F.abs(F.hash(col)) % buckets) < int(fraction * buckets))


def read_rees46_csv(spark: SparkSession, files=REES46_FILES, fraction: float = 1.0) -> DataFrame:
    paths = [config.find_file(f) for f in files]
    df = spark.read.csv(paths, header=True, schema=REES46_SCHEMA, mode="DROPMALFORMED")
    df = (
        df.withColumn("event_time", F.to_timestamp(F.substring("event_time", 1, 19)))
        .withColumn("event_date", F.to_date("event_time"))
        .filter(F.col("event_time").isNotNull() & F.col("user_id").isNotNull()
                & F.col("product_id").isNotNull())
    )
    return sample_by_user(df, fraction)


def write_events_parquet(df: DataFrame, out_dir: str) -> None:
    (df.repartition("event_date").write.mode("overwrite").partitionBy("event_date").parquet(out_dir))


def load_events(spark: SparkSession, fraction: float = 1.0) -> DataFrame:
    """Prefer the Parquet copy produced by notebook 01; fall back to the raw CSV files."""
    pq = config.find_dir("events_parquet")
    if pq:
        return sample_by_user(spark.read.parquet(pq), fraction)
    return read_rees46_csv(spark, fraction=fraction)


def read_instacart(spark: SparkSession) -> dict[str, DataFrame]:
    """Load the Instacart relational tables (orders, prior, train, products, aisles, departments)."""
    def rd(name: str, int_cols: list[str]) -> DataFrame:
        df = spark.read.csv(config.find_file(name), header=True)
        for c in int_cols:
            df = df.withColumn(c, F.col(c).cast("int"))
        return df

    return {
        "orders": rd("orders.csv", ["order_id", "user_id", "order_number", "order_dow",
                                    "order_hour_of_day"]).withColumn(
            "days_since_prior_order", F.col("days_since_prior_order").cast("double")),
        "prior": rd("order_products__prior.csv", ["order_id", "product_id", "add_to_cart_order",
                                                   "reordered"]),
        "train": rd("order_products__train.csv", ["order_id", "product_id", "add_to_cart_order",
                                                   "reordered"]),
        "products": rd("products.csv", ["product_id", "aisle_id", "department_id"]),
        "aisles": rd("aisles.csv", ["aisle_id"]),
        "departments": rd("departments.csv", ["department_id"]),
    }
