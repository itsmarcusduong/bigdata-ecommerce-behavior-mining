"""Customer analytics: RFM features, K-Means segmentation and funnel statistics."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def rfm_features(events: DataFrame) -> DataFrame:
    """Recency (days), Frequency (#purchase sessions), Monetary (sum price) per buyer."""
    p = events.filter(F.col("event_type") == "purchase")
    ref = p.agg(F.max("event_date")).first()[0]
    return (p.groupBy("user_id")
            .agg(F.datediff(F.lit(ref), F.max("event_date")).alias("recency"),
                 F.countDistinct("user_session").alias("frequency"),
                 F.sum("price").alias("monetary")))


def kmeans_segments(rfm: DataFrame, ks=(3, 4, 5, 6), seed: int = 42):
    """Log-scaled RFM -> StandardScaler -> K-Means; picks k by silhouette. Returns (df, k, scores)."""
    from pyspark.ml import Pipeline
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.ml.feature import StandardScaler, VectorAssembler

    d = (rfm.withColumn("r", F.log1p("recency")).withColumn("f", F.log1p("frequency"))
         .withColumn("m", F.log1p("monetary")))
    prep = Pipeline(stages=[VectorAssembler(inputCols=["r", "f", "m"], outputCol="raw"),
                            StandardScaler(inputCol="raw", outputCol="features", withMean=True)]).fit(d)
    x = prep.transform(d).cache()
    scores, best = {}, (None, -2.0, None)
    for k in ks:
        pred = KMeans(k=k, seed=seed, featuresCol="features").fit(x).transform(x)
        s = ClusteringEvaluator().evaluate(pred)
        scores[k] = float(s)
        if s > best[1]:
            best = (k, s, pred)
    return best[2].withColumnRenamed("prediction", "segment"), best[0], scores


def segment_profile(segmented: DataFrame) -> DataFrame:
    return (segmented.groupBy("segment")
            .agg(F.count("*").alias("users"), F.avg("recency").alias("avg_recency"),
                 F.avg("frequency").alias("avg_frequency"), F.avg("monetary").alias("avg_monetary"))
            .orderBy("segment"))


def funnel_by_category(events: DataFrame, level: int = 0) -> DataFrame:
    """view -> cart -> purchase distinct-user funnel per top-level category."""
    cat = F.coalesce(F.split("category_code", r"\.").getItem(level), F.lit("unknown"))
    f = (events.withColumn("category", cat).groupBy("category", "event_type")
         .agg(F.countDistinct("user_id").alias("users")))
    p = f.groupBy("category").pivot("event_type", ["view", "cart", "purchase"]).sum("users").na.fill(0)
    return (p.withColumn("view_to_cart", F.col("cart") / F.col("view"))
            .withColumn("cart_to_purchase", F.col("purchase") / F.col("cart"))
            .withColumn("view_to_purchase", F.col("purchase") / F.col("view"))
            .orderBy(F.desc("view")))
