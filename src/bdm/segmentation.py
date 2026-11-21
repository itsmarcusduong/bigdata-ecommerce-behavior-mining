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


def rfm_matrix(rfm: DataFrame) -> DataFrame:
    """Log-scaled, standardised RFM feature vectors (column ``features``), one row per buyer."""
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StandardScaler, VectorAssembler

    d = (rfm.withColumn("r", F.log1p("recency")).withColumn("f", F.log1p("frequency"))
         .withColumn("m", F.log1p("monetary")))
    prep = Pipeline(stages=[VectorAssembler(inputCols=["r", "f", "m"], outputCol="raw"),
                            StandardScaler(inputCol="raw", outputCol="features", withMean=True)]).fit(d)
    return prep.transform(d)


def make_clusterer(algo: str, k: int, seed: int):
    """'kmeans' (k-means||), 'bisecting' (divisive k-means) or 'gmm' (Gaussian mixture, hard assignment)."""
    from pyspark.ml.clustering import BisectingKMeans, GaussianMixture, KMeans

    if algo == "kmeans":
        return KMeans(k=k, seed=seed, featuresCol="features")
    if algo == "bisecting":
        return BisectingKMeans(k=k, seed=seed, featuresCol="features")
    if algo == "gmm":
        return GaussianMixture(k=k, seed=seed, featuresCol="features", maxIter=100)
    raise ValueError(algo)


def fit_clusterer(algo: str, k: int, seed: int, x: DataFrame):
    """Fit one clusterer. Callers must check how many clusters the fit actually uses: Spark 3.5's Bisecting K-Means
    can return fewer non-empty clusters than ``k`` (we saw [n, 0, 0] on RFM data), which inflates the silhouette."""
    return make_clusterer(algo, k, seed).fit(x)


def adjusted_rand_index(a, b) -> float:
    """Adjusted Rand index of two labelings (1 = identical partitions, ~0 = chance agreement)."""
    import numpy as np

    a, b = np.asarray(a), np.asarray(b)
    _, ai = np.unique(a, return_inverse=True)
    _, bi = np.unique(b, return_inverse=True)
    table = np.zeros((ai.max() + 1, bi.max() + 1), dtype=np.int64)
    np.add.at(table, (ai, bi), 1)

    def comb2(x):
        return (x * (x - 1) / 2.0).sum()

    n = len(a)
    s_ij, s_a, s_b = comb2(table), comb2(table.sum(1)), comb2(table.sum(0))
    expected = s_a * s_b / (n * (n - 1) / 2.0) if n > 1 else 0.0
    max_index = (s_a + s_b) / 2.0
    return 1.0 if max_index == expected else float((s_ij - expected) / (max_index - expected))


def compare_clusterers(x: DataFrame, algos=("kmeans", "bisecting", "gmm"), ks=(3, 4, 5, 6),
                       seeds=(42, 7, 2024), sample_size: int = 50_000):
    """Fit every (algo, k, seed); silhouette on all rows, seed stability (mean pairwise ARI) on a fixed sample.

    Returns (rows, stability) where rows = [{algo, k, seed, silhouette, clusters_used, degenerate, seconds}] and
    stability = {(algo, k): mean ARI between seeds}.
    """
    import itertools
    import time

    from pyspark.ml.evaluation import ClusteringEvaluator

    n = x.count()
    sample = x.orderBy(F.hash("user_id")).limit(min(sample_size, n)).cache()
    rows, labels = [], {}
    for algo in algos:
        for k in ks:
            for sd in seeds:
                t0 = time.perf_counter()
                model = fit_clusterer(algo, k, sd, x)
                pred = model.transform(x)
                n_used = pred.select("prediction").distinct().count()
                # a degenerate fit (fewer non-empty clusters than k) is flagged; its silhouette is not comparable
                sil = ClusteringEvaluator().evaluate(pred) if n_used > 1 else float("nan")
                rows.append({"algo": algo, "k": k, "seed": sd, "silhouette": float(sil),
                             "clusters_used": n_used, "degenerate": n_used < k,
                             "seconds": round(time.perf_counter() - t0, 1)})
                labels[(algo, k, sd)] = [r[0] for r in model.transform(sample).orderBy(F.hash("user_id"))
                                         .select("prediction").collect()]
    stability = {}
    for algo in algos:
        for k in ks:
            pairs = list(itertools.combinations(seeds, 2))
            stability[(algo, k)] = (sum(adjusted_rand_index(labels[(algo, k, a)], labels[(algo, k, b)])
                                        for a, b in pairs) / len(pairs)) if pairs else 1.0
    return rows, stability


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
