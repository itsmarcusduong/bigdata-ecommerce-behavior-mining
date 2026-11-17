"""Link analysis on the product graph (CS246): PageRank, topic-sensitive PageRank and HITS,
implemented with iterative Spark DataFrame joins (no GraphX/GraphFrames dependency)."""

from __future__ import annotations

from itertools import combinations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def transition_edges(events: DataFrame, min_weight: int = 2) -> DataFrame:
    """Directed navigation graph: edge a->b when a user views b right after a in a session."""
    w = Window.partitionBy("user_session").orderBy("event_time")
    v = (events.filter((F.col("event_type") == "view") & F.col("user_session").isNotNull())
         .select("user_session", "event_time", F.col("product_id").alias("src"),
                 F.lead("product_id").over(w).alias("dst")))
    e = v.filter(F.col("dst").isNotNull() & (F.col("src") != F.col("dst")))
    return (e.groupBy("src", "dst").agg(F.count("*").cast("double").alias("weight"))
            .filter(F.col("weight") >= min_weight))


def cooccurrence_edges(spark: SparkSession, baskets: DataFrame, items_col: str = "items",
                       max_items: int = 30, min_weight: int = 2) -> DataFrame:
    """Symmetric co-purchase graph from baskets (both directions, weight = #baskets)."""
    def emit(row):
        items = sorted(set(row[0]))[:max_items]
        for a, b in combinations(items, 2):
            yield (a, b), 1
            yield (b, a), 1

    rdd = baskets.select(items_col).rdd.flatMap(emit).reduceByKey(lambda x, y: x + y)
    df = spark.createDataFrame(rdd.map(lambda kv: (kv[0][0], kv[0][1], float(kv[1]))),
                               ["src", "dst", "weight"])
    return df.filter(F.col("weight") >= min_weight)


def pagerank(edges: DataFrame, beta: float = 0.85, max_iter: int = 50, tol: float = 1e-8,
             personalization: DataFrame | None = None):
    """Weighted PageRank by power iteration with dead-end handling.

    ``personalization``: optional DataFrame(node, p) (topic-sensitive / personalized PageRank);
    teleport and dangling mass both follow it (same convention as networkx).
    Returns ``(ranks_df[node, rank], deltas)``.
    """
    nodes = edges.select(F.col("src").alias("node")).union(edges.select(F.col("dst").alias("node"))).distinct()
    n = nodes.count()
    if personalization is None:
        tele = nodes.withColumn("t", F.lit(1.0 / n))
    else:
        tot = personalization.agg(F.sum("p")).first()[0]
        tele = nodes.join(personalization.select(F.col("node"), (F.col("p") / tot).alias("t")),
                          "node", "left").na.fill(0.0, ["t"])
    out_w = edges.groupBy("src").agg(F.sum("weight").alias("out_w"))
    trans = (edges.join(out_w, "src").select("src", "dst", (F.col("weight") / F.col("out_w")).alias("p"))
             .cache())
    dangling_nodes = nodes.join(out_w.select(F.col("src").alias("node")), "node", "left_anti").cache()
    tele = tele.cache()
    ranks = tele.select("node", F.col("t").alias("rank")).localCheckpoint()
    deltas = []
    for _ in range(max_iter):
        contrib = (trans.join(ranks, trans.src == ranks.node)
                   .groupBy(F.col("dst").alias("node")).agg(F.sum(F.col("p") * F.col("rank")).alias("c")))
        d_mass = dangling_nodes.join(ranks, "node").agg(F.coalesce(F.sum("rank"), F.lit(0.0))).first()[0]
        new = (tele.join(contrib, "node", "left").na.fill(0.0, ["c"])
               .select("node", (F.lit(beta) * (F.col("c") + F.lit(d_mass) * F.col("t"))
                                + F.lit(1 - beta) * F.col("t")).alias("rank")))
        new = new.localCheckpoint()
        delta = (new.join(ranks.withColumnRenamed("rank", "old"), "node")
                 .agg(F.sum(F.abs(F.col("rank") - F.col("old")))).first()[0])
        ranks = new
        deltas.append(delta)
        if delta < tol:
            break
    return ranks, deltas


def hits(edges: DataFrame, max_iter: int = 50, tol: float = 1e-8):
    """Kleinberg HITS (hubs/authorities) with L2 normalisation; returns DataFrame(node, hub, auth)."""
    e = edges.select("src", "dst", "weight").cache()
    nodes = e.select(F.col("src").alias("node")).union(e.select(F.col("dst").alias("node"))).distinct()
    hub = nodes.withColumn("hub", F.lit(1.0))
    auth = nodes.withColumn("auth", F.lit(1.0))
    for _ in range(max_iter):
        a_raw = (e.join(hub, e.src == hub.node).groupBy(F.col("dst").alias("node"))
                 .agg(F.sum(F.col("weight") * F.col("hub")).alias("auth")))
        a_new = nodes.join(a_raw, "node", "left").na.fill(0.0, ["auth"])
        norm = a_new.agg(F.sqrt(F.sum(F.col("auth") ** 2))).first()[0] or 1.0
        a_new = a_new.select("node", (F.col("auth") / norm).alias("auth")).localCheckpoint()
        h_raw = (e.join(a_new, e.dst == a_new.node).groupBy(F.col("src").alias("node"))
                 .agg(F.sum(F.col("weight") * F.col("auth")).alias("hub")))
        h_new = nodes.join(h_raw, "node", "left").na.fill(0.0, ["hub"])
        norm = h_new.agg(F.sqrt(F.sum(F.col("hub") ** 2))).first()[0] or 1.0
        h_new = h_new.select("node", (F.col("hub") / norm).alias("hub")).localCheckpoint()
        delta = (a_new.join(auth.withColumnRenamed("auth", "old"), "node")
                 .agg(F.sum(F.abs(F.col("auth") - F.col("old")))).first()[0])
        hub, auth = h_new, a_new
        if delta < tol:
            break
    return hub.join(auth, "node")
