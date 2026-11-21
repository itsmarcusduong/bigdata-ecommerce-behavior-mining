"""Recommender systems: ALS (implicit), popularity and item-kNN baselines, ranking metrics."""

from __future__ import annotations

import math

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

INT_MAX = 2**31 - 1


# ---------------------------------------------------------------- ranking metrics (pure python)
def precision_at_k(recs, truth, k: int) -> float:
    return len(set(recs[:k]) & set(truth)) / k


def recall_at_k(recs, truth, k: int) -> float:
    return len(set(recs[:k]) & set(truth)) / len(truth) if truth else 0.0


def hit_rate_at_k(recs, truth, k: int) -> float:
    return 1.0 if set(recs[:k]) & set(truth) else 0.0


def ap_at_k(recs, truth, k: int) -> float:
    truth = set(truth)
    hits, score = 0, 0.0
    for i, r in enumerate(recs[:k], start=1):
        if r in truth:
            hits += 1
            score += hits / i
    return score / min(len(truth), k) if truth else 0.0


def ndcg_at_k(recs, truth, k: int) -> float:
    truth = set(truth)
    dcg = sum(1.0 / math.log2(i + 2) for i, r in enumerate(recs[:k]) if r in truth)
    idcg = sum(1.0 / math.log2(i + 2) for i in range(min(len(truth), k)))
    return dcg / idcg if idcg else 0.0


METRICS = {"precision": precision_at_k, "recall": recall_at_k, "hit_rate": hit_rate_at_k,
           "map": ap_at_k, "ndcg": ndcg_at_k}


def evaluate(recs_df: DataFrame, truth_df: DataFrame, seen_df: DataFrame | None = None,
             ks=(10, 20), max_users: int | None = 50_000, users: DataFrame | None = None,
             per_user: str | None = None) -> dict:
    """Average ranking metrics.

    ``recs_df``: (user_id, recs array<long>) best-first, long enough (e.g. 100) to survive
    filtering; ``truth_df``: (user_id, truth array<long>); ``seen_df``: optional
    (user_id, seen array<long>) whose items are removed from recs before scoring.
    ``users``: optional (user_id) frame restricting the evaluation, so several models are scored on the
    **same** users. ``per_user``: metric name (e.g. ``"ndcg@10"``) whose per-user values are returned under
    ``"_per_user"`` as {user_id: value}, for paired bootstrap tests.
    """
    df = recs_df.join(truth_df, "user_id")
    if users is not None:
        df = df.join(users.select("user_id").distinct(), "user_id")
    if seen_df is not None:
        df = df.join(seen_df, "user_id", "left")
    else:
        df = df.withColumn("seen", F.array().cast("array<long>"))
    if max_users:
        df = df.orderBy(F.hash("user_id")).limit(max_users)  # reproducible subset, not arbitrary rows
    rows = df.select("user_id", "recs", "truth", "seen").collect()
    acc = {f"{m}@{k}": [] for m in METRICS for k in ks}
    for _, recs, truth, seen in rows:
        seen_s = set(seen or [])
        recs = [r for r in recs if r not in seen_s]
        for k in ks:
            for m, fn in METRICS.items():
                acc[f"{m}@{k}"].append(fn(recs, truth, k))
    out = {key: float(pd.Series(v).mean()) if v else 0.0 for key, v in acc.items()}
    out["n_users"] = len(rows)
    if per_user:
        out["_per_user"] = {int(r[0]): v for r, v in zip(rows, acc[per_user])}
    return out


def with_fallback(recs_df: DataFrame, fallback_df: DataFrame) -> DataFrame:
    """Users missing from ``recs_df`` (cold start) get the ``fallback_df`` list (e.g. popularity)."""
    fb = fallback_df.withColumnRenamed("recs", "fb")
    return (fb.join(recs_df, "user_id", "left")
            .select("user_id", F.coalesce("recs", "fb").alias("recs")))


def paired_bootstrap(a: dict, b: dict, n_boot: int = 2000, seed: int = 0) -> dict:
    """Paired bootstrap over users of mean(a) - mean(b) for per-user metric dicts {user: value}.

    Uses the users present in both. Returns the observed difference, a 95% percentile CI and the
    fraction of resamples where the difference is <= 0 (a one-sided p-value for "a > b").
    """
    import numpy as np
    common = sorted(set(a) & set(b))
    if not common:
        return {"n_users": 0}
    d = np.array([a[u] - b[u] for u in common])
    rng = np.random.default_rng(seed)
    boots = d[rng.integers(0, len(d), size=(n_boot, len(d)))].mean(axis=1)
    lo, hi = np.percentile(boots, [2.5, 97.5])
    return {"n_users": len(d), "diff": float(d.mean()), "ci95": [float(lo), float(hi)],
            "p_le_0": float((boots <= 0).mean())}


# ---------------------------------------------------------------- models
def check_int_ids(df: DataFrame, cols=("user_id", "product_id")) -> None:
    mx = df.agg(*[F.max(c).alias(c) for c in cols]).first()
    for c in cols:
        if mx[c] is not None and mx[c] > INT_MAX:
            raise ValueError(f"{c} exceeds int32; re-index before ALS")


def train_als(ui: DataFrame, rank: int = 32, reg: float = 0.05, alpha: float = 20.0,
              max_iter: int = 8, seed: int = 42):
    from pyspark.ml.recommendation import ALS

    check_int_ids(ui)
    data = ui.select(F.col("user_id").cast("int").alias("u"), F.col("product_id").cast("int").alias("i"),
                     F.col("rating").cast("float").alias("r"))
    return ALS(userCol="u", itemCol="i", ratingCol="r", implicitPrefs=True, rank=rank, regParam=reg,
               alpha=alpha, maxIter=max_iter, seed=seed, coldStartStrategy="drop",
               checkpointInterval=2).fit(data)  # truncate lineage: bounded shuffle files on disk


def als_recs(model, users: DataFrame, n: int = 100) -> DataFrame:
    u = users.select(F.col("user_id").cast("int").alias("u")).distinct()
    r = model.recommendForUserSubset(u, n)
    return r.select(F.col("u").cast("long").alias("user_id"),
                    F.expr("transform(recommendations, x -> cast(x.i as long))").alias("recs"))


def popularity_recs(train_purchases: DataFrame, users: DataFrame, n: int = 100) -> DataFrame:
    top = [r[0] for r in train_purchases.groupBy("product_id").count()
           .orderBy(F.desc("count")).limit(n).select("product_id").collect()]
    return users.select("user_id").distinct().withColumn("recs", F.array(*[F.lit(int(x)) for x in top]))


def item_neighbors(edges: DataFrame, top_n: int = 50) -> DataFrame:
    """Top similar items per item from co-occurrence edges, cosine-normalised by degree."""
    deg = edges.groupBy("src").agg(F.sum("weight").alias("deg"))
    e = (edges.join(deg.withColumnRenamed("deg", "d_src"), "src")
         .join(deg.withColumnRenamed("src", "dst").withColumnRenamed("deg", "d_dst"), "dst")
         .select("src", "dst", (F.col("weight") / F.sqrt(F.col("d_src") * F.col("d_dst"))).alias("sim")))
    from pyspark.sql.window import Window
    w = Window.partitionBy("src").orderBy(F.desc("sim"))
    return e.withColumn("rk", F.row_number().over(w)).filter(F.col("rk") <= top_n).drop("rk")


def itemknn_recs(history: DataFrame, neighbors: DataFrame, n: int = 100) -> DataFrame:
    """history: (user_id, product_id); score(u, j) = sum_i sim(i, j) over the user's history."""
    sc = (history.select("user_id", F.col("product_id").alias("src")).distinct()
          .join(neighbors, "src").groupBy("user_id", "dst").agg(F.sum("sim").alias("score")))
    from pyspark.sql.window import Window
    w = Window.partitionBy("user_id").orderBy(F.desc("score"))
    top = sc.withColumn("rk", F.row_number().over(w)).filter(F.col("rk") <= n)
    return (top.groupBy("user_id").agg(F.sort_array(F.collect_list(F.struct("rk", "dst"))).alias("s"))
            .select("user_id", F.expr("transform(s, x -> x.dst)").alias("recs")))


def truth_and_seen(train: DataFrame, test: DataFrame, event_type: str = "purchase"):
    """Ground truth = items bought in test by users known in train (and items known in train)."""
    known_items = train.select("product_id").distinct()
    known_users = train.select("user_id").distinct()
    truth = (test.filter(F.col("event_type") == event_type).join(known_users, "user_id")
             .join(known_items, "product_id").groupBy("user_id")
             .agg(F.collect_set("product_id").alias("truth")))
    seen = (train.filter(F.col("event_type") == event_type).groupBy("user_id")
            .agg(F.collect_set("product_id").alias("seen")))
    return truth, seen


# ---------------------------------------------------------------- more candidates (model comparison)
def recent_popularity_recs(train: DataFrame, users: DataFrame, ref_date: str, days: int = 7,
                           n: int = 100) -> DataFrame:
    """Top-selling products in the last ``days`` before ``ref_date`` (train data only)."""
    recent = train.filter((F.col("event_type") == "purchase")
                          & (F.col("event_date") >= F.date_sub(F.lit(ref_date).cast("date"), days)))
    return popularity_recs(recent, users, n)


def history_recs(train: DataFrame, users: DataFrame, ref_date: str, half_life_days: float = 3.0,
                 n: int = 100) -> DataFrame:
    """Re-recommend the user's own viewed/carted products, scored by event weight x exponential recency decay.

    score(u, i) = sum over events of weight(type) * 2^(-age_days / half_life). A strong baseline in e-commerce,
    where many purchases follow earlier views. Already-bought items are removed later by ``evaluate``.
    """
    from pyspark.sql.window import Window

    from bdm.sessions import with_weight

    age = F.datediff(F.lit(ref_date).cast("date"), F.col("event_date"))
    sc = (with_weight(train.join(users.select("user_id").distinct(), "user_id"))
          .filter(F.col("weight") > 0)
          .withColumn("s", F.col("weight") * F.pow(F.lit(2.0), -age / F.lit(float(half_life_days))))
          .groupBy("user_id", "product_id").agg(F.sum("s").alias("score")))
    w = Window.partitionBy("user_id").orderBy(F.desc("score"), "product_id")
    top = sc.withColumn("rk", F.row_number().over(w)).filter(F.col("rk") <= n)
    return (top.groupBy("user_id").agg(F.sort_array(F.collect_list(F.struct("rk", "product_id"))).alias("s"))
            .select("user_id", F.expr("transform(s, x -> x.product_id)").alias("recs")))


def blend_recs(first: DataFrame, second: DataFrame, n: int = 100) -> DataFrame:
    """Concatenate two ranked lists per user (``first`` before ``second``), drop duplicates, keep ``n``."""
    a = first.withColumnRenamed("recs", "ra")
    b = second.withColumnRenamed("recs", "rb")
    empty = F.array().cast("array<long>")
    return (a.join(b, "user_id", "full")
            .select("user_id", F.slice(F.array_distinct(F.concat(F.coalesce("ra", empty), F.coalesce("rb", empty))),
                                       1, n).alias("recs")))


def select_best(val_scores: dict, metric: str = "ndcg@10") -> str:
    """Pre-registered selection rule: the candidate with the highest validation metric (ties -> name order)."""
    return sorted(val_scores, key=lambda k: (-val_scores[k][metric], k))[0]


def verdict(boot: dict) -> str:
    """'better' / 'worse' only when the paired-bootstrap 95% CI excludes 0, else 'tie'."""
    if not boot or "ci95" not in boot:
        return "n/a"
    lo, hi = boot["ci95"]
    return "better" if lo > 0 else "worse" if hi < 0 else "tie"
