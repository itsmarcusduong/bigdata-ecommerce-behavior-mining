"""Shingling/MinHash/LSH for set similarity (CS246 similar-items), in NumPy + Spark RDDs.

Items are product ids; a "document" is the set of products a user touched (user similarity)
or the set of users who touched a product (product similarity).
"""

from __future__ import annotations

import zlib
from itertools import combinations

import numpy as np

from .sketches import P, _hash_params


def jaccard(a, b) -> float:
    a, b = set(a), set(b)
    return len(a & b) / len(a | b) if (a or b) else 0.0


def s_curve(s: float, bands: int, rows: int) -> float:
    """P[pair with Jaccard s becomes a candidate] = 1-(1-s^r)^b."""
    return 1.0 - (1.0 - s**rows) ** bands


def best_bands_rows(n_hashes: int, threshold: float, min_prob: float | None = None) -> tuple[int, int]:
    """Pick (b, r) with b*r <= n_hashes.

    Default: the S-curve inflection point (1/b)^(1/r) is closest to ``threshold``.
    With ``min_prob``: the most selective r such that a pair at exactly ``threshold`` becomes a
    candidate with probability >= min_prob (design for recall; false positives are removed later
    by exact verification).
    """
    if min_prob is not None:
        for r in range(n_hashes, 0, -1):
            b = n_hashes // r
            if b >= 1 and s_curve(threshold, b, r) >= min_prob:
                return b, r
        return n_hashes, 1
    best, best_err = (1, n_hashes), float("inf")
    for r in range(1, n_hashes + 1):
        b = n_hashes // r
        if b < 1:
            continue
        err = abs((1.0 / b) ** (1.0 / r) - threshold)
        if err < best_err:
            best, best_err = (b, r), err
    return best


class MinHasher:
    def __init__(self, n_hashes: int = 128, seed: int = 0) -> None:
        self.n, self.seed = n_hashes, seed
        self.a, self.b = _hash_params(n_hashes, seed)

    def signature(self, items) -> np.ndarray:
        x = np.unique(np.asarray(list(items), dtype=np.int64)) % P
        return ((self.a[:, None] * x[None, :] + self.b[:, None]) % P).min(axis=1)

    @staticmethod
    def estimate(sig_a: np.ndarray, sig_b: np.ndarray) -> float:
        return float((sig_a == sig_b).mean())


def band_keys(sig: np.ndarray, bands: int, rows: int) -> list[tuple[int, int]]:
    """Deterministic (crc32) bucket keys; Python's hash() is salted per process, so avoid it."""
    return [(b, zlib.crc32(sig[b * rows:(b + 1) * rows].tobytes())) for b in range(bands)]


def lsh_similar_pairs(sets_rdd, threshold: float = 0.5, n_hashes: int = 128, seed: int = 0,
                      max_bucket: int = 1000, bands_rows: tuple[int, int] | None = None,
                      min_prob: float = 0.9):
    """Own MinHash+banding LSH on an RDD of ``(id, iterable[int])``.

    Returns ``(pairs_rdd, stats)`` where ``pairs_rdd`` holds ``((a, b), jaccard)`` with a < b and
    jaccard >= threshold (candidates are verified against the exact sets).
    ``min_prob`` is the S-curve probability at the threshold used to choose (bands, rows).
    """
    bands, rows = bands_rows or best_bands_rows(n_hashes, threshold, min_prob)
    hasher = MinHasher(bands * rows, seed)
    sets = sets_rdd.mapValues(lambda s: sorted(set(s))).cache()

    def emit(rec):
        rid, items = rec
        if not items:
            return []
        return [(k, rid) for k in band_keys(hasher.signature(items), bands, rows)]

    buckets = sets.flatMap(emit).groupByKey().mapValues(lambda v: sorted(set(v)))
    buckets = buckets.filter(lambda kv: 1 < len(kv[1]) <= max_bucket)
    cand = buckets.flatMap(lambda kv: combinations(kv[1], 2)).distinct()
    # verify candidates against the exact sets: join on the first id, then on the second id
    by_a =cand.map(lambda p: (p[0], p[1])).join(sets).map(lambda kv: (kv[1][0], (kv[0], kv[1][1])))
    joined = by_a.join(sets).map(lambda kv: ((kv[1][0][0], kv[0]), jaccard(kv[1][0][1], kv[1][1])))
    result = joined.filter(lambda kv: kv[1] >= threshold).cache()
    return result, {"bands": bands, "rows": rows, "n_hashes": bands * rows}


def exact_similar_pairs(sets: dict, threshold: float) -> dict[tuple, float]:
    """Brute-force O(n^2) baseline on a (small) dict ``id -> set``."""
    out = {}
    ids = sorted(sets)
    for i, a in enumerate(ids):
        for b in ids[i + 1:]:
            j = jaccard(sets[a], sets[b])
            if j >= threshold:
                out[(a, b)] = j
    return out


def pair_recall_precision(found: set, truth: set) -> tuple[float, float]:
    tp = len(found & truth)
    return (tp / len(truth) if truth else 1.0), (tp / len(found) if found else 1.0)


def wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval for a proportion k/n (well-behaved at 0 and 1, unlike the normal interval)."""
    if n == 0:
        return (0.0, 1.0)
    p = k / n
    den = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / den
    half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / den
    return (max(0.0, centre - half), min(1.0, centre + half))


def mllib_minhash_pairs(df, id_col: str, items_col: str, jaccard_threshold: float,
                        num_tables: int = 8, seed: int = 0):
    """Spark MLlib ``MinHashLSH`` self-join; returns DataFrame (id_a, id_b, jaccard)."""
    from pyspark.ml.feature import MinHashLSH
    from pyspark.ml.linalg import Vectors, VectorUDT
    from pyspark.sql import functions as F

    dim = int(df.select(F.max(F.array_max(items_col))).first()[0]) + 1

    def to_vec(xs):
        idx = sorted(set(int(x) for x in xs))
        return Vectors.sparse(dim, idx, [1.0] * len(idx))

    feat = df.select(id_col, F.udf(to_vec, VectorUDT())(items_col).alias("features"))
    model = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=num_tables,
                       seed=seed).fit(feat)
    joined = model.approxSimilarityJoin(feat, feat, 1.0 - jaccard_threshold, distCol="dist")
    return joined.filter(F.col(f"datasetA.{id_col}") < F.col(f"datasetB.{id_col}")).select(
        F.col(f"datasetA.{id_col}").alias("id_a"), F.col(f"datasetB.{id_col}").alias("id_b"),
        (1.0 - F.col("dist")).alias("jaccard"))
