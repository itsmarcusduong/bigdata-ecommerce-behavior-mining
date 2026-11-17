"""Frequent itemsets (CS246): own A-Priori / PCY pair counting on RDDs vs Spark MLlib FP-Growth."""

from __future__ import annotations

from collections import Counter
from itertools import combinations

BUCKET_MULT = 1_000_003


def _pair_bucket(a: int, b: int, n_buckets: int) -> int:
    return (a * BUCKET_MULT + b) % n_buckets


def _clean(basket, max_basket: int):
    items = sorted(set(basket))
    return items if 2 <= len(items) <= max_basket else None


def count_pairs(baskets_rdd, min_support: int, n_buckets: int | None = None,
                max_basket: int = 50):
    """Frequent pairs with A-Priori (``n_buckets=None``) or PCY (hash-bucket filter in pass 1).

    ``baskets_rdd``: RDD of iterables of ints.  ``min_support``: absolute count.
    Returns ``(pairs_rdd[((a, b), count)], stats)``; stats expose the candidate-pair counts so the
    memory advantage of PCY over plain A-Priori can be reported (ablation).
    """
    baskets = baskets_rdd.map(lambda b: _clean(b, max_basket)).filter(lambda b: b is not None).cache()

    # ---- pass 1: item counts (+ bucket counts for PCY)
    item_counts = baskets.flatMap(lambda b: ((i, 1) for i in b)).reduceByKey(lambda x, y: x + y)
    freq_items = set(item_counts.filter(lambda kv: kv[1] >= min_support).keys().collect())
    sc = baskets_rdd.context
    fi_bc = sc.broadcast(freq_items)

    bitmap_bc = None
    n_freq_buckets = None
    if n_buckets:
        bucket_counts = baskets.flatMap(
            lambda b: ((_pair_bucket(x, y, n_buckets), 1) for x, y in combinations(b, 2))
        ).reduceByKey(lambda x, y: x + y)
        freq_b = set(bucket_counts.filter(lambda kv: kv[1] >= min_support).keys().collect())
        n_freq_buckets = len(freq_b)
        bitmap_bc = sc.broadcast(freq_b)

    # ---- pass 2: count only candidate pairs
    def candidates(b):
        fi = fi_bc.value
        keep = [i for i in b if i in fi]
        for x, y in combinations(keep, 2):
            if bitmap_bc is None or _pair_bucket(x, y, n_buckets) in bitmap_bc.value:
                yield (x, y), 1

    counted = baskets.flatMap(candidates).reduceByKey(lambda x, y: x + y).cache()
    n_candidates = counted.count()
    frequent = counted.filter(lambda kv: kv[1] >= min_support).cache()
    stats = {"method": "pcy" if n_buckets else "apriori", "n_baskets": baskets.count(),
             "n_frequent_items": len(freq_items), "n_candidate_pairs_counted": n_candidates,
             "n_frequent_pairs": frequent.count(), "n_buckets": n_buckets,
             "n_frequent_buckets": n_freq_buckets}
    return frequent, stats


def brute_force_pairs(baskets, min_support: int, max_basket: int = 50) -> dict:
    """Reference implementation on the driver (used by unit tests and tiny samples)."""
    c: Counter = Counter()
    for b in baskets:
        items = _clean(b, max_basket)
        if items:
            c.update(combinations(items, 2))
    return {p: n for p, n in c.items() if n >= min_support}


def fpgrowth(baskets_df, items_col: str = "items", min_support: float = 0.001,
             min_confidence: float = 0.1, num_partitions: int | None = None):
    """Spark MLlib FP-Growth; returns ``(freq_itemsets_df, association_rules_df)``."""
    from pyspark.ml.fpm import FPGrowth

    fp = FPGrowth(itemsCol=items_col, minSupport=min_support, minConfidence=min_confidence)
    if num_partitions:
        fp = fp.setNumPartitions(num_partitions)
    model = fp.fit(baskets_df)
    return model.freqItemsets, model.associationRules
