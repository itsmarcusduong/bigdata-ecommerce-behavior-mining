import random

from bdm.fpm import brute_force_pairs, count_pairs, fpgrowth


def _baskets(n=3000, seed=0):
    rnd = random.Random(seed)
    groups = [list(range(g * 10, g * 10 + 10)) for g in range(8)]
    out = []
    for _ in range(n):
        g = rnd.choice(groups)
        out.append(rnd.sample(g, rnd.randint(2, 5)) + ([rnd.randint(100, 400)] if rnd.random() < 0.5 else []))
    return out


def test_apriori_and_pcy_equal_brute_force(spark):
    baskets = _baskets()
    ref = brute_force_pairs(baskets, 40)
    rdd = spark.sparkContext.parallelize(baskets, 4)
    for nb in (None, 1009):
        pairs, stats = count_pairs(rdd, 40, n_buckets=nb)
        assert dict(pairs.collect()) == ref, stats["method"]


def test_pcy_counts_fewer_candidates_than_apriori(spark):
    rdd = spark.sparkContext.parallelize(_baskets(), 4)
    _, a = count_pairs(rdd, 40)
    _, p = count_pairs(rdd, 40, n_buckets=50_021)
    assert p["n_candidate_pairs_counted"] <= a["n_candidate_pairs_counted"]
    assert p["n_frequent_pairs"] == a["n_frequent_pairs"]


def test_fpgrowth_consistent_with_pair_counts(spark):
    baskets = _baskets()
    df = spark.createDataFrame([(i, sorted(set(b))) for i, b in enumerate(baskets)], ["id", "items"])
    freq, rules = fpgrowth(df, min_support=40 / len(baskets), min_confidence=0.1)
    fp_pairs = {tuple(sorted(r["items"])): r["freq"] for r in freq.collect() if len(r["items"]) == 2}
    assert fp_pairs == brute_force_pairs([sorted(set(b)) for b in baskets], 40)
    assert rules.count() > 0
