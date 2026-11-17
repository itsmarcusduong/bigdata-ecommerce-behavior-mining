import numpy as np

from bdm.lsh import (
    MinHasher,
    best_bands_rows,
    exact_similar_pairs,
    jaccard,
    lsh_similar_pairs,
    mllib_minhash_pairs,
    pair_recall_precision,
    s_curve,
)


def _cluster_sets(n=300, seed=0):
    rng = np.random.default_rng(seed)
    base = [rng.choice(5000, 40, replace=False) for _ in range(30)]
    sets = {}
    for i in range(n):
        b = base[i % 30]
        keep = rng.random(40) < 0.85
        extra = rng.choice(5000, 5, replace=False)
        sets[i] = set(b[keep].tolist()) | set(extra.tolist())
    return sets


def test_minhash_estimates_jaccard():
    rng = np.random.default_rng(0)
    a = set(rng.choice(1000, 300, replace=False).tolist())
    b = set(list(a)[:200]) | set(rng.choice(range(1000, 2000), 100, replace=False).tolist())
    h = MinHasher(512, seed=1)
    est = MinHasher.estimate(h.signature(a), h.signature(b))
    assert abs(est - jaccard(a, b)) < 0.07


def test_s_curve_and_band_choice():
    b, r = best_bands_rows(128, 0.5)
    assert b * r <= 128
    assert s_curve(0.8, b, r) > 0.95 and s_curve(0.2, b, r) < 0.1


def test_own_lsh_recall_matches_exact(spark):
    sets = _cluster_sets()
    truth = set(exact_similar_pairs(sets, 0.5))
    rdd = spark.sparkContext.parallelize(list(sets.items()), 4)
    pairs, info = lsh_similar_pairs(rdd, threshold=0.5, n_hashes=128, seed=3)
    found = {k for k, _ in pairs.collect()}
    rec, prec = pair_recall_precision(found, truth)
    assert rec >= 0.9 and prec == 1.0  # candidates are verified, so precision is exactly 1
    assert info["bands"] * info["rows"] <= 128


def test_mllib_minhash_runs(spark):
    sets = _cluster_sets(120)
    df = spark.createDataFrame([(i, sorted(s)) for i, s in sets.items()], ["id", "items"])
    out = mllib_minhash_pairs(df, "id", "items", 0.5, num_tables=10).collect()
    truth = set(exact_similar_pairs(sets, 0.5))
    found = {(r.id_a, r.id_b) for r in out}
    assert pair_recall_precision(found, truth)[0] >= 0.8
