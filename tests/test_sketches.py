import numpy as np

from bdm.sketches import DGIM, BloomFilter, CountMinSketch, HeavyHitters, MisraGries, reservoir_sample


def _zipf_stream(n=200_000, vocab=5_000, seed=0):
    rng = np.random.default_rng(seed)
    return (rng.zipf(1.3, n) % vocab).astype(np.int64)


def test_cms_never_underestimates_and_respects_error_bound():
    data = _zipf_stream()
    eps, delta = 0.001, 0.01
    cms = CountMinSketch.from_error(eps, delta, seed=1)
    cms.update_many(data)
    keys, true = np.unique(data, return_counts=True)
    est = cms.estimate_many(keys)
    assert (est >= true).all()
    assert (np.abs(est - true) <= eps * len(data)).mean() >= 1 - delta - 0.01


def test_cms_merge_equals_single_pass():
    data = _zipf_stream(50_000)
    a, b, full = (CountMinSketch(512, 4, 3) for _ in range(3))
    a.update_many(data[:20_000])
    b.update_many(data[20_000:])
    full.update_many(data)
    assert (a.merge(b).table == full.table).all()


def test_heavy_hitters_finds_true_top():
    data = _zipf_stream()
    hh = HeavyHitters(10, CountMinSketch.from_error(0.0005, 0.01, seed=2))
    for chunk in np.array_split(data, 40):
        hh.update(chunk)
    keys, counts = np.unique(data, return_counts=True)
    true_top = set(keys[np.argsort(-counts)[:10]].tolist())
    assert len(true_top & {k for k, _ in hh.top()}) >= 9


def test_bloom_no_false_negatives_and_fpr_close_to_theory():
    rng = np.random.default_rng(0)
    members = rng.choice(10**8, 20_000, replace=False)
    others = np.setdiff1d(rng.choice(10**8, 40_000, replace=False), members)
    m, k = BloomFilter.optimal_params(20_000, 0.01)
    bf = BloomFilter(m, k, seed=5)
    bf.add_many(members)
    assert bf.contains_many(members).all()
    fpr = bf.contains_many(others).mean()
    assert fpr < 0.02
    assert abs(fpr - bf.expected_fpr(20_000)) < 0.01


def test_bloom_merge():
    a, b = BloomFilter(10_000, 3, 1), BloomFilter(10_000, 3, 1)
    a.add_many([1, 2, 3])
    b.add_many([4, 5, 6])
    a.merge(b)
    assert a.contains_many([1, 2, 3, 4, 5, 6]).all()


def test_dgim_within_50_percent():
    rng = np.random.default_rng(1)
    bits = (rng.random(20_000) < 0.3).astype(int)
    window = 1000
    d = DGIM(window)
    worst = 0.0
    for t, b in enumerate(bits, start=1):
        d.update(b)
        if t >= window and t % 97 == 0:
            exact = bits[t - window:t].sum()
            worst = max(worst, abs(d.count() - exact) / max(exact, 1))
    assert worst <= 0.5
    assert len(d.buckets) < 40  # O(log N) buckets, not O(N)


def test_misra_gries_guarantee():
    data = _zipf_stream(30_000, 500)
    mg = MisraGries(50)
    for x in data:
        mg.update(int(x))
    keys, counts = np.unique(data, return_counts=True)
    for key, c in zip(keys, counts):
        est = mg.counters.get(int(key), 0)
        assert c - len(data) / 50 <= est <= c


def test_reservoir_sample_size_and_uniformity():
    s = reservoir_sample(range(100_000), 1000, seed=0)
    assert len(s) == 1000 and len(set(s)) == 1000
    assert 40_000 < np.mean(s) < 60_000
