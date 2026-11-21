from pyspark.sql import functions as F

from bdm import segmentation
from bdm.graph import cooccurrence_edges, transition_edges
from bdm.sessions import session_baskets


def test_graph_builders(spark, events):
    e = cooccurrence_edges(spark, session_baskets(events), min_weight=1)
    assert e.count() > 0
    assert e.filter(F.col("src") == F.col("dst")).count() == 0
    t = transition_edges(events, min_weight=1)
    assert t.count() > 0 and t.filter(F.col("src") == F.col("dst")).count() == 0


def test_rfm_kmeans_and_funnel(events):
    rfm = segmentation.rfm_features(events)
    assert rfm.filter((F.col("recency") < 0) | (F.col("frequency") < 1)).count() == 0
    seg, k, scores = segmentation.kmeans_segments(rfm, ks=(2, 3))
    assert k in (2, 3) and seg.select("segment").distinct().count() == k
    f = segmentation.funnel_by_category(events)
    assert f.count() > 0 and f.filter(F.col("view_to_cart") > 1.5).count() == 0


def test_adjusted_rand_index_matches_known_values():
    ari = segmentation.adjusted_rand_index
    assert ari([0, 0, 1, 1], [1, 1, 0, 0]) == 1.0           # same partition, renamed labels
    assert abs(ari([0, 0, 0, 1, 1, 1], [0, 0, 1, 1, 2, 2]) - 0.2424) < 1e-3   # sklearn: 0.24242424
    try:
        import random

        from sklearn.metrics import adjusted_rand_score
        rnd = random.Random(1)
        a = [rnd.randrange(4) for _ in range(300)]
        b = [rnd.randrange(3) for _ in range(300)]
        assert abs(ari(a, b) - adjusted_rand_score(a, b)) < 1e-9
    except ImportError:
        pass


def test_compare_clusterers_runs_all_algorithms(events):
    x = segmentation.rfm_matrix(segmentation.rfm_features(events)).cache()
    rows, stab = segmentation.compare_clusterers(x, ks=(2, 3), seeds=(1, 2), sample_size=200)
    assert {r["algo"] for r in rows} == {"kmeans", "bisecting", "gmm"} and len(rows) == 12
    assert all(-1 <= r["silhouette"] <= 1 or r["degenerate"] for r in rows)
    assert all(-1 <= v <= 1 for v in stab.values())


def test_degenerate_fits_are_flagged(events):
    x = segmentation.rfm_matrix(segmentation.rfm_features(events)).repartition(8).cache()
    rows, _ = segmentation.compare_clusterers(x, algos=("kmeans", "bisecting"), ks=(3,), seeds=(42,), sample_size=100)
    for r in rows:
        assert r["degenerate"] == (r["clusters_used"] < r["k"])
    assert not [r for r in rows if r["algo"] == "kmeans"][0]["degenerate"]


def test_bisecting_splits_standardised_features(events):
    # zero-mean features used to leave Bisecting K-Means with a single cluster ([n, 0, 0])
    x = segmentation.rfm_matrix(segmentation.rfm_features(events)).cache()
    model, data = segmentation.fit_clusterer("bisecting", 3, 42, x)
    assert model.transform(data).select("prediction").distinct().count() == 3


def test_translation_keeps_silhouette(events):
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    x = segmentation.rfm_matrix(segmentation.rfm_features(events)).cache()
    shifted = segmentation.clusterer_input("bisecting", x)
    a = ClusteringEvaluator().evaluate(KMeans(k=3, seed=1).fit(x).transform(x))
    b = ClusteringEvaluator().evaluate(KMeans(k=3, seed=1).fit(shifted).transform(shifted))
    assert abs(a - b) < 1e-6
