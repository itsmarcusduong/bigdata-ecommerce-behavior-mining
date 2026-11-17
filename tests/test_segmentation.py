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
