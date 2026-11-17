import networkx as nx
import numpy as np
from pyspark.sql import functions as F

from bdm.graph import hits, pagerank, transition_edges


def _edges(spark, n=60, m=300, seed=0):
    rng = np.random.default_rng(seed)
    e = {(int(a), int(b)): float(rng.integers(1, 5)) for a, b in rng.integers(0, n, (m, 2)) if a != b}
    # nodes >= n-5 never get out-edges -> dead ends
    e = {k: v for k, v in e.items() if k[0] < n - 5}
    return e, spark.createDataFrame([(a, b, w) for (a, b), w in e.items()], ["src", "dst", "weight"])


def test_pagerank_matches_networkx(spark):
    e, df = _edges(spark)
    g = nx.DiGraph()
    g.add_weighted_edges_from([(a, b, w) for (a, b), w in e.items()])
    ref = nx.pagerank(g, alpha=0.85, tol=1e-12, max_iter=500)
    ranks, deltas = pagerank(df, beta=0.85, max_iter=100, tol=1e-12)
    got = {r["node"]: r["rank"] for r in ranks.collect()}
    assert abs(sum(got.values()) - 1) < 1e-9
    assert max(abs(got[k] - ref[k]) for k in ref) < 1e-6
    assert deltas[-1] < deltas[0]


def test_personalized_pagerank_matches_networkx(spark):
    e, df = _edges(spark, seed=2)
    g = nx.DiGraph()
    g.add_weighted_edges_from([(a, b, w) for (a, b), w in e.items()])
    pers = {n: (1.0 if n in (0, 1, 2) else 0.0) for n in g.nodes}
    ref = nx.pagerank(g, alpha=0.85, personalization=pers, tol=1e-12, max_iter=500)
    p_df = spark.createDataFrame([(0, 1.0), (1, 1.0), (2, 1.0)], ["node", "p"])
    ranks, _ = pagerank(df, personalization=p_df, max_iter=100, tol=1e-12)
    got = {r["node"]: r["rank"] for r in ranks.collect()}
    assert max(abs(got[k] - ref[k]) for k in ref) < 1e-6


def test_hits_matches_networkx(spark):
    e, df = _edges(spark, seed=4)
    g = nx.DiGraph()
    g.add_weighted_edges_from([(a, b, 1.0) for (a, b) in e])
    h_ref, a_ref = nx.hits(g, max_iter=1000, tol=1e-12)
    unit = df.selectExpr("src", "dst", "1.0 as weight")
    res = {r["node"]: r for r in hits(unit, max_iter=200, tol=1e-12).collect()}
    s_h, s_a = sum(r["hub"] for r in res.values()), sum(r["auth"] for r in res.values())
    assert max(abs(res[k]["auth"] / s_a - a_ref[k]) for k in a_ref) < 1e-4
    assert max(abs(res[k]["hub"] / s_h - h_ref[k]) for k in h_ref) < 1e-4


def _tied_edges(spark, order):
    rows = [(f"s{i}", "2019-10-01 10:00:00", "view", p) for i in range(4) for p in order]
    df = spark.createDataFrame(rows, ["user_session", "t", "event_type", "product_id"])
    df = df.withColumn("event_time", F.to_timestamp("t"))
    return {(r.src, r.dst) for r in transition_edges(df, min_weight=1).collect()}


def test_transition_edges_deterministic_under_timestamp_ties(spark):
    """Events with identical timestamps must give the same graph whatever the input row order."""
    assert _tied_edges(spark, [1, 2, 3]) == _tied_edges(spark, [3, 2, 1]) == {(1, 2), (2, 3)}
