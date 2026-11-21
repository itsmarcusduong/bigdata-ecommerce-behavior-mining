import math

from pyspark.sql import functions as F

from bdm import recsys as R
from bdm.graph import cooccurrence_edges
from bdm.sessions import session_baskets, time_split, user_item_matrix


def test_metrics_hand_computed():
    recs, truth = [1, 2, 3, 4], [2, 4, 9]
    assert R.precision_at_k(recs, truth, 4) == 0.5
    assert abs(R.recall_at_k(recs, truth, 4) - 2 / 3) < 1e-12
    assert R.hit_rate_at_k(recs, truth, 1) == 0.0 and R.hit_rate_at_k(recs, truth, 2) == 1.0
    assert abs(R.ap_at_k(recs, truth, 4) - (1 / 2 + 2 / 4) / 3) < 1e-12
    dcg = 1 / math.log2(3) + 1 / math.log2(5)
    idcg = 1 + 1 / math.log2(3) + 1 / math.log2(4)
    assert abs(R.ndcg_at_k(recs, truth, 4) - dcg / idcg) < 1e-12


def test_als_beats_popularity_on_structured_data(spark, events):
    train, test = time_split(events, "2019-11-10")
    ui = user_item_matrix(train, 2, 2).cache()
    model = R.train_als(ui, rank=16, max_iter=6)
    truth, seen = R.truth_and_seen(train, test)
    users = truth.select("user_id")
    als = R.evaluate(R.als_recs(model, users, 100), truth, seen, ks=(10,))
    pop = R.evaluate(R.popularity_recs(train.filter(F.col("event_type") == "purchase"), users, 100),
                     truth, seen, ks=(10,))
    assert als["n_users"] > 20
    assert all(0 <= v <= 1 for k, v in als.items() if k != "n_users")
    assert als["ndcg@10"] >= pop["ndcg@10"]


def test_itemknn_pipeline_runs(spark, events):
    train, test = time_split(events, "2019-11-10")
    edges = cooccurrence_edges(spark, session_baskets(train, ("view", "cart", "purchase"), 2, 30), min_weight=2)
    nbrs = R.item_neighbors(edges, 20)
    hist = train.filter(F.col("event_type").isin("cart", "purchase")).select("user_id", "product_id")
    truth, seen = R.truth_and_seen(train, test)
    res = R.evaluate(R.itemknn_recs(hist, nbrs, 100), truth, seen, ks=(10,))
    assert 0 <= res["ndcg@10"] <= 1


def test_evaluate_subset_is_deterministic(spark, events):
    train, test = time_split(events, "2019-11-10")
    truth, seen = R.truth_and_seen(train, test)
    pop = R.popularity_recs(train.filter(F.col("event_type") == "purchase"), truth.select("user_id"), 100)
    a = R.evaluate(pop, truth, seen, ks=(10,), max_users=100)
    b = R.evaluate(pop.repartition(7), truth, seen, ks=(10,), max_users=100)
    assert a == b and a["n_users"] == 100


def test_evaluate_restricted_users_and_per_user(spark, events):
    train, test = time_split(events, "2019-11-10")
    truth, seen = R.truth_and_seen(train, test)
    pop = R.popularity_recs(train.filter(F.col("event_type") == "purchase"), truth.select("user_id"), 100)
    some = truth.select("user_id").orderBy("user_id").limit(30)
    res = R.evaluate(pop, truth, seen, ks=(10,), users=some, per_user="ndcg@10")
    assert res["n_users"] == 30 and len(res["_per_user"]) == 30
    assert abs(sum(res["_per_user"].values()) / 30 - res["ndcg@10"]) < 1e-12


def test_with_fallback_fills_missing_users(spark):
    recs = spark.createDataFrame([(1, [10, 11])], "user_id long, recs array<long>")
    fb = spark.createDataFrame([(1, [99]), (2, [99])], "user_id long, recs array<long>")
    got = {r.user_id: r.recs for r in R.with_fallback(recs, fb).collect()}
    assert got == {1: [10, 11], 2: [99]}


def test_paired_bootstrap_detects_difference_and_no_difference():
    import random
    rnd = random.Random(0)
    a = {u: rnd.random() for u in range(500)}
    better = {u: v + 0.1 for u, v in a.items()}
    r = R.paired_bootstrap(better, a)
    assert abs(r["diff"] - 0.1) < 1e-9 and r["ci95"][0] > 0 and r["p_le_0"] == 0.0
    same = R.paired_bootstrap(a, dict(a))
    assert same["diff"] == 0.0 and same["ci95"] == [0.0, 0.0]


def test_history_recs_prefers_recent_and_heavier_events(spark):
    rows = [(1, 10, "view", "2019-11-01"), (1, 11, "view", "2019-11-09"), (1, 12, "cart", "2019-11-08"),
            (2, 10, "view", "2019-11-09")]
    ev = spark.createDataFrame(rows, "user_id long, product_id long, event_type string, event_date string") \
        .withColumn("event_date", F.col("event_date").cast("date"))
    users = ev.select("user_id").distinct()
    got = {r.user_id: list(r.recs) for r in R.history_recs(ev, users, "2019-11-10", half_life_days=3).collect()}
    # cart (weight 3, 2 days old) > view yesterday (1, 1 day) > view 9 days ago
    assert got[1] == [12, 11, 10] and got[2] == [10]


def test_recent_popularity_uses_only_the_window(spark):
    rows = [(1, 10, "purchase", "2019-10-01"), (2, 10, "purchase", "2019-10-01"), (3, 20, "purchase", "2019-11-08")]
    ev = spark.createDataFrame(rows, "user_id long, product_id long, event_type string, event_date string") \
        .withColumn("event_date", F.col("event_date").cast("date"))
    users = spark.createDataFrame([(9,)], "user_id long")
    got = R.recent_popularity_recs(ev, users, "2019-11-10", days=7, n=5).first().recs
    assert list(got) == [20]


def test_blend_keeps_order_and_dedupes(spark):
    a = spark.createDataFrame([(1, [5, 6])], "user_id long, recs array<long>")
    b = spark.createDataFrame([(1, [6, 7, 8]), (2, [9])], "user_id long, recs array<long>")
    got = {r.user_id: list(r.recs) for r in R.blend_recs(a, b, n=3).collect()}
    assert got == {1: [5, 6, 7], 2: [9]}


def test_selection_rule_and_verdict():
    assert R.select_best({"a": {"ndcg@10": 0.1}, "b": {"ndcg@10": 0.2}}) == "b"
    assert R.verdict({"ci95": [0.01, 0.02]}) == "better"
    assert R.verdict({"ci95": [-0.01, 0.02]}) == "tie"
    assert R.verdict({"ci95": [-0.03, -0.01]}) == "worse"
