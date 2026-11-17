from pyspark.sql import functions as F

from bdm import eda, io
from bdm.sessions import instacart_baskets, session_baskets, time_split


def test_ingest_and_parquet_roundtrip(spark, events, tmp_path):
    n = events.count()
    assert n > 10_000
    assert set(r[0] for r in events.select("event_type").distinct().collect()) == {"view", "cart", "purchase"}
    out = str(tmp_path / "events_parquet")
    io.write_events_parquet(events, out)
    assert spark.read.parquet(out).count() == n


def test_user_sampling_keeps_whole_users(events):
    s = io.sample_by_user(events, 0.2)
    assert 0 < s.select("user_id").distinct().count() < events.select("user_id").distinct().count()
    assert s.count() == events.join(s.select("user_id").distinct(), "user_id").count()


def test_eda_overview(events):
    o = eda.overview(events)
    assert o["n_events"] == events.count() and o["n_users"] > 100
    assert o["n_users"] == events.select("user_id").distinct().count()
    assert o["n_products"] == events.select("product_id").distinct().count()
    assert 0 < o["null_category_code"] < 0.3


def test_session_baskets_are_distinct_and_bounded(events):
    b = session_baskets(events, min_items=2, max_items=50)
    assert b.count() > 100
    assert b.filter(~F.col("items").isNotNull() | (F.size("items") < 2) | (F.size("items") > 50)).count() == 0


def test_time_split_has_no_leakage(events):
    train, test = time_split(events, "2019-11-15")
    assert train.agg(F.max("event_date")).first()[0].isoformat() < "2019-11-15"
    assert test.agg(F.min("event_date")).first()[0].isoformat() >= "2019-11-15"
    assert train.count() + test.count() == events.count()


def test_instacart_loader(spark):
    t = io.read_instacart(spark)
    assert t["prior"].count() > 1000
    assert instacart_baskets(t["prior"]).count() > 100
