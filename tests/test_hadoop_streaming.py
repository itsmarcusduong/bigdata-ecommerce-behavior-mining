"""The Hadoop Streaming scripts (run via Unix pipes) must agree with the Spark implementation."""
import os
import subprocess
from pathlib import Path

from pyspark.sql import functions as F

from bdm.fpm import brute_force_pairs
from bdm.sessions import session_baskets

ROOT = Path(__file__).resolve().parents[1]


def test_mapreduce_pairs_equal_spark_pairs(events, tmp_path):
    csv = Path(os.environ["BDM_DATA_DIR"]) / "2019-Oct.csv"
    subprocess.run([str(ROOT / "hadoop/run_local_mapreduce.sh"), str(csv), str(tmp_path)], check=True,
                   capture_output=True)
    mr = {}
    for line in (tmp_path / "pair_counts.tsv").read_text().splitlines():
        k, v = line.split("\t")
        a, b = k.split(",")
        mr[(int(a), int(b))] = int(v)
    oct_events = events.filter(F.month("event_time") == 10)
    baskets = [r["items"] for r in session_baskets(oct_events, ("cart", "purchase"), 2, 50).collect()]
    assert mr == brute_force_pairs(baskets, 1)
    assert sum(1 for _ in (tmp_path / "event_counts.tsv").open()) > 100
