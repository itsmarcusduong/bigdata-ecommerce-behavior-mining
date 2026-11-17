"""Shared fixtures: a local Spark session and a small synthetic dataset (same schema as Kaggle)."""
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))

_TMP = Path(tempfile.mkdtemp(prefix="bdm-test-"))
os.environ["BDM_DATA_DIR"] = str(_TMP / "data")
os.environ["BDM_WORK_DIR"] = str(_TMP / "work")
os.environ.setdefault("BDM_SPARK_MEM", "2g")
os.environ.setdefault("BDM_SHUFFLE_PARTITIONS", "8")

import pytest  # noqa: E402

import make_synthetic  # noqa: E402

make_synthetic.make_rees46(Path(os.environ["BDM_DATA_DIR"]), 1200, 500, seed=3)
make_synthetic.make_instacart(Path(os.environ["BDM_DATA_DIR"]), 600, 300, seed=3)


@pytest.fixture(scope="session")
def spark():
    from bdm.config import get_spark
    s = get_spark("bdm-tests", driver_memory="2g", shuffle_partitions=8, cores="2")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def events(spark):
    from bdm.io import read_rees46_csv
    return read_rees46_csv(spark).cache()
