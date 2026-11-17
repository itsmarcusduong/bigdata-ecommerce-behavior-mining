"""Environment helpers: locate Kaggle inputs and build a tuned local SparkSession."""

from __future__ import annotations

import glob
import os
import zipfile
from pathlib import Path

KAGGLE_INPUT = Path("/kaggle/input")


def on_kaggle() -> bool:
    return KAGGLE_INPUT.exists()


WORK_DIR = Path(os.environ.get("BDM_WORK_DIR", "/kaggle/working" if on_kaggle() else "work"))
DATA_DIR = Path(os.environ.get("BDM_DATA_DIR", "data/sample"))
RESULTS_DIR = WORK_DIR / "results"


def _roots(extra: list[str] | None = None) -> list[Path]:
    roots = [KAGGLE_INPUT, DATA_DIR, WORK_DIR]
    roots += [Path(p) for p in (extra or [])]
    return [r for r in roots if r.exists()]


def find_file(name: str, extra_roots: list[str] | None = None) -> str:
    """Find ``name`` under the known roots; transparently unzip ``name.zip`` if needed."""
    for root in _roots(extra_roots):
        hits = sorted(glob.glob(str(root / "**" / name), recursive=True))
        if hits:
            return hits[0]
    for root in _roots(extra_roots):
        zips = sorted(glob.glob(str(root / "**" / (name + ".zip")), recursive=True))
        if zips:
            target = WORK_DIR / "_unzipped"
            target.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zips[0]) as zf:
                zf.extract(name, target)
            return str(target / name)
    raise FileNotFoundError(f"{name} not found under {[str(r) for r in _roots(extra_roots)]}")


def find_dir(name: str, extra_roots: list[str] | None = None) -> str | None:
    """Find a directory called ``name`` (e.g. a Parquet dataset) under the known roots."""
    for root in _roots(extra_roots):
        hits = sorted(p for p in glob.glob(str(root / "**" / name), recursive=True) if os.path.isdir(p))
        if hits:
            return hits[0]
    return None


def get_spark(app_name: str = "bdm", driver_memory: str | None = None,
              shuffle_partitions: int | None = None, cores: str = "*"):
    """Create a local-mode SparkSession tuned for a single Kaggle machine (4 CPU / ~29 GB)."""
    import sys

    from pyspark.sql import SparkSession

    # Python workers are separate processes: make `bdm` importable there and use the same interpreter.
    src_dir = str(Path(__file__).resolve().parents[1])
    paths = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
    if src_dir not in paths:
        os.environ["PYTHONPATH"] = os.pathsep.join([src_dir] + paths)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    mem = driver_memory or os.environ.get("BDM_SPARK_MEM", "4g")
    parts = shuffle_partitions or int(os.environ.get("BDM_SHUFFLE_PARTITIONS", "64"))
    tmp = WORK_DIR / "spark-tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.master(f"local[{cores}]")
        .appName(app_name)
        .config("spark.driver.memory", mem)
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.shuffle.partitions", str(parts))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", str(tmp))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
        # Arrow off: toPandas() is only used on small aggregates, and with limit() Spark 3.5 prints
        # noisy "Memory was leaked by query" errors when Arrow batches are cancelled.
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
