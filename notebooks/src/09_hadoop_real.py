# %% [markdown]
# # 09 - Hadoop MapReduce (Streaming) on the real REES46 CSV, checked against Spark
# The same Python mapper/reducer scripts as `hadoop/streaming/*` run on **Hadoop 3.3.6** (standalone, local job
# runner, 4 map/reduce slots) over the raw CSV files (~110M lines), then the outputs are compared key by key with a
# Spark job that applies exactly the same parsing rules to the same lines.
#
# | job | mapper -> reducer | Spark equivalent |
# |---|---|---|
# | event counts | `product_id:event_type -> 1`, combiner + reducer sum | `groupBy(key).count()` |
# | session baskets | `(session, product)` for cart/purchase -> all pairs a<b of baskets with 2..50 products | self-join per session |
# | pair counts | identity -> sum | `groupBy(pair).count()` |
#
# Kaggle has one machine, so this is Hadoop's local mode, not a cluster; the point is that the MapReduce programs
# run on real Hadoop over the real data and give the same answer as Spark, and what each costs.

# %%
import shutil
import subprocess
import time
from pathlib import Path

from pyspark.sql import functions as F

import bdm
from bdm import config
from bdm.results import save_fig, save_result

HV = "3.3.6"
HH = Path(os.environ.get("HADOOP_HOME", f"/tmp/hadoop-{HV}"))
FILES = [f for f in os.environ.get("BDM_HADOOP_FILES", "2019-Oct.csv,2019-Nov.csv").split(",") if f]
STREAMING = Path(bdm.__file__).resolve().parents[2] / "hadoop" / "streaming"
OUT = Path(os.environ.get("BDM_HADOOP_OUT", "/tmp/hadoop-real"))
LOG_DIR = config.RESULTS_DIR / "hadoop"; LOG_DIR.mkdir(parents=True, exist_ok=True)
SLOTS = int(os.environ.get("BDM_HADOOP_SLOTS", "4"))
csvs = [config.find_file(f) for f in FILES]
csvs = [c for c in csvs if c]
print("inputs:", csvs, "| sizes (GB):", [round(Path(c).stat().st_size / 1e9, 2) for c in csvs])
out = {"fraction": 1.0, "inputs": [Path(c).name for c in csvs], "input_bytes": sum(Path(c).stat().st_size for c in csvs),
       "hadoop_version": HV, "slots": SLOTS}

# %% [markdown]
# ## 1. Install Hadoop 3.3.6 (needs Internet on Kaggle) and run the three jobs

# %%
def sh(cmd, **kw):
    return subprocess.run(cmd, shell=True, text=True, capture_output=True, **kw)


engine = "hadoop"
if not (HH / "bin" / "hadoop").exists():
    t0 = time.perf_counter()
    r = sh(f"curl -sSfL https://archive.apache.org/dist/hadoop/common/hadoop-{HV}/hadoop-{HV}.tar.gz | tar -xz -C {HH.parent}")
    print("download:", r.returncode, f"{time.perf_counter() - t0:.0f}s", r.stderr[-300:])
if not (HH / "bin" / "hadoop").exists():
    if config.on_kaggle():
        raise RuntimeError("Hadoop could not be installed on Kaggle (Internet off?)")
    engine = "unix-pipe (local smoke test only)"
java = shutil.which("java")
env = dict(os.environ, JAVA_HOME=os.environ.get("JAVA_HOME") or str(Path(os.path.realpath(java)).parents[1]),
           HADOOP_HEAPSIZE_MAX=os.environ.get("BDM_HADOOP_HEAP", "8g"))
jar = next(iter(sorted((HH / "share/hadoop/tools/lib").glob("hadoop-streaming-*.jar"))), None) if engine == "hadoop" else None
print("engine:", engine, "| java:", env["JAVA_HOME"], "| jar:", jar)
shutil.rmtree(OUT, ignore_errors=True); OUT.mkdir(parents=True)


def run_job(name, mapper, reducer, inputs, combiner=None, reduces=4):
    """Run one streaming job; returns (seconds, counters dict). Output in OUT/name."""
    t0 = time.perf_counter()
    if engine == "hadoop":
        files = ",".join(str(STREAMING / f) for f in {mapper, reducer} | ({combiner} if combiner else set()))
        cmd = (f"{HH}/bin/hadoop jar {jar} -D mapreduce.job.reduces={reduces} "
               f"-D mapreduce.local.map.tasks.maximum={SLOTS} -D mapreduce.local.reduce.tasks.maximum={SLOTS} "
               f"-files {files} -mapper 'python3 {mapper}' -reducer 'python3 {reducer}' "
               + (f"-combiner 'python3 {combiner}' " if combiner else "")
               + f"-input {inputs} -output {OUT / name}")
        r = sh(cmd, env=env)
        (LOG_DIR / f"real_{name}.log").write_text(r.stderr[-200_000:])
        if r.returncode != 0:
            print(r.stderr[-3000:]); raise RuntimeError(f"hadoop job {name} failed")
        counters = {}
        for line in r.stderr.splitlines():
            line = line.strip()
            for key in ("Map input records", "Map output records", "Combine input records", "Combine output records",
                        "Reduce input records", "Reduce output records", "Spilled Records", "Bytes Read", "Bytes Written"):
                if line.startswith(key + "="):
                    counters[key] = counters.get(key, 0) + int(line.split("=")[1])
    else:  # pipe emulation so the notebook can be smoke-tested without Hadoop
        (OUT / name).mkdir(parents=True)
        src = " ".join(str(Path(p)) for p in inputs.split(",")) if "," in inputs or Path(inputs).is_file() \
            else " ".join(str(p) for p in sorted(Path(inputs).glob("part-*")))
        cmd = (f"cat {src} | python3 {STREAMING / mapper} | LC_ALL=C sort | python3 {STREAMING / reducer} "
               f"> {OUT / name / 'part-00000'}")
        r = sh(cmd)
        if r.returncode != 0:
            raise RuntimeError(r.stderr[-2000:])
        counters = {}
    return round(time.perf_counter() - t0, 1), counters


inputs = ",".join(csvs)
t_counts, c_counts = run_job("counts", "count_mapper.py", "sum_reducer.py", inputs, combiner="sum_reducer.py")
print(f"job counts: {t_counts}s", c_counts)
t_job1, c_job1 = run_job("baskets", "basket_mapper.py", "pair_reducer.py", inputs)
print(f"job baskets->pairs: {t_job1}s", c_job1)
t_job2, c_job2 = run_job("pairs", "identity_mapper.py", "sum_reducer.py", str(OUT / "baskets"), combiner="sum_reducer.py")
print(f"job pair counts: {t_job2}s", c_job2)

# %% [markdown]
# ## 2. The same computation in Spark, on the same raw lines and with the same parsing rules

# %%
spark = config.get_spark("09-hadoop-real", driver_memory=SPARK_MEM)
lines = spark.read.text(csvs).select(F.split("value", ",").alias("p"))
rows = lines.filter((F.size("p") >= 9) & (F.col("p").getItem(0) != "event_time"))
t0 = time.perf_counter()
sp_counts = rows.select(F.concat_ws(":", F.col("p").getItem(2), F.col("p").getItem(1)).alias("key")).groupBy("key").count().cache()
n_sp_counts = sp_counts.count(); t_sp_counts = round(time.perf_counter() - t0, 1)
t0 = time.perf_counter()
si = (rows.filter(F.col("p").getItem(1).isin("cart", "purchase") & (F.col("p").getItem(8) != ""))
      .select(F.col("p").getItem(8).alias("s"), F.col("p").getItem(2).cast("long").alias("i")).distinct())
ok_sessions = si.groupBy("s").count().filter(F.col("count").between(2, 50)).select("s")
si = si.join(ok_sessions, "s")
sp_pairs = (si.alias("a").join(si.alias("b"), "s").filter(F.col("a.i") < F.col("b.i"))
            .select(F.concat_ws(",", F.col("a.i"), F.col("b.i")).alias("key")).groupBy("key").count().cache())
n_sp_pairs = sp_pairs.count(); t_sp_pairs = round(time.perf_counter() - t0, 1)
print(f"Spark: {n_sp_counts:,} count keys in {t_sp_counts}s; {n_sp_pairs:,} pair keys in {t_sp_pairs}s")

# %% [markdown]
# ## 3. Key-by-key comparison

# %%
def read_hadoop(name):
    t = spark.read.text(str(OUT / name / "part-*")).select(F.split("value", "\t").alias("kv"))
    return t.select(F.col("kv").getItem(0).alias("key"), F.col("kv").getItem(1).cast("long").alias("h"))


def compare(h, s):
    j = h.join(s.withColumnRenamed("count", "s"), "key", "full")
    r = j.agg(F.count("h").alias("keys_hadoop"), F.count("s").alias("keys_spark"),
              F.sum("h").alias("total_hadoop"), F.sum("s").alias("total_spark"),
              F.sum(F.when(F.col("h").isNull() | F.col("s").isNull() | (F.col("h") != F.col("s")), 1).otherwise(0))
              .alias("mismatched_keys")).first().asDict()
    r["identical"] = r["mismatched_keys"] == 0
    return r


cmp_counts = compare(read_hadoop("counts"), sp_counts)
cmp_pairs = compare(read_hadoop("pairs"), sp_pairs)
print("event counts:", cmp_counts); print("pair counts:", cmp_pairs)
top_pairs = sp_pairs.orderBy(F.desc("count")).limit(10).toPandas()
top_pairs

# %%
timing = pd.DataFrame({"job": ["event counts", "baskets + pairs (2 jobs)"],
                       "hadoop_s": [t_counts, t_job1 + t_job2], "spark_s": [t_sp_counts, t_sp_pairs]})
fig, ax = plt.subplots(figsize=(6.5, 3.4)); timing.set_index("job").plot.bar(ax=ax, rot=0)
ax.set_ylabel("seconds"); ax.set_title(f"Hadoop {HV} (local, {SLOTS} slots) vs Spark local[*]: same answer")
save_fig(fig, "09_hadoop_vs_spark"); plt.show()
timing

# %%
out.update({"engine": engine, "jobs": {"counts": {"seconds": t_counts, "counters": c_counts},
                                       "baskets_to_pairs": {"seconds": t_job1, "counters": c_job1},
                                       "pair_counts": {"seconds": t_job2, "counters": c_job2}},
            "spark": {"counts_seconds": t_sp_counts, "pairs_seconds": t_sp_pairs},
            "compare_event_counts": cmp_counts, "compare_pair_counts": cmp_pairs,
            "identical": bool(cmp_counts["identical"] and cmp_pairs["identical"]),
            "top_pairs": top_pairs.to_dict("records")})
print("saved", save_result("09_hadoop_real", out))
