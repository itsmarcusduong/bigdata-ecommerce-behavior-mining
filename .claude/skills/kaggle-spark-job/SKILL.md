---
name: kaggle-spark-job
description: Use when writing, editing or debugging a PySpark notebook/job that runs on Kaggle (CPU session) over the REES46 / Instacart data - input paths, memory settings, Parquet chaining, progressive FRACTION runs and OOM fixes.
---

# kaggle-spark-job

Goal: a notebook that runs top-to-bottom on a Kaggle CPU session (4 cores, ~29 GB RAM, limited `/kaggle/working`) and
produces `results/<nn>_<name>.json` + figures. Spark runs in `local[*]`; there is no cluster.

## Do
1. **Edit the source, not the notebook**: change `notebooks/src/NN_*.py` (jupytext `# %%` cells), then `python tools/build_notebooks.py`.
   The bootstrap cell (added by the builder) clones the repo on Kaggle, sets `FRACTION`, `SPARK_MEM`.
2. **Find inputs with `bdm.config.find_file / find_dir`** (searches `/kaggle/input/**`, unzips `*.csv.zip`). Never hard-code `/kaggle/input/<slug>/...`.
3. **Create Spark only with `bdm.config.get_spark(app, driver_memory=SPARK_MEM)`**. It sets driver memory, shuffle partitions, `spark.local.dir`,
   and `PYTHONPATH` so Python workers can `import bdm`. Driver memory cannot change after the JVM started - restart the kernel to change it.
4. **Read Parquet, not CSV**: notebook 01 writes `events_parquet/` partitioned by `event_date`; later notebooks call `io.load_events(spark, FRACTION)`.
5. **Scale progressively**: `FRACTION=0.01` (find bugs) -> `0.1` -> `1.0`. Sampling is by **user hash** (`io.sample_by_user`) so sessions stay whole.
6. **Aggregate in Spark, collect small things**: `toPandas()`/`collect()` only on aggregated or explicitly limited data (<~1M rows).
7. **Save evidence**: end each experiment with `save_result(...)` and `save_fig(...)`; record `FRACTION`, timings, sizes.

## Memory / speed checklist (when it is slow or dies)
* Lower `FRACTION`; raise `BDM_SHUFFLE_PARTITIONS` (200-400 for the full data); avoid wide `cache()` on the 110M-row frame (cache a projected/filtered frame).
* Use explicit schemas (no `inferSchema` on 14 GB). Filter and project columns before joins. Prefer `approx_count_distinct` / `percentile_approx`.
* `rdd.groupByKey` on skewed keys -> use `reduceByKey` / `aggregateByKey`, cap bucket sizes (`max_bucket` in `lsh_similar_pairs`).
* Iterative algorithms (PageRank, HITS) must truncate lineage each iteration (`localCheckpoint`) - already done in `bdm/graph.py`.
* Disk: clear `/kaggle/working/spark-tmp`, keep only needed outputs (<20 GB limit).

## Don't
* Don't use Python's built-in `hash()` in distributed code (salted per process) - use `zlib.crc32`/Spark functions.
* Don't leak test data into training (see `bdm-experiment-protocol`).
* Don't claim a Kaggle result you did not run; synthetic smoke runs (`make smoke`) only prove the code works.

## Verify before opening a PR
`make lint && make test && make smoke` (smoke runs every notebook on `tools/make_synthetic.py` data), then a real `FRACTION=0.01` run on Kaggle if the change touches I/O or memory behaviour.
