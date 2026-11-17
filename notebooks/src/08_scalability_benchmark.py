# %% [markdown]
# # 08 - Scalability benchmark
# Does the pipeline scale?  We time the main kernels on growing **user-level samples** (1% ... 100%) and with
# 1, 2, 4 cores, then report throughput, empirical complexity and speed-up.
#
# Kernels: (a) scan+aggregate, (b) session baskets -> FP-Growth, (c) PageRank (5 iterations), (d) ALS (3 iterations).
#
# > Run this notebook **last**, ideally with the Parquet from notebook 01 (it is I/O sensitive).
# > The core-scaling part restarts Spark with `local[n]`.

# %%
import time

import numpy as np
from pyspark.sql import functions as F

from bdm import config, io, recsys
from bdm.fpm import fpgrowth
from bdm.graph import pagerank, transition_edges
from bdm.results import save_fig, save_result
from bdm.sessions import session_baskets, user_item_matrix

FRACTIONS = [float(x) for x in os.environ.get("BDM_BENCH_FRACTIONS", "0.01,0.05,0.1,0.25,0.5,1.0").split(",")]
CORES = [int(x) for x in os.environ.get("BDM_BENCH_CORES", "1,2,4").split(",")]
CORE_FRACTION = float(os.environ.get("BDM_BENCH_CORE_FRACTION", "0.1"))
# Fractions are of the FULL data. The Parquet from notebook 01 only holds FRACTION of the users, so larger
# fractions would silently re-read the same sample: keep only those that exist.
FRACTIONS = [f for f in FRACTIONS if f <= FRACTION + 1e-9] or [FRACTION]
CORE_FRACTION = min(CORE_FRACTION, FRACTION)
print("size fractions:", FRACTIONS, "| core-scaling fraction:", CORE_FRACTION)


def run_kernels(spark, frac: float) -> dict:
    ev = io.load_events(spark, frac).cache()
    r = {"fraction": frac}
    t0 = time.perf_counter(); r["n_events"] = ev.count(); r["t_load_count"] = time.perf_counter() - t0
    t0 = time.perf_counter(); ev.groupBy("user_id").agg(F.count("*"), F.sum("price")).count(); r["t_aggregate"] = time.perf_counter() - t0
    t0 = time.perf_counter()
    b = session_baskets(ev); n_b = b.count()
    fpgrowth(b, "items", max(0.001, 5 / max(n_b, 1)), 0.2)[0].count(); r["t_fpgrowth"] = time.perf_counter() - t0
    t0 = time.perf_counter(); e = transition_edges(ev, 2).cache(); r["n_edges"] = e.count()
    if r["n_edges"]:
        pagerank(e, max_iter=5, tol=0)[0].count()
    r["t_pagerank5"] = time.perf_counter() - t0
    t0 = time.perf_counter(); ui = user_item_matrix(ev, 2, 3).cache()
    if ui.count():
        recsys.train_als(ui, rank=16, max_iter=3)
    r["t_als3"] = time.perf_counter() - t0
    ev.unpersist()
    return {k: (round(v, 2) if isinstance(v, float) else v) for k, v in r.items()}


# %% [markdown]
# ## 1. Data-size scaling (all cores)

# %%
spark = config.get_spark("08-bench", driver_memory=SPARK_MEM)
rows = []
for f in FRACTIONS:
    rows.append(run_kernels(spark, f)); print(rows[-1])
size = pd.DataFrame(rows)
size["events_per_s_aggregate"] = (size["n_events"] / size["t_aggregate"]).round(0)
size

# %%
tcols = ["t_aggregate", "t_fpgrowth", "t_pagerank5", "t_als3"]
fig, ax = plt.subplots(figsize=(7, 4.2))
for c in tcols:
    ax.loglog(size["n_events"], size[c], marker="o", label=c)
ax.loglog(size["n_events"], size["t_aggregate"].iloc[0] * size["n_events"] / size["n_events"].iloc[0], "k:", label="linear")
ax.set_xlabel("events"); ax.set_ylabel("seconds"); ax.legend(); ax.set_title("Runtime vs data size")
save_fig(fig, "08_size_scaling"); plt.show()
slopes = {c: float(np.polyfit(np.log(size["n_events"]), np.log(size[c].clip(lower=1e-2)), 1)[0]) for c in tcols}
print("empirical exponents (1.0 = linear):", {k: round(v, 2) for k, v in slopes.items()})

# %% [markdown]
# ## 2. Core scaling (strong scaling at a fixed data size)

# %%
core_rows = []
for c in CORES:
    spark.stop()
    spark = config.get_spark(f"08-bench-{c}c", driver_memory=SPARK_MEM, cores=str(c))
    r = run_kernels(spark, CORE_FRACTION); r["cores"] = c; core_rows.append(r); print(r)
cores = pd.DataFrame(core_rows).set_index("cores")
speed = cores[tcols].rdiv(cores[tcols].iloc[0]).round(2)
speed.columns = [c.replace("t_", "speedup_") for c in speed.columns]
speed

# %%
fig, ax = plt.subplots(figsize=(5.5, 4)); speed.plot(marker="o", ax=ax)
ax.plot(speed.index, speed.index / speed.index[0], "k:", label="ideal"); ax.set_ylabel("speed-up vs 1 core"); ax.legend()
ax.set_title(f"Strong scaling ({int(CORE_FRACTION * 100)}% of users)"); save_fig(fig, "08_core_scaling"); plt.show()

# %%
print("saved", save_result("08_scalability_benchmark", {"base_fraction": FRACTION, "size_scaling": size.to_dict("records"), "exponents": slopes,
                           "core_scaling": cores.reset_index().to_dict("records"), "speedup": speed.reset_index().to_dict("records")}))
