# %% [markdown]
# # 11 - Recommender test for the extended grid, one seed per kernel (ADR-0007)
# Notebook 10 finished its reproduction gate and validation (selected: history + ALS; history half-life 7 days,
# ALS rank 128 / alpha 10) but was stopped by Kaggle's 12-hour limit before the test. This notebook runs the test
# **once** for a single ALS seed (`BDM_SEED`); three kernels run in parallel (42, 7, 2024) and
# `tools/combine_seed_runs.py` merges them under the ADR-0002 rule (better only if the CI excludes 0 for every seed).
# No setting is chosen here: the configuration is fixed by notebook 10's validation.

# %%
import gc
import time

from pyspark.sql import functions as F

from bdm import config, io, recsys
from bdm.results import mark_stage, save_result
from bdm.sessions import time_split, user_item_matrix

SEED = int(os.environ.get("BDM_SEED", "42"))
NB = f"11_recsys_test_seed{SEED}"
HALF_LIFE = float(os.environ.get("BDM_HALF_LIFE", "7"))
RANK = int(os.environ.get("BDM_ALS_RANK", "128"))
ALPHA = float(os.environ.get("BDM_ALS_ALPHA", "10"))
CUTOFF = os.environ.get("BDM_CUTOFF", "2019-11-15")

spark = config.get_spark(f"11-recsys-test-{SEED}", driver_memory=SPARK_MEM)
events = io.load_events(spark, FRACTION)
train, test = time_split(events, CUTOFF)
train.cache(); test.cache()
truth, seen = recsys.truth_and_seen(train, test)
truth.cache(); seen.cache()
users = truth.select("user_id")
n_eval = truth.count()
print(f"seed {SEED}: evaluable test users {n_eval:,}; history {HALF_LIFE} d, ALS rank {RANK}, alpha {ALPHA}")
mark_stage(NB, "split", n_eval_users=n_eval)
out = {"fraction": FRACTION, "cutoff": CUTOFF, "seed": SEED, "adr": "docs/decisions/ADR-0007-recsys-test-per-seed.md",
       "config": {"history": {"half_life_days": HALF_LIFE}, "als": {"rank": RANK, "alpha": ALPHA, "reg": 0.05, "max_iter": 8}}}

# %% [markdown]
# ## Refit on all training data and score once on test

# %%
t0 = time.perf_counter()
pop = recsys.popularity_recs(train.filter(F.col("event_type") == "purchase"), users, 100).cache()
hist_r = recsys.history_recs(train, users, CUTOFF, half_life_days=HALF_LIFE).cache()
ui = user_item_matrix(train, min_user_items=3, min_item_users=5).cache()
mdl = recsys.train_als(ui, rank=RANK, reg=0.05, alpha=ALPHA, max_iter=8, seed=SEED)
als_r = recsys.als_recs(mdl, users, 100).cache(); als_r.count()
als_seconds = round(time.perf_counter() - t0, 1)
del mdl; gc.collect()
mark_stage(NB, "refit", seconds=als_seconds)
blend = recsys.blend_recs(hist_r, als_r).cache(); blend.count()
common = als_r.select("user_id").join(hist_r.select("user_id"), "user_id").join(users, "user_id").cache()
n_common = common.count()
print(f"users history and ALS can both serve: {n_common:,} of {n_eval:,}; refit {als_seconds:.0f}s")

models = {"popularity": pop, "history": hist_r, "als": als_r, "history_als": blend}
test_rows, per_user, coverage = [], {}, {}
for name, r in models.items():
    m = recsys.evaluate(r, truth, seen, users=common, per_user="ndcg@10")
    per_user[name] = m.pop("_per_user"); test_rows.append({"model": name, "seed": SEED, **m})
    coverage[name] = recsys.evaluate(recsys.with_fallback(r, pop), truth, seen)
mark_stage(NB, "test", ndcg10={r["model"]: r["ndcg@10"] for r in test_rows})
pd.DataFrame(test_rows)

# %% [markdown]
# ## Paired bootstrap for the pairs fixed in ADR-0007

# %%
pairs = [("history_als", "history"), ("history_als", "als"), ("history_als", "popularity"), ("history", "als")]
sig = []
for a, b in pairs:
    boot = recsys.paired_bootstrap(per_user[a], per_user[b], seed=SEED)
    sig.append({"a": a, "b": b, "seed": SEED, **boot, "verdict": recsys.verdict(boot)})
pd.DataFrame(sig)

# %%
out.update({"n_eval_users": n_eval, "n_common_users": n_common, "test": test_rows,
            "models_full_coverage_with_popularity_fallback": coverage, "significance": sig,
            "fit_seconds": {"als": als_seconds}})
print("saved", save_result(NB, out))
mark_stage(NB, "saved")
