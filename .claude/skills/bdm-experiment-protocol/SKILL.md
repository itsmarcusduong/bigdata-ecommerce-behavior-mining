---
name: bdm-experiment-protocol
description: Use when designing, running or reporting any experiment in this project (sketch accuracy, PCY vs A-Priori, LSH recall, PageRank, recommender, benchmark) - enforces baselines, temporal splits, metrics, ablations, error analysis and honest reporting.
---

# bdm-experiment-protocol

Every experiment answers one research question (RQ1-RQ6 in `PROJECT.md`) with evidence a grader can re-derive.

## Before running
1. **State the hypothesis and the metric** in the GitHub issue (template *Experiment*): "PCY counts < 50% of A-Priori's candidate pairs at support 0.2%".
2. **Pick baselines that are honest**: exact answer (Spark groupBy), naive all-pairs, A-Priori, popularity, item-kNN. A method without a baseline is not a result.
3. **Fix the protocol**: seeds, `FRACTION`, cutoff date, hyper-parameter grid. Write them into the result JSON.
4. **Leakage check** (recommender): train strictly before the cutoff; item neighbours, popularity lists, ALS matrix, co-occurrence graph use train only;
   ground truth = purchases of users/items known in train; already-bought items are removed from recommendations before scoring.

## While running
* Smoke first (`make smoke`), then Kaggle `FRACTION=0.01`, `0.1`, `1.0`.
* Record wall-clock per stage (`Timer`), data sizes (rows, users, products, edges, baskets), and Spark config.
* Compare against a **reference implementation** where possible (networkx for PageRank/HITS, brute force for itemsets, exact Jaccard for LSH).

## Metrics (use these names)
| Area | Metrics |
|---|---|
| Sketches | top-k overlap, mean relative error, % within eps*N, empirical FPR vs theory, bytes of memory |
| Itemsets | #frequent items/pairs/itemsets, candidates counted (A-Priori vs PCY), runtime, rule support/confidence/lift |
| LSH | recall and precision vs exact pairs, #candidates, runtime, S-curve P(candidate\|s=threshold) |
| Graph | convergence (L1 delta), Spearman vs views/purchases, top-k overlap, topic mass |
| Recsys | Precision/Recall/HitRate/MAP/NDCG @10,@20, n_users evaluated, coverage if computed |
| Scalability | runtime vs size (log-log exponent), speed-up vs cores, throughput rows/s |

## Model selection (never assume the best model)
1. List >= 3 candidates per question, including a simple strong baseline and the library default.
2. Pre-register the rule in the issue/notebook header: validation metric, validation split (never the test period),
   tie rule (A > B only if the paired-bootstrap 95% CI excludes 0 and the sign holds over >= 3 seeds).
3. Give each candidate its own small grid on validation; refit the chosen setting on all training data; score once on test.
4. Report every candidate on the same users/items with intervals, the selected model and the runner-up gap.
   A tie or a baseline win is reported as such.

## After running
1. **Ablation**: change one factor at a time (ALS rank/alpha, behaviour signals, (b, r), PCY buckets, damping).
2. **Error analysis**: where does it fail (cold users, long-tail products, near-threshold pairs, tail of the CMS error)? Put one table/plot in the notebook.
3. **Uncertainty**: repeat with >=3 seeds or `FRACTION` samples for any claim of "A beats B"; report mean +/- std or a bootstrap CI over users.
4. **Report honestly**: negative or mixed results are fine - explain them. Never tune on the test period; never present synthetic numbers as results.
5. Run the `critical-review` skill (recheck, try to break, compare, re-review) before merging.
6. Save `results/<nn>_<name>.json` + `results/figures/*.png`; update `docs/RESULTS.md`; every number in the report must cite a JSON key.
