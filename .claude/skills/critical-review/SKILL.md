---
name: critical-review
description: Use before reporting any result, merging a results PR, or writing a conclusion in this project - review and challenge the work (recheck numbers, try to break the claim, look for leakage and unfair comparisons, compare several models under a rule fixed in advance) instead of assuming the first result is right.
---

# critical-review

A result is not done when the code runs. It is done when it has survived an honest attempt to break it.
Apply this before saying "done", before merging a `results/...` PR and before any sentence in the report.

## 1. Recheck (is the number what we think it is?)
- Open the JSON. Check `created_utc`, `fraction`, `base_fraction`: is this the run you think, not a stale copy?
- Sizes are plausible (5% run ~ 1/20 of full; counts grow with fraction); nothing is 0, NaN or identical across settings.
- Every number in text/slides is copied from a JSON key and cited. Recompute one derived number (ratio, %, diff) by hand.
- Cross-check two independent sources (Kaggle JSON vs a recomputation, report table vs JSON, this run vs the previous one).
- Open what you produced: render the figure, read the JSON, look at a few raw rows; do not trust a green test alone.

## 2. Try to break it (would a skeptical grader accept it?)
- **Leakage**: any structure (matrix, neighbours, popularity, thresholds, hyper-parameters) built from test data? Tuned on test?
- **Unfair comparison**: models scored on different users/items/periods, different candidate sets or budgets?
- **Too few cases**: recall from 3 pairs, a winner from 1 seed. Report n and an interval (Wilson, bootstrap, mean +/- std over seeds).
- **Defaults**: a library default (MLlib MinHashLSH tables, ALS rank) is a choice to test, not a fact.
- **Alternative explanations**: is the gain from the algorithm or from data size, caching, a lucky seed, sampling?
- **Adversarial pass**: write down 3 concrete ways the conclusion could be wrong, then check each one.
- **One group carries it?** Drop the largest contributor (top category, heaviest users, one seed) and recompute.
- **Mechanism**: a number that is too good or too bad needs a cause (sample size, leakage, how it is measured), not a story.
- **Downstream effect**: a better upstream metric (silhouette, recall of candidate pairs) does not prove a better final
  result; check the step that uses it.
- **Theory check**: sketches inside their bounds, PageRank sums to 1, A-Priori == PCY == FP-Growth, precision 1 after exact verification.

## 3. Compare models; never assume the winner
- Every question with a "which method is best" answer compares **at least 3 candidates**, including a simple strong
  baseline (popularity, recent history, exact/naive method) and the default the field would use.
- **Fix the selection rule before looking at test**: metric, validation split, tie rule. Example:
  "pick the highest validation NDCG@10; on test, call A better than B only if the paired-bootstrap 95% CI of A-B excludes 0,
  and only if it holds over >= 3 seeds; otherwise report a tie".
- Tune every candidate with comparable effort (each gets its own grid on validation), score all on the same users/items.
- Report the full table (all candidates, all seeds), the selected one and why, and the runner-up's gap with its interval.
- If the simple baseline wins or ties, say so; that is a result.

- The rule lives in an ADR merged to `main` before the run (the commit time is the evidence). After seeing results,
  do not edit it: anything judged by another criterion is labelled *exploratory* and goes into a new ADR for next time.

## 4. Improve, then re-review
- For each weakness found: fix it (more data for evaluation, a validation split, seeds), or state it as a limitation.
- After a fix, rerun the affected test/notebook and repeat steps 1-2 on the new output; do not reuse old conclusions.
- Write the review as a PR comment: what was checked, what was found, what changed, what is still open.
- Propose 1-3 measurable improvements for the next run, each with its rule written before that run.
- End the task with a short "Tự kiểm tra và phản biện" section; label claims [DOCUMENT] / [INFERENCE] / [DECISION].

## Red flags that block a merge
stale JSON - test-set tuning - different evaluation sets - n too small for the claim - single seed for "A beats B" -
a number in the report without a JSON key - a conclusion that ignores a contradicting result -
a selection rule edited after the results were seen - a PR with a new ADR/rule merged without the adversarial pass.
