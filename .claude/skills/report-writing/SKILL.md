---
name: report-writing
description: Use when writing the final report, slides or README results for this project - structure per rubric, citing results/*.json, figures, honest limitations.
---

# report-writing

Structure: `docs/REPORT_OUTLINE.md`. Grading rubric: `docs/RUBRIC_MAPPING.md` (Technical 40, Dataset 20, Results 20, Innovation 10, Presentation 10).

## Process
1. Read `results/*.json` and `docs/RESULTS.md`; list which RQs have real numbers. Missing = say "not yet run", never estimate.
2. For each RQ write: question -> method (1 paragraph, complexity, parameters) -> setup (baseline, split, metric) -> **table** -> **figure** -> 3-5 sentences of analysis (what, why, caveat).
3. Every number cites its source (`results/04_lsh_similarity.json: lsh_vs_naive[1].recall`). Round consistently; show units (seconds, MB, rows).
4. Figures come from `results/figures/` (title, axis labels, units; readable in grayscale).
5. A "Limitations" section is mandatory: single store, 2019 data, purchase sparsity, one-machine Spark (no real cluster), what Hadoop evidence exists, threats to validity (sampling by user, thresholds).
6. Slides: 1 message per slide, the RQ -> answer, one demo (similar products or recommendations), and an honest benchmark slide.

## Checks before submitting
* Dataset sizes quoted match `01_ingest_eda.json`.
* Claims of "better" have baselines and, where possible, spread over seeds/samples.
* Reproducibility paragraph: repo URL, tag, Kaggle notebook versions, commands (`make test`, `make smoke`), software versions.
