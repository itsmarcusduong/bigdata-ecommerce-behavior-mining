---
name: cs246-algorithms
description: Use when implementing, reviewing or testing course algorithms in src/bdm - Count-Min, Bloom, DGIM, MinHash/LSH banding, A-Priori/PCY, FP-Growth, PageRank (dead ends, personalization), HITS, ALS - for formulas, parameter rules and common pitfalls.
---

# cs246-algorithms

Where: `sketches.py`, `lsh.py`, `fpm.py`, `graph.py`, `recsys.py`. Each has a test with a reference; keep it that way.

## Streaming (`sketches.py`)
* **Count-Min**: width `w = ceil(e/eps)`, depth `d = ceil(ln(1/delta))`; estimate = min over rows; never underestimates; error <= eps*N w.p. >= 1-delta. Mergeable (add tables) -> build per partition, `reduce(merge)`. Heavy hitters need a candidate set (a sketch alone cannot enumerate keys).
* **Bloom**: `m = -n ln p / (ln 2)^2`, `k = (m/n) ln 2`; no false negatives; FPR ~ `(1 - e^{-kn/m})^k`; mergeable by OR.
* **DGIM**: <=2 buckets per size; estimate = sum of all buckets except the oldest + half of the oldest; relative error <= 50%; space O(log^2 N).
* Hash with `(a*x+b) mod P` for prime P=2^31-1 (stays in int64). Never Python `hash()` across processes.

## Similar items (`lsh.py`)
* MinHash signature of a set = min of `h_i(x)` over members; `P[sig_i equal] = Jaccard`.
* Banding with `b` bands x `r` rows: `P(candidate) = 1-(1-s^r)^b`, inflection ~ `(1/b)^(1/r)`. Choose (b, r) for **recall at the threshold** (`best_bands_rows(n, t, min_prob)`), then verify candidates exactly -> precision 1.
* Pitfalls: huge buckets (cap with `max_bucket`), bucket keys must be deterministic (crc32), items with empty sets, comparing the same pair twice (use `a<b`).

## Frequent itemsets (`fpm.py`)
* A-Priori: counts only pairs of frequent items. **PCY**: pass 1 also hashes all pairs into buckets; pass 2 counts a pair only if both items frequent **and** its bucket frequent. Result identical to A-Priori; candidates fewer.
* Support rounding must match MLlib: `minCount = ceil(minSupport * N)`. Baskets must hold *distinct* items (FP-Growth errors on duplicates). Cap basket size (pairs grow O(n^2)).
* Rules: confidence `P(B|A)`, lift `conf / P(B)`; look at lift and support together.

## Link analysis (`graph.py`)
* PageRank: `r' = beta * (M r + dangling_mass * t) + (1-beta) * t`; dead ends redistribute along the teleport vector `t` (uniform or personalised) - same convention as networkx. Check `sum(r) == 1`. Spider traps are handled by teleport.
* Topic-sensitive PR: `t` concentrated on topic nodes. HITS: `a = L^T h`, `h = L a`, normalise each step (L2 here; networkx uses L1).
* Truncate lineage every iteration (`localCheckpoint`) or the DAG explodes.

## Recommendation (`recsys.py`)
* Implicit ALS (Hu-Koren-Volinsky): preference `p = 1[r>0]`, confidence `c = 1 + alpha*r`; `rating` here is `log1p(sum weight)`; tune rank, reg, alpha.
* ALS needs int32 ids (`check_int_ids`). Remove already-bought items before scoring. Cold users/items are dropped (`coldStartStrategy="drop"`) - report how many.

## Testing rule
New/changed algorithm => add/adjust a test that compares with an independent reference on a small random instance, and a property check (never-underestimates, sum=1, same result as A-Priori, precision=1 after verification).
