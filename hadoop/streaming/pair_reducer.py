#!/usr/bin/env python3
"""Job 1 reducer: build the basket of one session and emit every product pair (a<b) with count 1.

Baskets larger than MAX_BASKET are skipped (same rule as the Spark implementation).
Output feeds Job 2 (sum_reducer) which yields the global pair counts = frequent-pair candidates.
"""
import sys
from itertools import combinations

MAX_BASKET = 50


def flush(items):
    items = sorted(set(items))
    if 2 <= len(items) <= MAX_BASKET:
        for a, b in combinations(items, 2):
            print(f"{a},{b}\t1")


cur, items = None, []
for line in sys.stdin:
    key, _, val = line.rstrip("\n").partition("\t")
    if key != cur:
        if cur is not None:
            flush(items)
        cur, items = key, []
    items.append(int(val))
if cur is not None:
    flush(items)
