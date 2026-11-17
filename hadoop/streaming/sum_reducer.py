#!/usr/bin/env python3
"""Generic Hadoop Streaming reducer/combiner: sum integer values per key (input sorted by key)."""
import sys

cur, total = None, 0
for line in sys.stdin:
    key, _, val = line.rstrip("\n").partition("\t")
    if key != cur:
        if cur is not None:
            print(f"{cur}\t{total}")
        cur, total = key, 0
    total += int(val)
if cur is not None:
    print(f"{cur}\t{total}")
