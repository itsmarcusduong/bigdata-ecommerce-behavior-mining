#!/usr/bin/env python3
"""Hadoop Streaming mapper: (product_id, event_type) -> 1  (WordCount-style event counting)."""
import sys

for i, line in enumerate(sys.stdin):
    parts = line.rstrip("\n").split(",")
    if len(parts) < 9 or parts[0] == "event_time":
        continue
    print(f"{parts[2]}:{parts[1]}\t1")
