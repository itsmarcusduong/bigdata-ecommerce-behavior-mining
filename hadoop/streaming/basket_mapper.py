#!/usr/bin/env python3
"""Job 1 mapper: emit (user_session, product_id) for cart/purchase events."""
import sys

for line in sys.stdin:
    parts = line.rstrip("\n").split(",")
    if len(parts) < 9 or parts[0] == "event_time" or parts[1] not in ("cart", "purchase") or not parts[8]:
        continue
    print(f"{parts[8]}\t{parts[2]}")
