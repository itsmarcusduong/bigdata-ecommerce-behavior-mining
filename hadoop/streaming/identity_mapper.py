#!/usr/bin/env python3
"""Job 2 mapper: pass (pair, 1) lines through unchanged."""
import sys

for line in sys.stdin:
    sys.stdout.write(line)
