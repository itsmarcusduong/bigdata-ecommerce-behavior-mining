#!/usr/bin/env bash
# Emulates Hadoop Streaming with Unix pipes (map | sort | reduce) - same scripts, no cluster needed.
# Usage: hadoop/run_local_mapreduce.sh <events.csv> <out_dir>
set -euo pipefail
IN=${1:?events csv}; OUT=${2:?out dir}; D="$(cd "$(dirname "$0")" && pwd)/streaming"
mkdir -p "$OUT"
cat "$IN" | python3 "$D/count_mapper.py" | LC_ALL=C sort | python3 "$D/sum_reducer.py" > "$OUT/event_counts.tsv"
cat "$IN" | python3 "$D/basket_mapper.py" | LC_ALL=C sort | python3 "$D/pair_reducer.py" \
  | LC_ALL=C sort | python3 "$D/sum_reducer.py" > "$OUT/pair_counts.tsv"
echo "wrote $OUT/event_counts.tsv and $OUT/pair_counts.tsv"
