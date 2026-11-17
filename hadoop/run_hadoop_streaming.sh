#!/usr/bin/env bash
# Runs the same mapper/reducer scripts on a real Hadoop installation (standalone/local mode on Kaggle,
# or pseudo-distributed HDFS on your own machine).   Needs Java 8/11/17 and internet access.
# Usage: hadoop/run_hadoop_streaming.sh <events.csv> <out_dir>
set -euo pipefail
IN=${1:?events csv}; OUT=${2:?out dir}; HV=3.3.6
D="$(cd "$(dirname "$0")" && pwd)/streaming"
HH=${HADOOP_HOME:-/tmp/hadoop-$HV}
if [ ! -d "$HH" ]; then
  curl -sSL "https://archive.apache.org/dist/hadoop/common/hadoop-$HV/hadoop-$HV.tar.gz" | tar -xz -C /tmp
fi
JAR=$(ls "$HH"/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1)
rm -rf "$OUT/job1" "$OUT/job2"; mkdir -p "$OUT"
"$HH/bin/hadoop" jar "$JAR" -D mapreduce.job.reduces=4 \
  -files "$D/basket_mapper.py,$D/pair_reducer.py" \
  -mapper "python3 basket_mapper.py" -reducer "python3 pair_reducer.py" \
  -input "$IN" -output "$OUT/job1"
"$HH/bin/hadoop" jar "$JAR" -D mapreduce.job.reduces=4 \
  -files "$D/identity_mapper.py,$D/sum_reducer.py" \
  -mapper "python3 identity_mapper.py" -combiner "python3 sum_reducer.py" -reducer "python3 sum_reducer.py" \
  -input "$OUT/job1" -output "$OUT/job2"
echo "pair counts in $OUT/job2/part-*"
