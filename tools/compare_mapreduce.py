"""Compare Hadoop Streaming output with the Unix-pipe emulation of the same mapper/reducer scripts.

Writes results/hadoop/hadoop_vs_pipe.json. Both sides count exactly, so any difference is a bug.
Usage: python tools/compare_mapreduce.py <hadoop_out_dir> <pipe_out_dir> <out_json> [extra key=value ...]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def read_counts(paths) -> dict[str, int]:
    out: dict[str, int] = {}
    for p in paths:
        for line in Path(p).read_text().splitlines():
            if line.strip():
                key, val = line.rsplit("\t", 1)
                out[key] = out.get(key, 0) + int(val)
    return out


def main() -> int:
    hadoop, pipe, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    extra = dict(kv.split("=", 1) for kv in sys.argv[4:])
    report = {"experiment": "hadoop_vs_pipe", **extra}
    ok = True
    jobs = [("event_counts", "counts", "event_counts.tsv"), ("pair_counts", "job2", "pair_counts.tsv")]
    for name, hdir, pfile in jobs:
        h = read_counts(sorted((hadoop / hdir).glob("part-*")))
        p = read_counts([pipe / pfile])
        same = h == p
        ok &= same
        report[name] = {"keys_hadoop": len(h), "keys_pipe": len(p), "total_hadoop": sum(h.values()),
                        "total_pipe": sum(p.values()), "identical": same}
    report["identical"] = ok
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
