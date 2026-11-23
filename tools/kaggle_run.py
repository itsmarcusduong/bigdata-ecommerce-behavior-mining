"""Run the project notebooks on Kaggle through the official Kaggle API (``pip install kaggle``).

Why: avoids clicking through the web UI. Authentication is read by the kaggle client from the
``KAGGLE_API_TOKEN`` environment variable or ``~/.kaggle/access_token`` - never put a token in the repo.

Usage (from the repo root):
    python tools/kaggle_run.py push 01 --fraction 0.05   # upload + start notebook 01 on a small user sample
    python tools/kaggle_run.py status 01                 # QUEUED / RUNNING / COMPLETE / ERROR
    python tools/kaggle_run.py output 01                 # download results/*.json + figures to results/
    python tools/kaggle_run.py push 02 03 04 --fraction 1.0

Notebooks 02-08 read the Parquet written by notebook 01, so 01 must be COMPLETE first (the metadata
attaches it as a ``kernel_sources`` input). The numbers in results/ come only from these real runs.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
USER = "minhquangdng"
REES46 = "mkechinov/ecommerce-behavior-data-from-multi-category-store"
INSTACART = "psparks/instacart-market-basket-analysis"
# notebook id -> (file stem, title, extra dataset sources)
NOTEBOOKS = {
    "01": ("01_ingest_eda", "BDM 01 Ingest EDA", [REES46]),
    "02": ("02_streaming_sketches", "BDM 02 Streaming Sketches", []),
    "03": ("03_frequent_itemsets", "BDM 03 Frequent Itemsets", [INSTACART]),
    "04": ("04_lsh_similarity", "BDM 04 LSH Similarity", []),
    "05": ("05_link_analysis", "BDM 05 Link Analysis", []),
    "06": ("06_recommender", "BDM 06 Recommender", []),
    "07": ("07_segmentation_funnel", "BDM 07 Segmentation Funnel", [INSTACART]),
    "08": ("08_scalability_benchmark", "BDM 08 Scalability Benchmark", []),
    "09": ("09_hadoop_real", "BDM 09 Hadoop Real", [REES46]),
    "10": ("10_recsys_grid_extension", "BDM 10 Recsys Grid Extension", []),
}


def slug(nb: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", NOTEBOOKS[nb][1].lower()).strip("-")


def metadata(nb: str, code_file: str) -> dict:
    stem, title, datasets = NOTEBOOKS[nb]
    return {
        "id": f"{USER}/{slug(nb)}",
        "title": title,
        "code_file": code_file,
        "language": "python",
        "kernel_type": "notebook",
        "is_private": True,
        "enable_gpu": False,
        "enable_tpu": False,
        "enable_internet": True,  # the header cell clones this repo
        "dataset_sources": datasets,
        "competition_sources": [],
        "kernel_sources": [] if nb in ("01", "09") else [f"{USER}/{slug('01')}"],  # 09 reads the raw CSV
    }


def set_params(code: str, fraction: float, mem: str) -> tuple[str, int]:
    """Replace the defaults of BDM_FRACTION / BDM_SPARK_MEM in ``code``; returns (code, number of matches)."""
    code, n1 = re.subn(r"""(BDM_FRACTION['"]\s*,\s*['"])[^'"]*(['"])""", rf"\g<1>{fraction}\g<2>", code)
    code, n2 = re.subn(r"""(BDM_SPARK_MEM['"]\s*,\s*['"])[^'"]*(['"])""", rf"\g<1>{mem}\g<2>", code)
    return code, n1 + n2


def kaggle(*args: str, retries: int = 6, wait_s: int = 60) -> int:
    """Run the kaggle CLI, retrying on rate limits (HTTP 429) and on the batch CPU session limit."""
    for attempt in range(1, retries + 1):
        res = subprocess.run(["kaggle", *args], capture_output=True, text=True, check=False)
        out = res.stdout + res.stderr
        print(out.strip(), flush=True)
        busy = "429" in out or "Too Many Requests" in out or "session count" in out
        if not busy and res.returncode == 0 and "error" not in out.lower():
            return 0
        if not busy:
            return res.returncode or 1
        if attempt < retries:
            pause = min(wait_s * attempt, 600)
            print(f"Kaggle busy/rate-limited, retry {attempt}/{retries - 1} in {pause}s", flush=True)
            time.sleep(pause)
    return 1


def push(nb: str, fraction: float, mem: str) -> int:
    stem = NOTEBOOKS[nb][0]
    src = ROOT / "notebooks" / f"{stem}.ipynb"
    with tempfile.TemporaryDirectory() as tmp:
        nbk = json.loads(src.read_text(encoding="utf-8"))
        # Kaggle's API cannot pass env vars, so bake the run parameters into the copy that is uploaded.
        hits = 0
        for cell in nbk["cells"]:
            body = cell["source"] if isinstance(cell["source"], str) else "".join(cell["source"])
            new, n = set_params(body, fraction, mem)
            if n:
                cell["source"], hits = new.splitlines(keepends=True), hits + n
        if hits < 1:
            seen = [ln for c in nbk["cells"] for ln in "".join(c["source"]).splitlines() if "BDM_" in ln]
            sys.exit(f"BDM_FRACTION line not found in {src} (run git pull). Lines with BDM_: {seen[:5]}")
        text = json.dumps(nbk, indent=1)
        (Path(tmp) / src.name).write_text(text, encoding="utf-8")
        (Path(tmp) / "kernel-metadata.json").write_text(json.dumps(metadata(nb, src.name), indent=2))
        return kaggle("kernels", "push", "-p", tmp, retries=20)


def wait(nb: str, timeout_min: float, poll_s: int = 60) -> int:
    """Poll ``kaggle kernels status`` until the run finishes; 0 = complete, 1 = error/cancelled, 2 = timeout."""
    deadline = time.time() + timeout_min * 60
    while True:
        res = subprocess.run(["kaggle", "kernels", "status", f"{USER}/{slug(nb)}"],
                             capture_output=True, text=True, check=False)
        msg = (res.stdout + res.stderr).strip()
        low = msg.lower()
        print(time.strftime("%H:%M:%S"), msg, flush=True)
        if "complete" in low:
            return 0
        if "error" in low or "cancel" in low:
            return 1
        if time.time() > deadline:
            print(f"still running after {timeout_min} min - fetch later with: output {nb}")
            return 2
        time.sleep(poll_s)


def print_log_tail(path: Path, n: int = 60) -> None:
    """Print the end of a Kaggle kernel log (a JSON list of {stream_name, data}) so errors show in CI logs."""
    try:
        entries = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        text = "".join(e.get("data", "") for e in entries if isinstance(e, dict))
    except (json.JSONDecodeError, OSError):
        text = path.read_text(encoding="utf-8", errors="replace")
    print(f"----- last {n} lines of {path.name} -----")
    print("\n".join(text.splitlines()[-n:]))
    print("-----")


def output(nb: str) -> int:
    dest = ROOT / "results" / "_kaggle" / slug(nb)
    shutil.rmtree(dest, ignore_errors=True)
    dest.mkdir(parents=True)
    # only results and logs: the events_parquet output of notebook 01 is several GB
    rc = kaggle("kernels", "output", f"{USER}/{slug(nb)}", "-p", str(dest), "-o",
                "--file-pattern", r".*(\.json|\.png|\.log|\.ipynb)$")
    # Only the notebook's own outputs (/kaggle/working/results). The notebook also clones this repo into
    # /kaggle/working/repo, whose committed results/*.json are stale copies and must never be picked up.
    out_dir = dest / "results"
    for js in out_dir.glob("*.json"):
        (ROOT / "results").mkdir(exist_ok=True)
        shutil.copy(js, ROOT / "results" / js.name)
    for log in (out_dir / "hadoop").glob("real_*.log"):
        (ROOT / "results" / "hadoop").mkdir(parents=True, exist_ok=True)
        shutil.copy(log, ROOT / "results" / "hadoop" / log.name)
    for png in (out_dir / "figures").glob("*.png"):
        (ROOT / "results" / "figures").mkdir(parents=True, exist_ok=True)
        shutil.copy(png, ROOT / "results" / "figures" / png.name)
    for log in dest.glob("*.log"):
        print_log_tail(log)
    print(f"downloaded to {dest}; copied results/*.json and figures (review before committing)")
    return rc


def meta(nb: str) -> int:
    """Save the Kaggle API metadata (incl. license) of every dataset the notebook reads, with the access date.

    Evidence for the license claim in docs/DATASETS.md comes from the source itself, not from a secondary page.
    """
    from datetime import datetime, timezone

    rc = 0
    for ref in NOTEBOOKS[nb][2]:
        dest = ROOT / "results" / "licenses" / ref.replace("/", "__")
        dest.mkdir(parents=True, exist_ok=True)
        rc |= kaggle("datasets", "metadata", "-p", str(dest), ref)
        (dest / "accessed_utc.txt").write_text(datetime.now(timezone.utc).isoformat() + "\n")
        for f in dest.glob("*.json"):
            try:
                body = json.loads(f.read_text())
                info = body.get("info", body) if isinstance(body, dict) else {}
                lic = info.get("licenses") or info.get("licenseName")
                print(f"{ref}: licenses = {lic}")
            except (ValueError, AttributeError):
                print(f"{ref}: could not parse {f.name}")
    return rc


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["push", "status", "wait", "output", "meta"])
    ap.add_argument("notebooks", nargs="+", choices=sorted(NOTEBOOKS))
    ap.add_argument("--fraction", type=float, default=0.05, help="share of USERS to keep (1.0 = full data)")
    ap.add_argument("--mem", default="20g", help="Spark driver memory")
    ap.add_argument("--timeout-min", type=float, default=330, help="wait: give up after this many minutes")
    a = ap.parse_args()
    if shutil.which("kaggle") is None:
        sys.exit("kaggle CLI not found: pip install kaggle (and set KAGGLE_API_TOKEN)")
    rc = 0
    for nb in a.notebooks:
        if a.action == "push":
            rc |= push(nb, a.fraction, a.mem)
        elif a.action == "wait":
            rc |= wait(nb, a.timeout_min)
        elif a.action == "status":
            rc |= kaggle("kernels", "status", f"{USER}/{slug(nb)}")
        elif a.action == "meta":
            rc |= meta(nb)
        else:
            rc |= output(nb)
    return rc


if __name__ == "__main__":
    sys.exit(main())
