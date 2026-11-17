"""Build Kaggle-ready .ipynb files from the jupytext-style sources in notebooks/src/*.py.

    python tools/build_notebooks.py            # (re)generate notebooks/*.ipynb
    python tools/build_notebooks.py --execute  # also run them top-to-bottom (needs data, see docs)

Every notebook starts with the same bootstrap cell (HEADER) that makes ``bdm`` importable, locally
or on Kaggle (where the repo is cloned; add a GITHUB_TOKEN Kaggle secret if the repo is private).
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parents[1]
SRC, OUT = ROOT / "notebooks" / "src", ROOT / "notebooks"

HEADER = '''import os, subprocess, sys

REPO_URL = "https://github.com/itsmarcusduong/bigdata-ecommerce-behavior-mining.git"
if os.path.isdir("src/bdm"):          # local run from repo root / CI
    sys.path.insert(0, os.path.abspath("src"))
elif os.path.isdir("../src/bdm"):     # local run from notebooks/
    sys.path.insert(0, os.path.abspath("../src"))
else:                                 # Kaggle: clone the repo (needs Internet ON)
    if not os.path.isdir("/kaggle/working/repo"):
        url = REPO_URL
        try:
            from kaggle_secrets import UserSecretsClient
            url = REPO_URL.replace("https://", f"https://{UserSecretsClient().get_secret('GITHUB_TOKEN')}@")
        except Exception:
            pass                      # public repo or no secret configured
        subprocess.run(["git", "clone", "--depth", "1", url, "/kaggle/working/repo"], check=True)
    sys.path.insert(0, "/kaggle/working/repo/src")

# ---- run parameters (override with environment variables) -------------------------------------
FRACTION = float(os.environ.get("BDM_FRACTION", "0.05"))   # share of USERS to keep; 1.0 = full data
SPARK_MEM = os.environ.get("BDM_SPARK_MEM", "20g")          # Kaggle CPU session has ~29 GB RAM
os.environ.setdefault("BDM_SHUFFLE_PARTITIONS", "200")
import matplotlib
matplotlib.use(os.environ.get("MPLBACKEND", "module://matplotlib_inline.backend_inline")
               if "ipykernel" in sys.modules else "Agg")
import matplotlib.pyplot as plt
import pandas as pd
pd.set_option("display.width", 160); pd.set_option("display.max_columns", 30)
print("FRACTION =", FRACTION, "| SPARK_MEM =", SPARK_MEM)
'''


def parse(text: str):
    cells, kind, buf = [], "code", []

    def flush():
        body = "\n".join(buf).strip("\n")
        if not body.strip():
            return
        if kind == "markdown":
            body = "\n".join(re.sub(r"^# ?", "", ln) for ln in body.splitlines())
            cells.append(nbf.v4.new_markdown_cell(body))
        else:
            cells.append(nbf.v4.new_code_cell(body))

    for line in text.splitlines():
        m = re.match(r"^# %%(\s*\[markdown\])?\s*$", line)
        if m:
            flush()
            kind, buf = ("markdown" if m.group(1) else "code"), []
        else:
            buf.append(line)
    flush()
    return cells


def build() -> list[Path]:
    OUT.mkdir(exist_ok=True)
    made = []
    for src in sorted(SRC.glob("[0-9]*.py")):
        nb = nbf.v4.new_notebook()
        cells = parse(src.read_text())
        nb.cells = [cells[0], nbf.v4.new_code_cell(HEADER)] + cells[1:]
        nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
        dst = OUT / (src.stem + ".ipynb")
        nbf.write(nb, dst)
        made.append(dst)
    return made


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    paths = [p for p in build() if a.only in p.name]
    print("built", *[p.name for p in paths], sep="\n  ")
    if a.execute:
        from nbclient import NotebookClient
        for p in paths:
            nb = nbf.read(p, as_version=4)
            NotebookClient(nb, timeout=7200, kernel_name="python3",
                           resources={"metadata": {"path": str(ROOT)}}).execute()
            print("executed OK:", p.name)
