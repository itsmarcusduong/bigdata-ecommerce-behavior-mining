"""Tiny experiment-tracking helpers: timers and JSON result files (one per experiment)."""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from . import config


class Timer:
    """Collects named wall-clock timings: ``with timer("step"): ...``."""

    def __init__(self) -> None:
        self.times: dict[str, float] = {}

    @contextmanager
    def __call__(self, name: str):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self.times[name] = round(time.perf_counter() - t0, 3)


def save_result(name: str, payload: dict) -> str:
    """Write ``results/<name>.json`` with a timestamp; returns the path."""
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = config.RESULTS_DIR / f"{name}.json"
    body = {"experiment": name, "created_utc": datetime.now(timezone.utc).isoformat(), **payload}
    out.write_text(json.dumps(body, indent=2, default=str))
    return str(out)


def save_fig(fig, name: str) -> str:
    """Save a matplotlib figure to ``results/figures/<name>.png`` (also displayed in notebooks)."""
    out_dir = config.RESULTS_DIR / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{name}.png"
    fig.savefig(out, dpi=130, bbox_inches="tight")
    return str(out)
