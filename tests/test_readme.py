"""README.md must stay in step with the repo (CLAUDE.md invariant 12).

Cheap structural checks that fail CI when a PR adds a notebook, skill, tool or release without touching README.
They cannot check that the numbers are current: that part is the PR checklist ("README: không đổi" or updated).
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
README = (ROOT / "README.md").read_text(encoding="utf-8")


def test_every_notebook_is_listed():
    missing = [p.stem for p in sorted((ROOT / "notebooks" / "src").glob("[0-9][0-9]_*.py"))
               if f"notebooks/{p.stem}.ipynb" not in README]
    assert not missing, f"README notebook table lacks: {missing}"


def test_every_skill_is_listed():
    missing = [d.name for d in sorted((ROOT / ".claude" / "skills").iterdir()) if d.is_dir() and d.name not in README]
    assert not missing, f"README does not mention skills: {missing}"


def test_every_tool_is_listed():
    missing = [p.name for p in sorted((ROOT / "tools").glob("*.py")) if p.name not in README]
    assert not missing, f"README does not mention tools: {missing}"


def test_latest_release_is_named():
    def key(p):
        return tuple(int(x) for x in re.findall(r"\d+", p.stem))

    notes = sorted((ROOT / "docs" / "release_notes").glob("v*.md"), key=key)
    assert notes, "no release notes"
    assert notes[-1].stem in README, f"README does not name the latest release {notes[-1].stem}"
