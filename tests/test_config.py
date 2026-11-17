import os
import zipfile
from pathlib import Path

import pytest

from bdm import config


def test_find_file_in_data_dir():
    p = config.find_file("2019-Oct.csv")
    assert Path(p).name == "2019-Oct.csv" and os.path.getsize(p) > 0


def test_find_file_unzips_when_only_zip_exists(tmp_path, monkeypatch):
    src = tmp_path / "in" / "nested"
    src.mkdir(parents=True)
    with zipfile.ZipFile(src / "only_zipped.csv.zip", "w") as zf:
        zf.writestr("only_zipped.csv", "a,b\n1,2\n")
    monkeypatch.setattr(config, "WORK_DIR", tmp_path / "work")
    p = config.find_file("only_zipped.csv", extra_roots=[str(tmp_path / "in")])
    assert Path(p).read_text().startswith("a,b")


def test_find_file_missing_raises():
    with pytest.raises(FileNotFoundError):
        config.find_file("does_not_exist.csv")


def test_find_dir(tmp_path):
    (tmp_path / "x" / "events_parquet").mkdir(parents=True)
    assert config.find_dir("events_parquet", extra_roots=[str(tmp_path)]).endswith("events_parquet")
    assert config.find_dir("nope_dir") is None
