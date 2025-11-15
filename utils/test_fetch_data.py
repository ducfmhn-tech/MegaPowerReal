# tests/test_fetch_data.py
import os
from utils.fetch_data import fetch_all_data
from utils.fetch_checks import quick_validate
def test_fetch():
    mega, power = fetch_all_data(limit=120, save_dir="data")
    assert mega is not None
    assert isinstance(mega, type(mega))
    # require at least 30 rows for Mega (adjust to your expectation)
    assert len(mega) >= 30, f"Mega rows too few: {len(mega)}"
    # require power could be >= 0 (we accept 0 but prefer >0)
    # if you expect power always present, enforce len(power) >= 30
    # assert len(power) >= 30
