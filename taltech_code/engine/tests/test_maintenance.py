import os
import pandas as pd

from src.maintenance import flag_maintenance, load_maintenance_periods


def test_maintenance_flag_matches_diary_periods():
    diary_path = os.path.join("data_raw", "Anne kanal diary.csv")
    periods = load_maintenance_periods(diary_path)
    timestamps = pd.Series(["31/10/2025 12:00", "01/11/2025 12:00"])
    flags = flag_maintenance(timestamps, periods, metadata_index=None)
    assert flags == ["FAIL", "PASS"]
