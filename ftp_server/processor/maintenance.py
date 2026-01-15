from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from ftp_server.processor.qc_checks import parse_timestamp, PASS, FAIL


@dataclass
class MaintenancePeriod:
    start: pd.Timestamp
    end: pd.Timestamp


def load_maintenance_periods(diary_path: str) -> List[MaintenancePeriod]:
    try:
        diary_df = pd.read_csv(diary_path, dtype=str, keep_default_na=False)
    except FileNotFoundError:
        return []

    periods: List[MaintenancePeriod] = []
    for _, row in diary_df.iterrows():
        exclude_value = str(row.get("Exclude from Analysis (Yes/No)", "")).strip().lower()
        if exclude_value != "yes":
            continue

        start_raw = row.get("Date (Start) UTC", "")
        end_raw = row.get("Date (End) UTC", "")
        start_ts = parse_timestamp(start_raw, fmt="%d/%m/%Y %H:%M")
        end_ts = parse_timestamp(end_raw, fmt="%d/%m/%Y %H:%M")
        if start_ts is None:
            continue
        if end_ts is None:
            end_ts = start_ts
        periods.append(MaintenancePeriod(start=start_ts, end=end_ts))

    return periods


def flag_maintenance(timestamps: pd.Series, periods: List[MaintenancePeriod], metadata_index: Optional[int]) -> List[str]:
    flags: List[str] = []
    for idx, value in timestamps.items():
        if metadata_index is not None and idx == metadata_index:
            flags.append("")
            continue
        ts = parse_timestamp(value)
        if ts is None or not periods:
            flags.append(PASS)
            continue
        in_period = any(period.start <= ts <= period.end for period in periods)
        flags.append(FAIL if in_period else PASS)
    return flags
