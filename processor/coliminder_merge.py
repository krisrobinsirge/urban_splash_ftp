from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


@dataclass(frozen=True)
class ColiminderColumns:
    timestamp: str = "Timestamp (UTC)"
    activity: str = "Activity"
    sample_numb: str = "Sample Numb."


COLIMINDER_COLUMNS = ColiminderColumns()


def _find_timestamp_column(columns: Iterable[str]) -> Optional[str]:
    candidates = [
        "TimeStamp",
        "Timestamp",
        "Time",
        "Time (UTC)",
        "Timestamp (UTC)",
        "Time_UTC",
    ]
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _parse_observator_timestamps(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def _parse_coliminder_uid(series: pd.Series) -> pd.Series:
    uid_numeric = pd.to_numeric(series, errors="coerce")
    dt = pd.to_datetime(uid_numeric, unit="s", utc=True, errors="coerce")
    return dt.dt.tz_convert(None)


def _format_coliminder_timestamp(value: datetime) -> str:
    return value.strftime("%d-%m-%Y %H:%M:%S")


def merge_coliminder_into_file(
    observator_path: str | Path,
    coliminder_path: str | Path | None,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    Append Coliminder columns to the Observator CSV and align by nearest timestamps.

    Returns True if any Coliminder rows were merged, otherwise False.
    """
    log = logger or logging.getLogger(__name__)
    obs_path = Path(observator_path)

    df = pd.read_csv(
        obs_path,
        dtype=str,
        keep_default_na=False,
        sep=None,
        engine="python",
    )

    original_columns = list(df.columns)
    new_columns = [COLIMINDER_COLUMNS.timestamp, COLIMINDER_COLUMNS.activity, COLIMINDER_COLUMNS.sample_numb]
    for column in new_columns:
        if column not in df.columns:
            df[column] = ""

    timestamp_col = _find_timestamp_column(df.columns)
    if timestamp_col is None:
        log.warning("No timestamp column found in %s; leaving Coliminder columns empty.", obs_path)
        _write_with_order(df, obs_path, original_columns, new_columns)
        return False

    obs_times = _parse_observator_timestamps(df[timestamp_col])
    valid_obs = obs_times.dropna()
    if valid_obs.empty:
        log.warning("No valid timestamps found in %s; leaving Coliminder columns empty.", obs_path)
        _write_with_order(df, obs_path, original_columns, new_columns)
        return False

    if coliminder_path is None:
        log.info("No Coliminder file provided; leaving Coliminder columns empty.")
        _write_with_order(df, obs_path, original_columns, new_columns)
        return False

    col_path = Path(coliminder_path)
    try:
        col_df = pd.read_csv(
            col_path,
            dtype=str,
            keep_default_na=False,
            sep=";",
        )
    except Exception as exc:
        log.warning("Failed to read Coliminder CSV %s: %s", col_path, exc)
        _write_with_order(df, obs_path, original_columns, new_columns)
        return False

    uid_col = "UID" if "UID" in col_df.columns else None
    activity_col = "mU" if "mU" in col_df.columns else None
    sample_col = "activeSample" if "activeSample" in col_df.columns else None
    if uid_col is None or activity_col is None or sample_col is None:
        log.warning(
            "Coliminder CSV %s missing required columns (UID, mU, activeSample).",
            col_path,
        )
        _write_with_order(df, obs_path, original_columns, new_columns)
        return False

    col_times = _parse_coliminder_uid(col_df[uid_col])
    col_df = col_df.assign(_col_time=col_times)
    col_df = col_df[col_df["_col_time"].notna()]

    start_time = valid_obs.min()
    end_time = valid_obs.max()
    col_df = col_df[(col_df["_col_time"] >= start_time) & (col_df["_col_time"] <= end_time)]

    if col_df.empty:
        log.info("No Coliminder rows within %s to %s; leaving columns empty.", start_time, end_time)
        _write_with_order(df, obs_path, original_columns, new_columns)
        return False

    assignments: dict[int, tuple[pd.Timestamp, str, str]] = {}
    diffs: dict[int, pd.Timedelta] = {}
    for _, row in col_df.iterrows():
        col_time = row["_col_time"]
        if pd.isna(col_time):
            continue
        time_diffs = (valid_obs - col_time).abs()
        if time_diffs.empty:
            continue
        target_idx = time_diffs.idxmin()
        diff = time_diffs.loc[target_idx]
        if target_idx in diffs and diff >= diffs[target_idx]:
            continue
        diffs[target_idx] = diff
        assignments[target_idx] = (
            col_time,
            str(row.get(activity_col, "")),
            str(row.get(sample_col, "")),
        )

    for idx, (col_time, activity, sample) in assignments.items():
        df.at[idx, COLIMINDER_COLUMNS.timestamp] = _format_coliminder_timestamp(col_time)
        df.at[idx, COLIMINDER_COLUMNS.activity] = activity
        df.at[idx, COLIMINDER_COLUMNS.sample_numb] = sample

    _write_with_order(df, obs_path, original_columns, new_columns)
    return bool(assignments)


def _write_with_order(df: pd.DataFrame, path: Path, original_columns: list[str], new_columns: list[str]) -> None:
    final_columns = [col for col in original_columns if col not in new_columns] + new_columns
    df = df.reindex(columns=final_columns)
    df.to_csv(path, index=False)
