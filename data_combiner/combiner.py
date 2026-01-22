"""Combine Observator and ColiMinder cleaned CSV files, then build aligned outputs."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from processor.file_funcs import CLEANED_DIR, COMBINED_DIR, OUTPUT_DATA_DIR

PROJECT_ROOT = Path(__file__).resolve().parents[1]

COLUMN_ORDER: List[str] = [
    "TimeStamp",
    "Origin",
    "Activity - Coliminder",
    "BGA PC RFU",
    "BGA PC ug/L",
    "Chlorophyll RFU",
    "Chlorophyll ug/L",
    "Cond uS/cm",
    "fDOM QSU",
    "fDOM RFU",
    "microFlu_TRP",
    "pH",
    "Signal strength",
    "SpCond uS/cm",
    "Temp C",
    "Turbidity",
]

UNIT_ROW = {
    "TimeStamp": "dd-mm-yyyy hh:mm:ss",
    "Origin": "",
    "Activity - Coliminder": "mMFU/100ml",
    "BGA PC RFU": "RFU",
    "BGA PC ug/L": "ug/L",
    "Chlorophyll RFU": "RFU",
    "Chlorophyll ug/L": "ug/L",
    "Cond uS/cm": "uS/cm",
    "fDOM QSU": "QSU",
    "fDOM RFU": "RFU",
    "microFlu_TRP": "g/L",
    "pH": "pH",
    "Signal strength": "dBm",
    "SpCond uS/cm": "uS/cm",
    "Temp C": "C",
    "Turbidity": "NTU",
}

OBS_PATTERN = re.compile(r"cleaned_data_Observator_(\d{8})_to_(\d{8})\.csv$")
COLI_PATTERN = re.compile(r"cleaned_data_ColiMinder_(\d{8})_to_(\d{8})\.csv$")

ORIGIN_OBS = "Observator"
ORIGIN_COLI = "Coliminder"

OBS_COLUMNS = [
    "TimeStamp",
    "BGA PC RFU",
    "BGA PC ug/L",
    "Chlorophyll RFU",
    "Chlorophyll ug/L",
    "Cond uS/cm",
    "fDOM QSU",
    "fDOM RFU",
    "microFlu_TRP",
    "pH",
    "Signal strength",
    "SpCond uS/cm",
    "Temp C",
    "Turbidity",
]

OBS_TIMESTAMP_COL = "Observator TimeStamp"
COLI_TIMESTAMP_COL = "Coliminder TimeStamp"
MEASUREMENT_COLUMNS = [c for c in COLUMN_ORDER if c not in {"TimeStamp", "Origin"}]
ALIGNED_COLUMN_ORDER = [OBS_TIMESTAMP_COL, COLI_TIMESTAMP_COL] + MEASUREMENT_COLUMNS

ALIGNED_UNIT_ROW = {
    OBS_TIMESTAMP_COL: UNIT_ROW["TimeStamp"],
    COLI_TIMESTAMP_COL: UNIT_ROW["TimeStamp"],
}
for col in MEASUREMENT_COLUMNS:
    ALIGNED_UNIT_ROW[col] = UNIT_ROW.get(col, "")


def find_pairs(input_dir: Path) -> List[Tuple[Tuple[str, str], Path, Path]]:
    obs_files: Dict[Tuple[str, str], Path] = {}
    for path in input_dir.glob("cleaned_data_Observator_*_to_*.csv"):
        match = OBS_PATTERN.match(path.name)
        if match:
            obs_files[(match.group(1), match.group(2))] = path

    coli_files: Dict[Tuple[str, str], Path] = {}
    for path in input_dir.glob("cleaned_data_ColiMinder_*_to_*.csv"):
        match = COLI_PATTERN.match(path.name)
        if match:
            coli_files[(match.group(1), match.group(2))] = path

    shared_keys = sorted(set(obs_files.keys()) & set(coli_files.keys()))
    return [(key, obs_files[key], coli_files[key]) for key in shared_keys]

def find_latest_pair(input_dir: Path) -> Tuple[Path | None, Path | None]:
    candidates = list(input_dir.glob("cleaned_data_*.csv"))
    obs_files = [p for p in candidates if "observator" in p.name.lower()]
    coli_files = [p for p in candidates if "coliminder" in p.name.lower()]
    obs_path = max(obs_files, key=lambda p: p.stat().st_mtime, default=None)
    coli_path = max(coli_files, key=lambda p: p.stat().st_mtime, default=None)
    return obs_path, coli_path


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    return df


def load_observator(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, skiprows=[1, 2, 3])
    df = _clean_columns(df)
    if "TimeStamp" not in df.columns:
        raise ValueError(f"Expected 'TimeStamp' column in {path.name}")
    df = df.dropna(subset=["TimeStamp"])
    cleaned = pd.DataFrame({col: df[col] if col in df.columns else pd.NA for col in OBS_COLUMNS})
    cleaned["Origin"] = ORIGIN_OBS
    cleaned["Activity - Coliminder"] = pd.NA
    cleaned = cleaned[COLUMN_ORDER]
    return cleaned


def load_coliminder(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, skiprows=[1, 2])
    df = _clean_columns(df).rename(columns={"Time (UTC)": "TimeStamp", "Activity": "Activity - Coliminder"})
    if "TimeStamp" not in df.columns:
        raise ValueError(f"Expected 'Time (UTC)' column in {path.name}")
    df = df.dropna(subset=["TimeStamp", "Activity - Coliminder"], how="all")

    data = pd.DataFrame()
    data["TimeStamp"] = df["TimeStamp"]
    data["Origin"] = ORIGIN_COLI
    data["Activity - Coliminder"] = df.get("Activity - Coliminder")
    for col in COLUMN_ORDER:
        if col not in data.columns:
            data[col] = pd.NA
    data = data[COLUMN_ORDER]
    return data


def sort_by_timestamp_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    ts = pd.to_datetime(df[column], dayfirst=True, errors="coerce")
    return df.assign(_ts=ts).sort_values("_ts").drop(columns="_ts")


def sort_by_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    return sort_by_timestamp_column(df, "TimeStamp")


def add_unit_row(data_rows: pd.DataFrame, unit_row: Dict[str, str]) -> pd.DataFrame:
    unit_df = pd.DataFrame([unit_row])
    return pd.concat([unit_df, data_rows], ignore_index=True)


def write_period_file(output_dir: Path, key: Tuple[str, str], data_rows: pd.DataFrame) -> Path:
    start, end = key
    filename = f"cleaned_and_combined_data_{start}_to_{end}.csv"
    output_path = output_dir / filename
    with_unit = add_unit_row(data_rows, UNIT_ROW)
    with_unit.to_csv(output_path, index=False)
    return output_path


def write_latest_file(output_dir: Path, data_rows: pd.DataFrame) -> Path:
    output_path = output_dir / "cleaned_and_combined_data_latest.csv"
    with_unit = add_unit_row(data_rows, UNIT_ROW)
    with_unit.to_csv(output_path, index=False)
    return output_path


def update_general_file(output_dir: Path, new_rows: pd.DataFrame) -> Path:
    general_path = output_dir / "cleaned_and_combined_data_general.csv"
    if general_path.exists():
        existing = pd.read_csv(general_path)
        existing_data = existing.iloc[1:] if len(existing) else pd.DataFrame(columns=COLUMN_ORDER)
    else:
        existing_data = pd.DataFrame(columns=COLUMN_ORDER)

    if existing_data.empty:
        combined = new_rows.copy()
    else:
        combined = pd.concat([existing_data.astype(object), new_rows.astype(object)], ignore_index=True)
    combined = combined[COLUMN_ORDER]
    combined = combined.drop_duplicates(subset=["TimeStamp", "Origin"], keep="first")
    combined = sort_by_timestamp(combined)

    final_df = add_unit_row(combined, UNIT_ROW)
    final_df.to_csv(general_path, index=False)
    return general_path


def combine_pair(obs_path: Path, coli_path: Path) -> pd.DataFrame:
    obs_df = load_observator(obs_path)
    coli_df = load_coliminder(coli_path)
    frames = [df.astype(object) for df in (obs_df, coli_df) if not df.empty]
    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMN_ORDER)
    merged = merged[COLUMN_ORDER]
    merged = sort_by_timestamp(merged)
    return merged


def align_combined_rows(combined_rows: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, float]]:
    if combined_rows.empty:
        stats = {
            "total_coliminder_rows": 0,
            "matched_coliminder_rows": 0,
            "unmatched_coliminder_rows": 0,
            "unmatched_percentage": 0.0,
        }
        return pd.DataFrame(columns=ALIGNED_COLUMN_ORDER), stats

    obs = combined_rows.loc[combined_rows["Origin"] == ORIGIN_OBS].copy()
    coli = combined_rows.loc[combined_rows["Origin"] == ORIGIN_COLI].copy()

    obs["obs_ts"] = pd.to_datetime(obs["TimeStamp"], dayfirst=True, errors="coerce")
    coli["coli_ts"] = pd.to_datetime(coli["TimeStamp"], dayfirst=True, errors="coerce")

    obs = obs.dropna(subset=["obs_ts"]).reset_index(drop=True)
    coli = coli.dropna(subset=["coli_ts"]).reset_index(drop=True)

    assignments: Dict[int, pd.Series] = {}
    assigned_mask = pd.Series(False, index=obs.index)

    for _, coli_row in coli.iterrows():
        available = assigned_mask[~assigned_mask].index
        if len(available) == 0:
            break
        diffs = (obs.loc[available, "obs_ts"] - coli_row["coli_ts"]).abs()
        if diffs.empty:
            break
        target_idx = diffs.idxmin()
        assignments[target_idx] = coli_row
        assigned_mask.loc[target_idx] = True

    total_coli = len(coli)
    matched = len(assignments)
    unmatched = total_coli - matched
    unmatched_pct = (unmatched / total_coli * 100) if total_coli else 0.0

    aligned = pd.DataFrame(columns=ALIGNED_COLUMN_ORDER)
    aligned[OBS_TIMESTAMP_COL] = obs["TimeStamp"]
    aligned[COLI_TIMESTAMP_COL] = pd.NA
    for col in MEASUREMENT_COLUMNS:
        aligned[col] = obs[col] if col in obs.columns else pd.NA
    aligned = aligned.astype(object)

    for target_idx, coli_row in assignments.items():
        aligned.at[target_idx, COLI_TIMESTAMP_COL] = coli_row["TimeStamp"]
        for col in MEASUREMENT_COLUMNS:
            if col in coli_row and pd.notna(coli_row[col]):
                aligned.at[target_idx, col] = coli_row[col]

    aligned = sort_by_timestamp_column(aligned, OBS_TIMESTAMP_COL)

    stats = {
        "total_coliminder_rows": total_coli,
        "matched_coliminder_rows": matched,
        "unmatched_coliminder_rows": unmatched,
        "unmatched_percentage": unmatched_pct,
    }
    return aligned, stats


def write_aligned_period_file(output_dir: Path, key: Tuple[str, str], aligned_rows: pd.DataFrame) -> Path:
    start, end = key
    filename = f"cleaned_and_combined_and_aligned_data_{start}_to_{end}.csv"
    output_path = output_dir / filename
    with_unit = add_unit_row(aligned_rows, ALIGNED_UNIT_ROW)
    with_unit.to_csv(output_path, index=False)
    return output_path


def write_aligned_latest_file(output_dir: Path, aligned_rows: pd.DataFrame) -> Path:
    output_path = output_dir / "cleaned_and_combined_and_aligned_data_latest.csv"
    with_unit = add_unit_row(aligned_rows, ALIGNED_UNIT_ROW)
    with_unit.to_csv(output_path, index=False)
    return output_path


def update_aligned_general_file(output_dir: Path, new_rows: pd.DataFrame) -> Path:
    general_path = output_dir / "cleaned_and_combined_and_aligned_data_general.csv"
    if general_path.exists():
        existing = pd.read_csv(general_path)
        existing_data = existing.iloc[1:] if len(existing) else pd.DataFrame(columns=ALIGNED_COLUMN_ORDER)
    else:
        existing_data = pd.DataFrame(columns=ALIGNED_COLUMN_ORDER)

    if existing_data.empty:
        combined = new_rows.copy()
    else:
        combined = pd.concat([existing_data.astype(object), new_rows.astype(object)], ignore_index=True)

    combined = combined[ALIGNED_COLUMN_ORDER]
    combined = combined.drop_duplicates(subset=[OBS_TIMESTAMP_COL], keep="first")
    combined = sort_by_timestamp_column(combined, OBS_TIMESTAMP_COL)

    final_df = add_unit_row(combined, ALIGNED_UNIT_ROW)
    final_df.to_csv(general_path, index=False)
    return general_path


def combine_cleaned(
    input_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> List[Path]:
    cleaned_dir = (
        Path(input_dir)
        if input_dir
        else PROJECT_ROOT / OUTPUT_DATA_DIR / CLEANED_DIR
    )
    combined_dir = (
        Path(output_dir)
        if output_dir
        else PROJECT_ROOT / OUTPUT_DATA_DIR / COMBINED_DIR
    )
    combined_dir.mkdir(parents=True, exist_ok=True)

    # this is the original logic that has _to_ in the filename 
    # which now it does not.
    pairs = find_pairs(cleaned_dir)
    outputs: List[Path] = []
    if pairs:
        for key, obs_path, coli_path in pairs:
            combined_rows = combine_pair(obs_path, coli_path)
            outputs.append(write_period_file(combined_dir, key, combined_rows))
            outputs.append(update_general_file(combined_dir, combined_rows))

            aligned_rows, _ = align_combined_rows(combined_rows)
            outputs.append(write_aligned_period_file(combined_dir, key, aligned_rows))
            outputs.append(update_aligned_general_file(combined_dir, aligned_rows))
    # just use the two latest files in the output_dir/cleaned
    else:
        obs_path, coli_path = find_latest_pair(cleaned_dir)
        if obs_path and coli_path:
            combined_rows = combine_pair(obs_path, coli_path)
            outputs.append(write_latest_file(combined_dir, combined_rows))
            outputs.append(update_general_file(combined_dir, combined_rows))

            aligned_rows, _ = align_combined_rows(combined_rows)
            outputs.append(write_aligned_latest_file(combined_dir, aligned_rows))
            outputs.append(update_aligned_general_file(combined_dir, aligned_rows))
    return outputs
