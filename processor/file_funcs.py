from __future__ import annotations

import os
import logging
from typing import List, Optional

import pandas as pd

RAW_PREFIX = "data"
FLAGGED_PREFIX = "flagged_data_"
CLEANED_PREFIX = "cleaned_data_"
COMBINED_PREFIX = "combined_data_"
DIARY_FILENAME = "Anne kanal diary.csv"
UPLOAD_DIR = "uploads"
OUTPUT_DATA_DIR = "output_data"
FLAGGED_DIR = "flagged" # writes flagged data to a directroy in root
CLEANED_DIR = "cleaned" # writes cleaned data (failed flags removed) to a directory in root
COMBINED_DIR = "combined" # combines both Observator and Coliminder cleaned data to a directory in root

def detect_origin(file_path: str, logger: Optional[logging.Logger] = None) -> Optional[str]:
    name = os.path.basename(file_path).lower()
    observator_index = name.find("observator")
    coliminder_index = name.find("coliminder")

    origin = None
    if observator_index != -1 and (coliminder_index == -1 or observator_index < coliminder_index):
        origin = "Observator"
        if coliminder_index != -1 and logger:
            logger.warning("Filename contains both origins; choosing Observator for %s", file_path)
    elif coliminder_index != -1:
        origin = "ColiMinder"
        if observator_index != -1 and logger:
            logger.warning("Filename contains both origins; choosing ColiMinder for %s", file_path)
    return origin


def load_raw_csv(file_path: str) -> pd.DataFrame:
    # Keep strings as-is to preserve formatting for decimal checks.
    return pd.read_csv(
        file_path,
        dtype=str,
        keep_default_na=False,
        sep=None,
        engine="python",
    )


def write_output_csv(df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)

def build_output_path(input_path: str, output_dir: str) -> str:
    base = os.path.basename(input_path)
    if base.startswith(RAW_PREFIX):
        base = base.replace(RAW_PREFIX, FLAGGED_PREFIX, 1)
    else:
        base = f"{FLAGGED_PREFIX}{base}"
    return os.path.join(output_dir, base)

def build_clean_output_path(input_path: str, output_dir: str) -> str:
    base = os.path.basename(input_path)
    if base.startswith(RAW_PREFIX):
        base = base.replace(RAW_PREFIX, CLEANED_PREFIX, 1)
    else:
        base = f'{CLEANED_PREFIX}{base}'
    return os.path.join(output_dir, base)

# this seems to work
def list_raw_files(input_dir: str, logger: Optional[logging.Logger] = None) -> List[str]:
    paths: List[str] = []
    for entry in os.listdir(input_dir):
        entry_lower = entry.lower()
        if not entry_lower.endswith(".csv"):
            continue
        if entry_lower == DIARY_FILENAME.lower():
            continue
        file_path = os.path.join(input_dir, entry)
        origin = detect_origin(file_path, logger=logger)
        if origin is None:
            continue
        paths.append(file_path)
    return paths
