from __future__ import annotations

import os
import re
import logging
from pathlib import Path
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

SITE_FILE_RE = re.compile(r"^(?P<site>[a-z0-9]+)_(\d{8})_(\d{6})$", re.IGNORECASE)

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
    else:
        stem = Path(file_path).stem
        if SITE_FILE_RE.match(stem):
            origin = "Combined"
    return origin


def extract_site_from_filename(file_path: str) -> Optional[str]:
    stem = Path(file_path).stem
    match = SITE_FILE_RE.match(stem)
    if not match:
        return None
    return match.group("site")


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
    stem = Path(base).stem
    if SITE_FILE_RE.match(stem):
        if not base.lower().startswith("flagged_"):
            base = f"flagged_{base}"
        return os.path.join(output_dir, base)
    if base.startswith(RAW_PREFIX):
        base = base.replace(RAW_PREFIX, FLAGGED_PREFIX, 1)
    else:
        base = f"{FLAGGED_PREFIX}{base}"
    return os.path.join(output_dir, base)

def build_clean_output_path(input_path: str, output_dir: str) -> str:
    base = os.path.basename(input_path)
    stem = Path(base).stem
    if SITE_FILE_RE.match(stem):
        if not base.lower().startswith("cleaned_"):
            base = f"cleaned_{base}"
        return os.path.join(output_dir, base)
    if base.startswith(RAW_PREFIX):
        base = base.replace(RAW_PREFIX, CLEANED_PREFIX, 1)
    else:
        base = f'{CLEANED_PREFIX}{base}'
    return os.path.join(output_dir, base)

# this seems to work but returns a list. in the use case there should be a single file
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

# helper to simply return the first file 
def get_raw_file(input_dir: str, logger: Optional[logging.Logger] = None) -> Optional[str]:
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

        return file_path  # <-- return immediately

    return None
