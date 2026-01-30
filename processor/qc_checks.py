from __future__ import annotations

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd

from processor.config import ParameterConfig

PASS = "PASS"
FAIL = "FAIL"


def normalize_column(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def match_raw_column(actual_columns: List[str], candidates: List[str]) -> Optional[str]:
    normalized_map = {normalize_column(col): col for col in actual_columns}
    for candidate in candidates:
        normalized = normalize_column(candidate)
        if normalized in normalized_map:
            return normalized_map[normalized]
    return None


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    if isinstance(value, str):
        if value.strip() == "":
            return True
        lowered = value.strip().lower()
        return lowered in {"na", "nan", "none"}
    return False


def to_float(value: Any) -> Optional[float]:
    if is_missing(value):
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def count_decimals(value: Any) -> int:
    if is_missing(value):
        return 0
    text = str(value).strip()
    if "." not in text:
        return 0
    decimal_part = text.split(".")[-1]
    return len(decimal_part)


def parse_timestamp(value: Any, fmt: Optional[str] = None) -> Optional[pd.Timestamp]:
    if is_missing(value):
        return None
    text = str(value).strip()
    if fmt:
        parsed = pd.to_datetime(text, format=fmt, errors="coerce")
        if pd.notna(parsed):
            return parsed
    parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed


def applicable_checks(param: ParameterConfig, global_checks: Dict[str, bool]) -> List[str]:
    checks: List[str] = []
    rules = param.rules or {}
    if global_checks.get("numeric", False) and rules.get("numeric_required", False):
        checks.append("numeric")
    if global_checks.get("completeness", False) and not rules.get("allow_nulls", False):
        checks.append("completeness")
    if global_checks.get("format", False) and rules.get("decimal_max") is not None:
        checks.append("format")
    if global_checks.get("range", False) and (
        rules.get("min_value") is not None or rules.get("max_value") is not None
    ):
        checks.append("range")
    if global_checks.get("nonnegative", False) and rules.get("nonnegative_required", False):
        checks.append("nonnegative")
    if global_checks.get("spike", False) and rules.get("max_delta_per_step") is not None:
        checks.append("spike")
    if global_checks.get("flatline", False) and rules.get("streak_threshold"):
        checks.append("flatline")
    if rules.get("allowed_values") is not None:
        checks.append("allowed_values")
    return checks


def evaluate_parameter(
    series: pd.Series,
    param: ParameterConfig,
    global_checks: Dict[str, bool],
    metadata_index: Optional[int],
    missing_ok: bool = False,
) -> Tuple[Dict[str, List[str]], List[str]]:
    rules = param.rules or {}
    checks_to_run = applicable_checks(param, global_checks)

    flag_columns: Dict[str, List[str]] = {f"{param.key}_{chk}_flag": [] for chk in checks_to_run}
    qc_flags: List[str] = []

    last_value: Optional[float] = None
    streak_count = 0
    streak_threshold = rules.get("streak_threshold")
    previous_numeric: Optional[float] = None
    spike_threshold = rules.get("max_delta_per_step")

    for idx, raw in series.items():
        if metadata_index is not None and idx == metadata_index:
            for col in flag_columns:
                flag_columns[col].append("")
            qc_flags.append("")
            last_value = None
            streak_count = 0
            previous_numeric = None
            continue

        missing = is_missing(raw)
        numeric_val = to_float(raw) if not missing else None
        row_results: Dict[str, str] = {}

        for check in checks_to_run:
            if check == "completeness":
                row_results[f"{param.key}_completeness_flag"] = PASS if (not missing or missing_ok) else FAIL
            elif check == "numeric":
                row_results[f"{param.key}_numeric_flag"] = PASS if (numeric_val is not None or missing_ok) else FAIL
            elif check == "format":
                decimal_max = rules.get("decimal_max")
                if numeric_val is None:
                    row_results[f"{param.key}_format_flag"] = PASS if missing_ok else FAIL
                else:
                    decimals = count_decimals(raw)
                    row_results[f"{param.key}_format_flag"] = PASS if decimals <= decimal_max else FAIL
            elif check == "range":
                min_val = rules.get("min_value")
                max_val = rules.get("max_value")
                if numeric_val is None:
                    row_results[f"{param.key}_range_flag"] = PASS if missing_ok else FAIL
                else:
                    too_low = min_val is not None and numeric_val < float(min_val)
                    too_high = max_val is not None and numeric_val > float(max_val)
                    row_results[f"{param.key}_range_flag"] = FAIL if too_low or too_high else PASS
            elif check == "nonnegative":
                if numeric_val is None:
                    row_results[f"{param.key}_nonnegative_flag"] = PASS if missing_ok else FAIL
                else:
                    row_results[f"{param.key}_nonnegative_flag"] = PASS if numeric_val >= 0 else FAIL
            elif check == "spike":
                if numeric_val is None:
                    row_results[f"{param.key}_spike_flag"] = PASS
                else:
                    try:
                        threshold = float(spike_threshold)
                    except (TypeError, ValueError):
                        row_results[f"{param.key}_spike_flag"] = PASS
                    else:
                        if previous_numeric is None:
                            row_results[f"{param.key}_spike_flag"] = PASS
                        else:
                            delta = abs(numeric_val - previous_numeric)
                            row_results[f"{param.key}_spike_flag"] = FAIL if delta > threshold else PASS
                        previous_numeric = numeric_val
            elif check == "flatline":
                if numeric_val is None:
                    row_results[f"{param.key}_flatline_flag"] = PASS
                    last_value = None
                    streak_count = 0
                else:
                    if last_value is not None and numeric_val == last_value:
                        streak_count += 1
                    else:
                        streak_count = 1
                    last_value = numeric_val
                    row_results[f"{param.key}_flatline_flag"] = (
                        FAIL if streak_threshold and streak_count >= int(streak_threshold) else PASS
                    )
            elif check == "allowed_values":
                allowed = rules.get("allowed_values") or []
                if missing:
                    row_results[f"{param.key}_allowed_values_flag"] = PASS if missing_ok else FAIL
                else:
                    raw_text = str(raw).strip()
                    allowed_match = False
                    for allowed_val in allowed:
                        if raw_text == str(allowed_val):
                            allowed_match = True
                            break
                        allowed_num = to_float(raw_text)
                        if allowed_num is not None and allowed_val is not None:
                            try:
                                if float(allowed_val) == allowed_num:
                                    allowed_match = True
                                    break
                            except (TypeError, ValueError):
                                continue
                    row_results[f"{param.key}_allowed_values_flag"] = PASS if allowed_match else FAIL

        for col, result in row_results.items():
            flag_columns[col].append(result)

        qc_flags.append(FAIL if FAIL in row_results.values() else PASS)

    return flag_columns, qc_flags
