from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .config import DQConfig, ParameterConfig, load_config
from .io import detect_origin, load_raw_csv, write_output_csv, build_output_path, build_clean_output_path, DIARY_FILENAME, list_raw_files
from .maintenance import load_maintenance_periods, flag_maintenance
from .qc_checks import (
    FAIL,
    PASS,
    applicable_checks,
    evaluate_parameter,
    match_raw_column,
    normalize_column,
    parse_timestamp,
)


class QCEngine:
    def __init__(self, config_path: str, input_dir: str, output_dir: str, logger: Optional[logging.Logger] = None):
        self.config_path = config_path
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.logger = logger or logging.getLogger(__name__)

    def _reload_config(self) -> DQConfig:
        return load_config(self.config_path)

    def _load_maintenance_periods(self) -> List:
        diary_path = os.path.join(self.input_dir, DIARY_FILENAME)
        return load_maintenance_periods(diary_path)

    def _drop_unwanted_columns(self, df: pd.DataFrame, origin: str) -> pd.DataFrame:
        drop_targets = {
            "Observator": {
                "spconduscm",
                "bgapcugl",
                "chlorophyllugl",
                "fdomqsu",
            },
        }
        target = drop_targets.get(origin)
        if not target:
            return df
        normalized = {normalize_column(col): col for col in df.columns}
        to_drop = [col for norm, col in normalized.items() if norm in target]
        if not to_drop:
            return df
        if self.logger:
            self.logger.info("Dropping columns for %s: %s", origin, ", ".join(to_drop))
        return df.drop(columns=to_drop)


    def _map_columns(self, df: pd.DataFrame, params: List[ParameterConfig]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for param in params:
            match = match_raw_column(list(df.columns), param.raw_columns)
            if match:
                mapping[param.key] = match
        return mapping

    def _timestamp_column(self, params: List[ParameterConfig], mapping: Dict[str, str]) -> Optional[str]:
        for param in params:
            if "timestamp_format" in (param.rules or {}) and param.key in mapping:
                return mapping[param.key]
        return None

    def _metadata_row_index(self, df: pd.DataFrame, timestamp_col: Optional[str], timestamp_format: Optional[str]) -> Optional[int]:
        if timestamp_col is None or timestamp_col not in df.columns:
            return None
        first_value = df.iloc[0][timestamp_col]
        parsed = parse_timestamp(first_value, fmt=timestamp_format)
        return 0 if parsed is None else None

    def _build_cleaned_df(self, flagged_df: pd.DataFrame) -> pd.DataFrame:
        if "overall_dq_check" not in flagged_df.columns:
            return flagged_df.copy()
        cleaned = flagged_df[flagged_df["overall_dq_check"] == PASS].copy()
        drop_cols = [col for col in cleaned.columns if col.endswith("_flag") and col != "overall_dq_check"]
        if drop_cols:
            cleaned = cleaned.drop(columns=drop_cols)
        return cleaned


    def process_file(self, file_path: str) -> Optional[str]:
        origin = detect_origin(file_path, logger=self.logger)
        if origin is None:
            return None

        config = self._reload_config()
        params_for_origin = config.parameters_for_origin(origin)
        if not params_for_origin:
            return None

        df = load_raw_csv(file_path)
        df = self._drop_unwanted_columns(df, origin)
        column_mapping = self._map_columns(df, params_for_origin)
        if not column_mapping:
            if self.logger:
                self.logger.warning(
                    "Skipping QC for %s (origin %s) because no raw columns matched YAML definitions",
                    file_path,
                    origin,
                )
            return None

        timestamp_param = next((p for p in params_for_origin if "timestamp_format" in (p.rules or {})), None)
        timestamp_col = self._timestamp_column(params_for_origin, column_mapping)
        metadata_index = self._metadata_row_index(df, timestamp_col, timestamp_param.rules.get("timestamp_format") if timestamp_param else None)

        maintenance_periods = self._load_maintenance_periods()
        timestamp_series = df[timestamp_col] if timestamp_col else pd.Series([None] * len(df))
        maintenance_flags = flag_maintenance(timestamp_series, maintenance_periods, metadata_index)

        new_columns: Dict[str, List[str]] = {}
        # origin column with metadata row empty
        origin_col: List[str] = []
        for idx in range(len(df)):
            if metadata_index is not None and idx == metadata_index:
                origin_col.append("")
            else:
                origin_col.append(origin)
        new_columns["origin"] = origin_col

        # Parameter checks
        for param in params_for_origin:
            if param.key not in column_mapping:
                continue
            series = df[column_mapping[param.key]]
            flag_columns, qc_flags = evaluate_parameter(series, param, config.checks, metadata_index)
            for col_name, values in flag_columns.items():
                new_columns[col_name] = values
            new_columns[f"{param.key}_qc_flag"] = qc_flags

        new_columns["maintenance_flag"] = maintenance_flags

        # Overall DQ check
        flag_columns_for_overall = [
            col for col in new_columns.keys() if col not in {"origin", "overall_dq_check"}
        ]
        overall: List[str] = []
        for idx in range(len(df)):
            if metadata_index is not None and idx == metadata_index:
                overall.append("")
                continue
            has_fail = any(new_columns[col][idx] == FAIL for col in flag_columns_for_overall)
            overall.append(FAIL if has_fail else PASS)
        new_columns["overall_dq_check"] = overall

        # Append new columns in required order
        append_order: List[str] = ["origin", "overall_dq_check", "maintenance_flag"]
        for param in params_for_origin:
            if param.key not in column_mapping:
                continue
            for check in applicable_checks(param, config.checks):
                append_order.append(f"{param.key}_{check}_flag")
            append_order.append(f"{param.key}_qc_flag")

        for col in append_order:
            if col not in new_columns:
                # Fill missing columns with blanks for completeness
                new_columns[col] = [""] * len(df)

        for col in append_order:
            df[col] = new_columns[col]

        # Prepend PASS/FAIL percentage rows for flag columns.
        pass_row: Dict[str, str] = {col: "" for col in df.columns}
        fail_row: Dict[str, str] = {col: "" for col in df.columns}
        for col in df.columns:
            values = df[col]
            total = sum(1 for v in values if v in {PASS, FAIL})
            if total == 0:
                continue
            pass_count = sum(1 for v in values if v == PASS)
            fail_count = sum(1 for v in values if v == FAIL)
            pass_row[col] = f"{(pass_count / total) * 100:.2f}"
            fail_row[col] = f"{(fail_count / total) * 100:.2f}"

        df = pd.concat([pd.DataFrame([pass_row, fail_row]), df], ignore_index=True)

        output_path = build_output_path(file_path, self.output_dir)
        write_output_csv(df, output_path)

        cleaned_df = self._build_cleaned_df(df)
        cleaned_output_path = build_clean_output_path(file_path, self.output_dir)
        write_output_csv(cleaned_df, cleaned_output_path)

        return output_path

    def process_directory_once(self) -> List[str]:
        processed: List[str] = []
        for file_path in list_raw_files(self.input_dir, logger=self.logger):
            if not os.path.isfile(file_path):
                continue
            output = self.process_file(file_path)
            if output:
                processed.append(output)
        return processed
