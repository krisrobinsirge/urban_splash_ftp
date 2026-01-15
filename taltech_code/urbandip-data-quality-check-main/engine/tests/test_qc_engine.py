import io
import logging
import os
from pathlib import Path

import pandas as pd
import pytest

from src.config import load_config
from src.io import detect_origin, list_raw_files, load_raw_csv
from src.qc_checks import evaluate_parameter, applicable_checks
from src.qc_engine import QCEngine


def test_raw_column_mapping_matches_sample_files():
    engine = QCEngine(config_path="dq_master.yaml", input_dir="data_raw", output_dir="data_out")
    config = load_config("dq_master.yaml")

    obs_candidates = list(Path("data_raw").glob("*Observator*.csv"))
    if not obs_candidates:
        pytest.skip("No Observator sample CSV present in data_raw")
    obs_df = load_raw_csv(str(obs_candidates[0]))
    obs_params = config.parameters_for_origin("Observator")
    obs_mapping = engine._map_columns(obs_df, obs_params)
    assert obs_mapping["SpCond_uScm"] == "SpCond uS/cm"
    assert obs_mapping["BGA_PC_RFU"] == "BGA PC RFU"
    assert obs_mapping["Chlorophyll_RFU"] == "Chlorophyll RFU"

    coli_candidates = list(Path("data_raw").glob("*ColiMinder*.csv"))
    if not coli_candidates:
        pytest.skip("No ColiMinder sample CSV present in data_raw")
    coli_df = load_raw_csv(str(coli_candidates[0]))
    coli_params = config.parameters_for_origin("ColiMinder")
    coli_mapping = engine._map_columns(coli_df, coli_params)
    assert coli_mapping["Sample_Numb"] in {"Sample Numb.", "Sample Numb", "Sample Number"}
    assert coli_mapping["Activity"] == "Activity"
    assert coli_mapping["Time_UTC"] in {"Time (UTC)", "Time UTC", "Time_UTC"}


def test_sample_numb_allowed_values_pass_and_fail():
    config = load_config("dq_master.yaml")
    param = next(p for p in config.parameters if p.key == "Sample_Numb")
    series = pd.Series(["0", "1"])
    flag_columns, qc_flags = evaluate_parameter(series, param, config.checks, metadata_index=None)
    allowed_col = flag_columns[f"{param.key}_allowed_values_flag"]
    assert allowed_col == ["PASS", "FAIL"]
    assert qc_flags == ["PASS", "FAIL"]


def test_spcond_outlier_triggers_range_fail():
    config = load_config("dq_master.yaml")
    param = next(p for p in config.parameters if p.key == "SpCond_uScm")
    series = pd.Series(["31402.00"])
    flag_columns, qc_flags = evaluate_parameter(series, param, config.checks, metadata_index=None)
    assert flag_columns[f"{param.key}_range_flag"][0] == "FAIL"
    assert qc_flags[0] == "FAIL"


def test_overall_flag_fails_when_any_check_fails(tmp_path):
    input_dir = tmp_path / "data_raw"
    output_dir = tmp_path / "data_out"
    input_dir.mkdir()
    output_dir.mkdir()

    csv_path = input_dir / "raw_data_Observator_test.csv"
    csv_path.write_text("TimeStamp,SpCond (uS/cm)\n30/11/2025 00:00,31402.00\n", encoding="utf-8")

    engine = QCEngine(config_path="dq_master.yaml", input_dir=str(input_dir), output_dir=str(output_dir))
    output_path = engine.process_file(str(csv_path))
    assert output_path is not None

    output_df = pd.read_csv(output_path, dtype=str)
    assert output_df.loc[0, "SpCond_uScm_range_flag"] == "FAIL"
    assert output_df.loc[0, "overall_dq_check"] == "FAIL"


def test_relaxed_file_matching_and_diary_ignore(tmp_path):
    input_dir = tmp_path / "data_raw"
    input_dir.mkdir()
    (input_dir / "my_observator_file.CSV").write_text("TimeStamp,Temp (C)\n30/11/2025 00:00,1", encoding="utf-8")
    (input_dir / "data_coliminder_test.csv").write_text("Time (UTC),Sample Numb,Activity\n30/11/2025 00:00,0,1", encoding="utf-8")
    (input_dir / "Anne Kanal Diary.csv").write_text("", encoding="utf-8")
    (input_dir / "readme.txt").write_text("", encoding="utf-8")

    files = list_raw_files(str(input_dir))
    names = {os.path.basename(p) for p in files}
    assert "my_observator_file.CSV" in names
    assert "data_coliminder_test.csv" in names
    assert "Anne Kanal Diary.csv" not in names


def test_detect_origin_prefers_first_match_and_logs_warning(tmp_path):
    logger, stream = _build_capture_logger()
    file_path = tmp_path / "coliminder_observator.csv"
    file_path.write_text("", encoding="utf-8")
    origin = detect_origin(str(file_path), logger=logger)
    assert origin == "ColiMinder"
    assert "both origins" in stream.getvalue()


def test_process_file_skips_when_no_matching_columns(tmp_path):
    input_dir = tmp_path / "data_raw"
    output_dir = tmp_path / "data_out"
    input_dir.mkdir()
    output_dir.mkdir()
    file_path = input_dir / "observator_missing_cols.csv"
    file_path.write_text("Other\n1\n", encoding="utf-8")
    logger, stream = _build_capture_logger()

    engine = QCEngine(config_path="dq_master.yaml", input_dir=str(input_dir), output_dir=str(output_dir), logger=logger)
    result = engine.process_file(str(file_path))
    assert result is None
    assert "Skipping QC" in stream.getvalue()
    assert not list(output_dir.iterdir())


def test_spike_detection_up_and_down():
    config = load_config("dq_master.yaml")
    param = next(p for p in config.parameters if p.key == "SpCond_uScm")
    config.checks["spike"] = True
    param.rules["max_delta_per_step"] = 5

    series = pd.Series(["1", "7", "2"])
    flag_columns, _ = evaluate_parameter(series, param, config.checks, metadata_index=None)
    assert flag_columns[f"{param.key}_spike_flag"] == ["PASS", "FAIL", "PASS"]


def test_spike_missing_and_nonnumeric_do_not_update_previous():
    config = load_config("dq_master.yaml")
    param = next(p for p in config.parameters if p.key == "Activity")
    config.checks["spike"] = True
    param.rules["max_delta_per_step"] = 20

    series = pd.Series(["10", "", "abc", "25"])
    flag_columns, _ = evaluate_parameter(series, param, config.checks, metadata_index=None)
    assert flag_columns[f"{param.key}_spike_flag"] == ["PASS", "PASS", "PASS", "PASS"]


def test_spike_not_included_when_globally_disabled():
    config = load_config("dq_master.yaml")
    param = next(p for p in config.parameters if p.key == "SpCond_uScm")
    checks = applicable_checks(param, config.checks)
    assert "spike" not in checks


def test_percent_rows_added(tmp_path):
    input_dir = tmp_path / "data_raw"
    output_dir = tmp_path / "data_out"
    input_dir.mkdir()
    output_dir.mkdir()
    csv_path = input_dir / "raw_data_Observator_test.csv"
    csv_path.write_text("TimeStamp,SpCond (uS/cm)\n30/11/2025 00:00,1\n30/11/2025 00:05,2000\n", encoding="utf-8")

    engine = QCEngine(config_path="dq_master.yaml", input_dir=str(input_dir), output_dir=str(output_dir))
    output_path = engine.process_file(str(csv_path))
    assert output_path is not None

    df = pd.read_csv(output_path, dtype=str)
    # First two rows are percentage rows
    pass_row = df.iloc[0]
    fail_row = df.iloc[1]
    # SpCond range: one PASS (1), one FAIL (2000 > 1000)
    assert pass_row["SpCond_uScm_range_flag"].startswith("50.")
    assert fail_row["SpCond_uScm_range_flag"].startswith("50.")


def _build_capture_logger():
    stream = io.StringIO()
    logger = logging.getLogger("dq_test_capture")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger, stream
