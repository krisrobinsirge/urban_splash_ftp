# Data Quality Check Engine

Python tool that reads raw Observator and ColiMinder CSVs, applies YAML-driven quality checks, flags maintenance periods, and writes checked CSVs to `data_out/`. A watchdog is included to process new raw files automatically.

## Setup

```bash
pip install pandas pyyaml watchdog pytest
```

## Usage

Process existing files once:

```bash
python -m src.main --input data_raw --output data_out --config dq_master.yaml --once
```

Watch `data_raw` for new files and process them:

```bash
python -m src.main --input data_raw --output data_out --config dq_master.yaml --watch
```

## Outputs

- Flagged files are written as `flagged_data_*.csv` in `data_out/` with all QC flag columns.
- Cleaned files are written alongside as `cleaned_data_*.csv` containing only rows where `overall_dq_check` is `PASS`, and only the `overall_dq_check` flag column is kept (other *_flag columns are removed).
- For Observator inputs, raw columns `SpCond uS/cm`, `BGA PC ug/L`, `Chlorophyll ug/L`, and `fDOM QSU` are dropped before QC output.

## Raw file requirements

- File type: `.csv` (case-insensitive).
- Origin detection: filename must contain `observator` or `coliminder` (case-insensitive); if both appear, the first match wins.
- The maintenance diary file must be named `Anne kanal diary.csv` (any casing) and is ignored by QC.
- Columns: at least one column must match a parameter's `raw_columns` for the detected origin from `dq_master.yaml`; otherwise the file is skipped with a warning.
- Timestamp column: should match the YAML timestamp `raw_columns` and format (`%d/%m/%Y %H:%M`) so maintenance and metadata-row handling work correctly.
- All other columns are preserved; unmapped columns are not QC-checked but remain in the output.

## Testing

Run the pytest suite:

```bash
pytest
```
