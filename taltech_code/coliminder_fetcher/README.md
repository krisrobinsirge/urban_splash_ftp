# Coliminder Data Fetcher

This project periodically downloads a single CSV file from an Apache directory listing endpoint. It watches a remote `timestamp.txt` file and saves the latest CSV when the remote timestamp changes. The script keeps only one CSV file locally.

## Setup

1) Create a virtual environment:
```bash
python -m venv .venv
```

2) Activate the virtual environment:
```bash
# Windows (PowerShell)
.\.venv\Scripts\Activate.ps1

# macOS/Linux
source .venv/bin/activate
```

3) Install dependencies:
```bash
pip install -r requirements.txt
```

4) Run the script:
```bash
python coliminder_fetcher.py
```

## Outputs

- Downloads are saved to `downloads/` as `CSV_FILENAME` and older timestamped copies (if any) are removed.
- State files are stored in `state/` to track the last timestamp and CSV hash.
- Logs are written to `coliminder_fetch.log` in the project root.

## Basic Auth

To enable Basic Auth, edit `coliminder_fetcher.py`:

- Set `USE_BASIC_AUTH = True`
- Set `BASIC_AUTH_USERNAME` and `BASIC_AUTH_PASSWORD` to your credentials

## CSV name

The script downloads the CSV at `BASE_URL/CSV_FILENAME`. Set `CSV_FILENAME` in `coliminder_fetcher.py` to match the exact filename shown in the Apache listing.
