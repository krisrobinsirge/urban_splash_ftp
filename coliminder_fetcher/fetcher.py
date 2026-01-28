from __future__ import annotations

import datetime
import hashlib
import logging
import os
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv
from logger.logger import build_logger

load_dotenv()

# load all the config from the .env file
BASE_URL = os.getenv("COLIMINDER_BASE_URL", "").strip()
TIMESTAMP_FILENAME = os.getenv("COLIMINDER_TIMESTAMP_FILENAME", "timestamp.txt").strip()
CSV_FILENAME = os.getenv("COLIMINDER_CSV_FILENAME", "results_red_V01_04.csv").strip()

USE_BASIC_AUTH = os.getenv("COLIMINDER_USE_BASIC_AUTH", "true").strip().lower() == "true"
BASIC_AUTH_USERNAME = os.getenv("COLIMINDER_BASIC_AUTH_USERNAME", "").strip()
BASIC_AUTH_PASSWORD = os.getenv("COLIMINDER_BASIC_AUTH_PASSWORD", "").strip()

PARTIAL_DOWNLOAD_DELAY_SECONDS = int(os.getenv("COLIMINDER_PARTIAL_DOWNLOAD_DELAY_SECONDS", "5"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("COLIMINDER_REQUEST_TIMEOUT_SECONDS", "60"))

if BASE_URL and not BASE_URL.endswith("/"):
    BASE_URL += "/"

PROJECT_ROOT = Path(__file__).resolve().parent
DOWNLOADS_DIR = Path("raw_input")
STATE_DIR = PROJECT_ROOT / "state"
LOG_FILE = Path("logs") / "coliminder_fetch.log"


def ensure_dirs(download_dir: Path) -> None:
    download_dir.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def get_auth(logger: logging.Logger):
    if not USE_BASIC_AUTH:
        return None
    if not BASIC_AUTH_USERNAME or not BASIC_AUTH_PASSWORD:
        logger.error("Basic auth is enabled but username/password are missing.")
        return None
    return (BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD)


def read_state_text(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def write_state_text(path: Path, value: str) -> None:
    path.write_text(value, encoding="utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fetch_timestamp(logger: logging.Logger) -> int | None:
    if not BASE_URL:
        logger.error("COLIMINDER_BASE_URL is not configured.")
        return None

    url = urljoin(BASE_URL, TIMESTAMP_FILENAME)
    logger.info("Checking timestamp at %s", url)
    try:
        response = requests.get(url, auth=get_auth(logger), timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Failed to fetch timestamp: %s", exc)
        return None

    text = response.text.strip()
    try:
        return int(text)
    except ValueError:
        logger.error("Timestamp is not an integer: %s", text)
        return None


def download_csv(logger: logging.Logger) -> bytes | None:
    url = urljoin(BASE_URL, CSV_FILENAME)
    logger.info("Downloading CSV from %s", url)
    try:
        response = requests.get(url, auth=get_auth(logger), timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.content
    except requests.RequestException as exc:
        logger.error("Failed to download CSV: %s", exc)
        return None


def cleanup_timestamped_downloads(logger: logging.Logger, download_dir: Path) -> None:
    for path in download_dir.glob(f"*__{CSV_FILENAME}"):
        try:
            path.unlink()
            logger.info("Removed old download %s", path)
        except OSError as exc:
            logger.warning("Failed to remove old download %s: %s", path, exc)


def check_for_update(logger: logging.Logger, download_dir: Path) -> Path | None:
    timestamp = fetch_timestamp(logger)
    if timestamp is None:
        return None

    utc_formatted = datetime.datetime.utcfromtimestamp(timestamp).strftime("%d/%m/%Y %H:%M:%S")
    output_filename = CSV_FILENAME
    if "coliminder" not in output_filename.lower():
        output_filename = f"raw_data_ColiMinder_{timestamp}.csv"
    output_path = download_dir / output_filename

    last_timestamp_text = read_state_text(STATE_DIR / "last_timestamp.txt")
    last_timestamp = None
    if last_timestamp_text:
        try:
            last_timestamp = int(last_timestamp_text)
        except ValueError:
            logger.warning("Invalid last_timestamp.txt contents, treating as missing.")

    if last_timestamp == timestamp and output_path.exists():
        logger.info("No update. Timestamp unchanged at %s (%s UTC).", timestamp, utc_formatted)
        return None

    logger.info("UPDATE detected: %s (%s UTC).", timestamp, utc_formatted)

    first_content = download_csv(logger)
    if first_content is None:
        return None
    first_hash = sha256_bytes(first_content)
    logger.info("First download size=%d bytes hash=%s", len(first_content), first_hash)

    time.sleep(PARTIAL_DOWNLOAD_DELAY_SECONDS)

    second_content = download_csv(logger)
    if second_content is None:
        return None
    second_hash = sha256_bytes(second_content)
    logger.info("Second download size=%d bytes hash=%s", len(second_content), second_hash)

    if first_hash != second_hash:
        logger.warning("Hashes differ between downloads; using latest version anyway.")
    stable_content = second_content
    stable_hash = second_hash

    last_csv_hash = read_state_text(STATE_DIR / "last_csv_hash.txt")
    if last_csv_hash == stable_hash:
        if output_path.exists():
            logger.info("Content unchanged, skipping save.")
        else:
            output_path.write_bytes(stable_content)
            logger.info("Content unchanged but no file found; saved %s (hash=%s)", output_path, stable_hash[:12])
        cleanup_timestamped_downloads(logger, download_dir)
        write_state_text(STATE_DIR / "last_timestamp.txt", str(timestamp))
        write_state_text(STATE_DIR / "last_csv_hash.txt", stable_hash)
        return output_path if output_path.exists() else None

    cleanup_timestamped_downloads(logger, download_dir)
    output_path.write_bytes(stable_content)
    logger.info("Saved %s (hash=%s)", output_path, stable_hash[:12])

    write_state_text(STATE_DIR / "last_timestamp.txt", str(timestamp))
    write_state_text(STATE_DIR / "last_csv_hash.txt", stable_hash)
    return output_path


def fetch_coliminder_once(
    logger: logging.Logger | None = None,
    output_dir: str | Path | None = None,
) -> Path | None:
    download_dir = Path(output_dir) if output_dir else DOWNLOADS_DIR
    ensure_dirs(download_dir)
    if logger is None:
        logger = build_logger(str(LOG_FILE))
    return check_for_update(logger, download_dir)
