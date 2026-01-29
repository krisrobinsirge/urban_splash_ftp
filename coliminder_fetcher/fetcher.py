from __future__ import annotations

import datetime
import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass
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

PROJECT_ROOT = Path(__file__).resolve().parent
DOWNLOADS_DIR = Path("raw_input")
STATE_DIR = PROJECT_ROOT / "state"
LOG_FILE = Path("logs") / "coliminder_fetch.log"

@dataclass(frozen=True)
class ColiminderConfig:
    base_url: str
    timestamp_filename: str
    csv_filename: str
    use_basic_auth: bool
    basic_auth_username: str
    basic_auth_password: str
    partial_download_delay_seconds: int
    request_timeout_seconds: int
    state_dir: Path
    site_key: str | None


def _normalize_site_key(site: str | None) -> str | None:
    if not site:
        return None
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", site.strip())
    cleaned = cleaned.strip("_").upper()
    return cleaned or None


def _get_site_env(site_key: str | None, suffix: str, default: str = "") -> str:
    if site_key:
        value = os.getenv(f"COLIMINDER_{site_key}_{suffix}")
        if value:
            return value.strip()
    return os.getenv(f"COLIMINDER_{suffix}", default).strip()


def build_config(site: str | None) -> ColiminderConfig:
    site_key = _normalize_site_key(site)
    base_url = _get_site_env(site_key, "BASE_URL", BASE_URL)
    timestamp_filename = _get_site_env(site_key, "TIMESTAMP_FILENAME", TIMESTAMP_FILENAME)
    csv_filename = _get_site_env(site_key, "CSV_FILENAME", CSV_FILENAME)
    use_basic_auth = _get_site_env(site_key, "USE_BASIC_AUTH", str(USE_BASIC_AUTH)).lower() == "true"
    basic_auth_username = _get_site_env(site_key, "BASIC_AUTH_USERNAME", BASIC_AUTH_USERNAME)
    basic_auth_password = _get_site_env(site_key, "BASIC_AUTH_PASSWORD", BASIC_AUTH_PASSWORD)
    partial_delay = int(_get_site_env(site_key, "PARTIAL_DOWNLOAD_DELAY_SECONDS", str(PARTIAL_DOWNLOAD_DELAY_SECONDS)) or "5")
    request_timeout = int(_get_site_env(site_key, "REQUEST_TIMEOUT_SECONDS", str(REQUEST_TIMEOUT_SECONDS)) or "60")
    state_dir = STATE_DIR / (site_key.lower() if site_key else "default")

    if base_url and not base_url.endswith("/"):
        base_url += "/"

    return ColiminderConfig(
        base_url=base_url,
        timestamp_filename=timestamp_filename,
        csv_filename=csv_filename,
        use_basic_auth=use_basic_auth,
        basic_auth_username=basic_auth_username,
        basic_auth_password=basic_auth_password,
        partial_download_delay_seconds=partial_delay,
        request_timeout_seconds=request_timeout,
        state_dir=state_dir,
        site_key=site_key,
    )


def ensure_dirs(download_dir: Path, state_dir: Path) -> None:
    download_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)


def get_auth(logger: logging.Logger, config: ColiminderConfig):
    if not config.use_basic_auth:
        return None
    if not config.basic_auth_username or not config.basic_auth_password:
        logger.error("Basic auth is enabled but username/password are missing.")
        return None
    return (config.basic_auth_username, config.basic_auth_password)


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


def fetch_timestamp(logger: logging.Logger, config: ColiminderConfig) -> int | None:
    if not config.base_url:
        logger.error("COLIMINDER_BASE_URL is not configured.")
        return None

    url = urljoin(config.base_url, config.timestamp_filename)
    logger.info("Checking timestamp at %s", url)
    try:
        response = requests.get(url, auth=get_auth(logger, config), timeout=config.request_timeout_seconds)
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


def download_csv(logger: logging.Logger, config: ColiminderConfig) -> bytes | None:
    url = urljoin(config.base_url, config.csv_filename)
    logger.info("Downloading CSV from %s", url)
    try:
        response = requests.get(url, auth=get_auth(logger, config), timeout=config.request_timeout_seconds)
        response.raise_for_status()
        return response.content
    except requests.RequestException as exc:
        logger.error("Failed to download CSV: %s", exc)
        return None


def cleanup_timestamped_downloads(logger: logging.Logger, download_dir: Path, csv_filename: str) -> None:
    for path in download_dir.glob(f"*__{csv_filename}"):
        try:
            path.unlink()
            logger.info("Removed old download %s", path)
        except OSError as exc:
            logger.warning("Failed to remove old download %s: %s", path, exc)


def check_for_update(logger: logging.Logger, download_dir: Path, config: ColiminderConfig) -> Path | None:
    timestamp = fetch_timestamp(logger, config)
    if timestamp is None:
        return None

    utc_formatted = datetime.datetime.utcfromtimestamp(timestamp).strftime("%d/%m/%Y %H:%M:%S")
    output_filename = config.csv_filename
    site_label = (config.site_key or "coliminder").lower()
    if "coliminder" not in output_filename.lower():
        output_filename = f"raw_data_ColiMinder_{site_label}_{timestamp}.csv"
    elif config.site_key:
        stem = Path(output_filename).stem
        output_filename = f"{stem}_{site_label}{Path(output_filename).suffix}"
    output_path = download_dir / output_filename

    last_timestamp_text = read_state_text(config.state_dir / "last_timestamp.txt")
    last_timestamp = None
    if last_timestamp_text:
        try:
            last_timestamp = int(last_timestamp_text)
        except ValueError:
            logger.warning("Invalid last_timestamp.txt contents, treating as missing.")

    if last_timestamp == timestamp and output_path.exists():
        logger.info("No update. Timestamp unchanged at %s (%s UTC).", timestamp, utc_formatted)
        return output_path

    logger.info("UPDATE detected: %s (%s UTC).", timestamp, utc_formatted)

    first_content = download_csv(logger, config)
    if first_content is None:
        return None
    first_hash = sha256_bytes(first_content)
    logger.info("First download size=%d bytes hash=%s", len(first_content), first_hash)

    time.sleep(config.partial_download_delay_seconds)

    second_content = download_csv(logger, config)
    if second_content is None:
        return None
    second_hash = sha256_bytes(second_content)
    logger.info("Second download size=%d bytes hash=%s", len(second_content), second_hash)

    if first_hash != second_hash:
        logger.warning("Hashes differ between downloads; using latest version anyway.")
    stable_content = second_content
    stable_hash = second_hash

    last_csv_hash = read_state_text(config.state_dir / "last_csv_hash.txt")
    if last_csv_hash == stable_hash:
        if output_path.exists():
            logger.info("Content unchanged, skipping save.")
        else:
            output_path.write_bytes(stable_content)
            logger.info("Content unchanged but no file found; saved %s (hash=%s)", output_path, stable_hash[:12])
        cleanup_timestamped_downloads(logger, download_dir, config.csv_filename)
        write_state_text(config.state_dir / "last_timestamp.txt", str(timestamp))
        write_state_text(config.state_dir / "last_csv_hash.txt", stable_hash)
        return output_path if output_path.exists() else output_path

    cleanup_timestamped_downloads(logger, download_dir, config.csv_filename)
    output_path.write_bytes(stable_content)
    logger.info("Saved %s (hash=%s)", output_path, stable_hash[:12])

    write_state_text(config.state_dir / "last_timestamp.txt", str(timestamp))
    write_state_text(config.state_dir / "last_csv_hash.txt", stable_hash)
    return output_path


def fetch_coliminder_once(
    logger: logging.Logger | None = None,
    output_dir: str | Path | None = None,
    site: str | None = None,
) -> Path | None:
    download_dir = Path(output_dir) if output_dir else DOWNLOADS_DIR
    config = build_config(site)
    ensure_dirs(download_dir, config.state_dir)
    if logger is None:
        logger = build_logger(str(LOG_FILE))
    return check_for_update(logger, download_dir, config)
