import datetime
import hashlib
import logging
import time
from pathlib import Path
from urllib.parse import urljoin

import requests

# ==============================
# Configuration
# ==============================
BASE_URL = "http://vpn.vwm.solutions:8005/Leander/"
TIMESTAMP_FILENAME = "timestamp.txt"
CSV_FILENAME = "results_red_V01_04.csv"

USE_BASIC_AUTH = True
BASIC_AUTH_USERNAME = "leander_query"
BASIC_AUTH_PASSWORD = "WpzHjERPgG"

PARTIAL_DOWNLOAD_DELAY_SECONDS = 5
REQUEST_TIMEOUT_SECONDS = 60

if not BASE_URL.endswith("/"):
    BASE_URL += "/"

PROJECT_ROOT = Path(__file__).resolve().parent
DOWNLOADS_DIR = PROJECT_ROOT / "downloads"
STATE_DIR = PROJECT_ROOT / "state"
LOG_FILE = PROJECT_ROOT / "coliminder_fetch.log"


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("coliminder_fetcher")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger


def ensure_dirs() -> None:
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)


def parse_bool(prompt: str, default: bool) -> bool:
    true_values = {"true", "t", "yes", "y", "1"}
    false_values = {"false", "f", "no", "n", "0"}
    while True:
        value = input(prompt).strip().lower()
        if value == "":
            return default
        if value in true_values:
            return True
        if value in false_values:
            return False
        print("Please enter true/false.")


def parse_int(prompt: str, default: int) -> int:
    while True:
        value = input(prompt).strip()
        if value == "":
            return default
        try:
            parsed = int(value)
            if parsed <= 0:
                raise ValueError("non-positive")
            return parsed
        except ValueError:
            print("Please enter a positive integer.")


def get_auth():
    if USE_BASIC_AUTH:
        return (BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD)
    return None


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
    url = urljoin(BASE_URL, TIMESTAMP_FILENAME)
    logger.info("Checking timestamp at %s", url)
    try:
        response = requests.get(url, auth=get_auth(), timeout=REQUEST_TIMEOUT_SECONDS)
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
        response = requests.get(url, auth=get_auth(), timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.content
    except requests.RequestException as exc:
        logger.error("Failed to download CSV: %s", exc)
        return None


def cleanup_timestamped_downloads(logger: logging.Logger) -> None:
    for path in DOWNLOADS_DIR.glob(f"*__{CSV_FILENAME}"):
        try:
            path.unlink()
            logger.info("Removed old download %s", path)
        except OSError as exc:
            logger.warning("Failed to remove old download %s: %s", path, exc)


def check_for_update(logger: logging.Logger) -> None:
    timestamp = fetch_timestamp(logger)
    if timestamp is None:
        return

    utc_formatted = datetime.datetime.utcfromtimestamp(timestamp).strftime("%d/%m/%Y %H:%M:%S")
    last_timestamp_text = read_state_text(STATE_DIR / "last_timestamp.txt")
    last_timestamp = None
    if last_timestamp_text:
        try:
            last_timestamp = int(last_timestamp_text)
        except ValueError:
            logger.warning("Invalid last_timestamp.txt contents, treating as missing.")

    if last_timestamp == timestamp:
        logger.info("No update. Timestamp unchanged at %s (%s UTC).", timestamp, utc_formatted)
        return

    logger.info("UPDATE detected: %s (%s UTC).", timestamp, utc_formatted)

    first_content = download_csv(logger)
    if first_content is None:
        return
    first_hash = sha256_bytes(first_content)
    logger.info("First download size=%d bytes hash=%s", len(first_content), first_hash)

    time.sleep(PARTIAL_DOWNLOAD_DELAY_SECONDS)

    second_content = download_csv(logger)
    if second_content is None:
        return
    second_hash = sha256_bytes(second_content)
    logger.info("Second download size=%d bytes hash=%s", len(second_content), second_hash)

    if first_hash != second_hash:
        logger.warning("Hashes differ between downloads; using latest version anyway.")
    stable_content = second_content
    stable_hash = second_hash

    last_csv_hash = read_state_text(STATE_DIR / "last_csv_hash.txt")
    output_path = DOWNLOADS_DIR / CSV_FILENAME

    if last_csv_hash == stable_hash:
        if output_path.exists():
            logger.info("Content unchanged, skipping save.")
        else:
            output_path.write_bytes(stable_content)
            logger.info("Content unchanged but no file found; saved %s (hash=%s)", output_path, stable_hash[:12])
        cleanup_timestamped_downloads(logger)
        write_state_text(STATE_DIR / "last_timestamp.txt", str(timestamp))
        write_state_text(STATE_DIR / "last_csv_hash.txt", stable_hash)
        return

    cleanup_timestamped_downloads(logger)
    output_path.write_bytes(stable_content)
    logger.info("Saved %s (hash=%s)", output_path, stable_hash[:12])

    write_state_text(STATE_DIR / "last_timestamp.txt", str(timestamp))
    write_state_text(STATE_DIR / "last_csv_hash.txt", stable_hash)


def main() -> None:
    ensure_dirs()
    logger = setup_logging()

    run_once = parse_bool("Run once? (true/false) [true]: ", True)
    poll_interval = parse_int("Poll interval seconds? (e.g., 300) [300]: ", 300)

    mode_text = "once" if run_once else "loop"
    logger.info("Script start. Mode=%s PollInterval=%s seconds", mode_text, poll_interval)

    try:
        if run_once:
            check_for_update(logger)
            return

        while True:
            check_for_update(logger)
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Interrupted by user; exiting.")
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)


if __name__ == "__main__":
    main()
