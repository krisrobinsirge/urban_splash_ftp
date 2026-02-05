from __future__ import annotations
import asyncio
import os
import shutil
from datetime import datetime
from queue import Queue
from threading import Thread
from dotenv import load_dotenv
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
from pyftpdlib.authorizers import DummyAuthorizer
from pathlib import Path
from logger.logger import build_logger

from processor.qc_engine import QCEngine
from uploader.azure_uploader import AzureUploader  # import Azure uploader
from coliminder_fetcher.fetcher import fetch_coliminder_once
from processor.coliminder_merge import merge_coliminder_into_file
from processor.file_funcs import extract_site_from_filename, get_raw_file


## notes:
# secure against brute force login 
# handler.max_login_attempts = 3
# handler.timeout = 30
# disable active mode via the callbacks 
#def ftp_PORT(self, line):
#    self.log("Blocked active FTP request: PORT %s" % line)
#    self.respond("502 Active mode not supported. Use PASV.")

#def ftp_EPRT(self, line):

#    self.respond("502 Active mode not supported. Use PASV.")


# -- Load environment from .env -- #
load_dotenv()

FTP_USERNAME = os.getenv("FTP_USERNAME", None)
FTP_PASSWORD = os.getenv("FTP_PASSWORD", None)
FTP_PORT = int(os.getenv("FTP_PORT", "2121"))
FTP_MASQUERADE_ADDRESS = os.getenv("FTP_MASQUERADE_ADDRESS", None)
FTP_PASSIVE_PORTS = os.getenv("FTP_PASSIVE_PORTS", "30000-30010")
ENABLE_AZURE_UPLOADS = os.getenv("ENABLE_AZURE_UPLOADS", "false").strip().lower() == "true"

UPLOAD_DIR = "uploads"
RAW_INPUT_DIR = "raw_input"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(RAW_INPUT_DIR, exist_ok=True)

processing_queue: "Queue[str]" = Queue()

def start_processing_worker(engine: QCEngine, uploader: AzureUploader, logger) -> None:
    def worker():
        while True:
            file_path = processing_queue.get()
            try:
                process_data(engine, uploader, UPLOAD_DIR, RAW_INPUT_DIR, logger, file_path)
            finally:
                processing_queue.task_done()
    Thread(target=worker, daemon=True).start()

# ftp trigger on recieved file in the /uploads directory
# NOTE: this could be generic for other uploaders if needed
class UploadFTPHandler(FTPHandler):
    uploader: AzureUploader = None
    upload_dir: str = None
    qc_engine: QCEngine | None = None
    logger = None

    def on_file_received(self, file_path):
        # Keep as debug (you'll see temp.$$$ here)
        self.log(f"[DEBUG] on_file_received: {file_path}")

    def ftp_RNFR(self, path):
        # remember source for logging / debugging
        self._last_rnfr = path
        return super().ftp_RNFR(path)

    def ftp_RNTO(self, path):
        # path is the *destination* (final name) requested by client
        src = getattr(self, "_last_rnfr", None)
        dst = path

        # perform the rename first
        ret = super().ftp_RNTO(path)

        # If RNTO succeeded, client gets 250 (you already see that in logs).
        # Now queue processing ONLY for final CSV.
        if dst and dst.lower().endswith(".csv"):
            final_path = os.path.join(self.upload_dir, os.path.basename(dst))

            # In your logs dst is already absolute (/app/uploads/...), but normalize anyway.
            if os.path.abspath(dst) != os.path.abspath(final_path):
                os.rename(dst, final_path)
            else:
                final_path = dst

            self.log(f"[INFO] Rename complete: {src} -> {final_path}")
            processing_queue.put(final_path)

        return ret


    
# -- Testing the processing and upload -- #
def run_once(engine: QCEngine, file_path: str):
    processed_files = engine.process_file(file_path)
    return processed_files or []

def archive_file(file_path: str, file_type: str, archive_root: str = "archive") -> str:
    archive_dir = Path(archive_root) / file_type
    archive_dir.mkdir(parents=True, exist_ok=True)

    src = Path(file_path)
    target = archive_dir / src.name
    if target.exists():
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        target = archive_dir / f"{src.stem}_{stamp}{src.suffix}"

    shutil.move(str(src), str(target))
    return str(target)


def clear_directory(path: str) -> None:
    root = Path(path)
    if not root.exists():
        return
    for child in root.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()

# this copies the uploaded file to an input directory - it is only observator at the moment 
# as that is the only one that is FTP
def copy_raw_input(file_path: str, raw_input_dir: str) -> Path:
    raw_input_path = Path(raw_input_dir)
    raw_input_path.mkdir(parents=True, exist_ok=True)
    target = raw_input_path / Path(file_path).name
    shutil.copy2(file_path, target)
    return target


def get_site_from_filename(file_path: str) -> str:
    site = extract_site_from_filename(file_path)
    return site or "unknown"


def process_data(
    engine: QCEngine,
    uploader: AzureUploader,
    upload_dir: str,
    raw_input_dir: str,
    logger,
    ftp_file_path: str,
):
    # potential issue is data from different sites are recieved at the same time
    ''' 
        test the processor with upload
        place observator file to process in uploads directory prior to running the test
        recieved files are copied to raw_inputs
        colliminder is fetched from "api" and placed in raw_inputs

    '''
    if not ftp_file_path or not os.path.exists(ftp_file_path):
        logger.warning("No FTP file path provided; skipping processing.")
        return []

    raw_input_path = copy_raw_input(ftp_file_path, raw_input_dir)

    site = get_site_from_filename(raw_input_path)
    print(f"[INFO] Processing received data from {site}", flush=True)

    print(f"[INFO] Fetching Coliminder data for {site}", flush=True)
    fetched_path = fetch_coliminder_once(logger, output_dir=raw_input_dir, site=site)
    merged = merge_coliminder_into_file(raw_input_path, fetched_path, logger=logger)
    if not merged:
        print("[INFO] No Coliminder rows merged (columns left empty).", flush=True)

    # copy updated raw file back to uploads so it is the combined file
    target_upload = Path(upload_dir) / raw_input_path.name
    shutil.copy2(raw_input_path, target_upload)

    processed_files = run_once(engine, str(raw_input_path))

    # -- dont combine anything yet
    #print("[INFO] combining cleaned data")
    #combined_outputs = combine_cleaned()

    if ENABLE_AZURE_UPLOADS:
        upload_jobs = []
        upload_jobs.append((raw_input_path, "raw"))

        for output in processed_files:
            path = Path(output)
            blob_path = path.relative_to("output_data")
            file_type = blob_path.parts[0]   # raw | clean | flagged
            print(f"[INFO] uploading processed files for site {site}", output, file_type)
            upload_jobs.append((output, file_type))

        async def run_uploads():
            tasks = []
            for file_path, file_type in upload_jobs:
                task = asyncio.to_thread(
                    uploader.upload_file,
                    str(file_path),
                    file_type,
                    site,
                    True,
                )
                tasks.append(task)
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results

        results = asyncio.run(run_uploads()) if upload_jobs else []
        all_success = True
        for (file_path, file_type), result in zip(upload_jobs, results, strict=False):
            ok = False if isinstance(result, Exception) else bool(result)
            if not ok:
                archive_file(str(file_path), file_type)
                all_success = False

        if all_success:
            clear_directory(raw_input_dir)
            clear_directory(upload_dir)
    else:
        clear_directory(raw_input_dir)
        clear_directory(upload_dir)

    return processed_files
        

def main():
    # create a logger
    log_path = os.path.join("logs", "watchdog.log")
    logger = build_logger(log_path)
    # Initialize Azure uploader - default is raw container
    uploader = AzureUploader()
    # create a QC engine - inject this into the FTP handler later
    engine = QCEngine(config_path="processor/dq_master.yaml", upload_dir=RAW_INPUT_DIR, logger=logger)

    # -- test data processing (comment out when live) -- #
    seed_path = get_raw_file(UPLOAD_DIR, logger=logger)
    if seed_path:
        process_data(engine, uploader, UPLOAD_DIR, RAW_INPUT_DIR, logger, seed_path)

    authorizer = DummyAuthorizer()
    authorizer.add_user(
        FTP_USERNAME,
        FTP_PASSWORD,
        homedir=UPLOAD_DIR,
        perm="elwmfd"
    )

    handler = UploadFTPHandler
    handler.authorizer = authorizer
    port_start, port_end = FTP_PASSIVE_PORTS.split("-", 1)
    handler.passive_ports = range(int(port_start), int(port_end) + 1)

    if FTP_MASQUERADE_ADDRESS:
        handler.masquerade_address = FTP_MASQUERADE_ADDRESS

    handler.uploader = uploader
    handler.upload_dir = UPLOAD_DIR
    handler.qc_engine = engine
    handler.logger = logger

    start_processing_worker(engine, uploader, logger)

    # -- Start the FTP Server -- #
    # probably need to specify the address?
    server = FTPServer(("0.0.0.0", FTP_PORT), handler)
    print(f"[INFO] FTP Server running on port {FTP_PORT}...", flush=True)
    server.serve_forever()

if __name__ == "__main__":
    main()
