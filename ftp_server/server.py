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
from data_combiner.combiner import combine_cleaned

# -- Load environment from .env -- #
load_dotenv()

FTP_USERNAME = os.getenv("FTP_USERNAME", None)
FTP_PASSWORD = os.getenv("FTP_PASSWORD", None)
FTP_PORT = int(os.getenv("FTP_PORT", "2121"))
FTP_MASQUERADE_ADDRESS = os.getenv("FTP_MASQUERADE_ADDRESS", None)
FTP_PASSIVE_PORTS = os.getenv("FTP_PASSIVE_PORTS", "30000-30010")

UPLOAD_DIR = "uploads"
RAW_INPUT_DIR = "raw_input"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(RAW_INPUT_DIR, exist_ok=True)

processing_queue: "Queue[None]" = Queue()

def start_processing_worker(engine: QCEngine, uploader: AzureUploader, logger) -> None:
    def worker():
        while True:
            processing_queue.get()
            try:
                process_data(engine, uploader, UPLOAD_DIR, RAW_INPUT_DIR, logger)
            finally:
                processing_queue.task_done()
    Thread(target=worker, daemon=True).start()

# ftp trigger on recieved file in the /uploads directory
# NOTE: this could be generic for other uploaders if needed
class UploadFTPHandler(FTPHandler):
    """Custom FTP handler that triggers Azure uploads."""

    uploader: AzureUploader = None
    upload_dir: str = None
    qc_engine: QCEngine | None = None
    logger = None

    def on_file_received(self, file_path):
        # Move file to uploads directory
        dest = os.path.join(self.upload_dir, os.path.basename(file_path))
        os.rename(file_path, dest)
        print(f"[INFO] File received: {dest}", flush=True)

        processing_queue.put(None)

    
# -- Testing the processing and upload -- #
def run_once(engine: QCEngine) :
        processed_files = engine.process_directory_once()  # do we need to do this or can we just process individual files?
        return processed_files

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

def copy_raw_inputs(upload_dir: str, raw_input_dir: str, logger):
    from processor.file_funcs import list_raw_files
    raw_input_path = Path(raw_input_dir)
    raw_input_path.mkdir(parents=True, exist_ok=True)
    for file_path in list_raw_files(upload_dir, logger=logger):
        target = raw_input_path / Path(file_path).name
        shutil.copy2(file_path, target)


def process_data(engine: QCEngine, uploader: AzureUploader, upload_dir: str, raw_input_dir: str, logger):
    ''' 
        test the processor with upload
        place observator file to process in uploads directory prior to running the test
        recieved files are copied to raw_inputs
        colliminder is fetched from "api" and placed in raw_inputs

    '''
    site = "temp_site"


    print(f"[INFO], preparing raw input data")
    copy_raw_inputs(upload_dir, raw_input_dir, logger)

    # fetch the coliminder data into raw_input
    print(f"[INFO], fetching coliminder data")
    fetched_path = fetch_coliminder_once(logger, output_dir=raw_input_dir)

    print(f"[INFO], processing recieved data")
    engine.input_dir = raw_input_dir
    processed_files = run_once(engine)

    print("[INFO] combining cleaned data")
    combined_outputs = combine_cleaned()

    # -- upload files to azure blob -- #
    upload_jobs = []

    # upload raw data from raw_input
    from processor.file_funcs import list_raw_files
    for file_path in list_raw_files(raw_input_dir, logger=logger):
        print("[INFO] uploading raw file:", file_path)
        upload_jobs.append((file_path, "raw"))

    # clean and flagged observator / coliminder data
    for output in processed_files:
        path = Path(output)
        blob_path = path.relative_to("output_data")
        file_type = blob_path.parts[0]   # raw | clean | flagged
        print("[INFO] uploading file", output, file_type)
        upload_jobs.append((output, file_type))

    for output in combined_outputs:
        print("[INFO] uploading combined file", output)
        upload_jobs.append((str(output), "combined"))

    async def run_uploads():
        tasks = []
        for file_path, file_type in upload_jobs:
            task = asyncio.to_thread(
                uploader.upload_file,
                file_path,
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
            archive_file(file_path, file_type)
            all_success = False

    if all_success:
        clear_directory(upload_dir)
        clear_directory(raw_input_dir)
        clear_directory("output_data")
    return processed_files, fetched_path
        

def main():
    # create a logger
    log_path = os.path.join("logs", "watchdog.log")
    logger = build_logger(log_path)
    # Initialize Azure uploader - default is raw container
    uploader = AzureUploader()
    # create a QC engine - inject this into the FTP handler later
    engine = QCEngine(config_path="processor/dq_master.yaml", upload_dir=RAW_INPUT_DIR, logger=logger)

    # -- test data processing (comment out when live) -- #
    #process_data(engine, uploader, UPLOAD_DIR, RAW_INPUT_DIR, logger)

    authorizer = DummyAuthorizer()
    authorizer.add_user(
        FTP_USERNAME,
        FTP_PASSWORD,
        homedir=UPLOAD_DIR,
        perm="elwm"
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
