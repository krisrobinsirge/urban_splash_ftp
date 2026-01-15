from __future__ import annotations
import os
from threading import Thread
from dotenv import load_dotenv
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
from pyftpdlib.authorizers import DummyAuthorizer

from logger.logger import build_logger

from processor.qc_engine import QCEngine
from uploader.azure_uploader import AzureUploader  # import Azure uploader

# -- Load environment from .env -- #
load_dotenv()

FTP_USERNAME = os.getenv("FTP_USERNAME", None)
FTP_PASSWORD = os.getenv("FTP_PASSWORD", None)
FTP_PORT = int(os.getenv("FTP_PORT", "2121"))

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ftp trigger on recieved file in the /uploads directory
# NOTE: this could be generic for other uploaders if needed
class UploadFTPHandler(FTPHandler):
    """Custom FTP handler that triggers Azure uploads."""

    uploader: AzureUploader = None
    upload_dir: str = None
    qc_engine: QCEngine | None = None

    def on_file_received(self, file_path):
        # Move file to uploads directory
        dest = os.path.join(self.upload_dir, os.path.basename(file_path))
        os.rename(file_path, dest)
        print(f"[INFO] File received: {dest}", flush=True)

        # -- extract site -- #
        site = "temp_site" # dummy site for testing

        ###################
        ## should be a background job
        def background_job():
            # upload the raw file
            self.uploader.upload_file(dest, site=site, file_type="raw") # put in raw folder for the site

            if self.qc_engine:
                processed_files = self.qc_engine.process_directory_once()
                print(processed_files)

        Thread(target=background_job, daemon=True).start()

        # -- process clean files -- #
        # process the file with QC engine

        # -- upload clean files to the clean storage container -- #
        # uploader.upload_file(dest, container_name="clean")

    
# -- Testing the processing and upload -- #
def run_once(engine: QCEngine) :
        processed_files = engine.process_directory_once()  # do we need to do this or can we just process individual files?
        return processed_files

def test_processor(engine: QCEngine, uploader: AzureUploader, upload_dir: str, logger):
    ''' 
        test the processor with upload
        place files to process in uploads directory prior to running the test

    '''
    site = "temp_site"

    print(f"[INFO], testing the processor")
    processed_files = run_once(engine)

    # -- upload files to azure blob -- #

    # raw - SUCCESS
    from processor.file_funcs import list_raw_files
    for file_path in list_raw_files(upload_dir, logger=logger):
        print("[INFO] uploading raw file:", file_path)
        uploader.upload_file(file_path, site=site, file_type="raw")

    # clean and flagged
    for output in processed_files:
        print("[INFO] uploading file", output)
        file_type = output.split('/')[0]
        uploader.upload_file(output, site=site, file_type=file_type)
        

def main():
    # create a logger
    log_path = os.path.join("../logs", "watchdog.log")
    logger = build_logger(log_path)
    # Initialize Azure uploader - default is raw container
    uploader = AzureUploader()
    # create a QC engine - inject this into the FTP handler later
    engine = QCEngine(config_path="processor/dq_master.yaml", upload_dir=UPLOAD_DIR, logger=logger)

    # -- test the processor (comment out when live) -- #
    test_processor(engine, uploader, UPLOAD_DIR, logger)

    authorizer = DummyAuthorizer()
    authorizer.add_user(
        FTP_USERNAME,
        FTP_PASSWORD,
        homedir=UPLOAD_DIR,
        perm="elradfmw"
    )

    handler = UploadFTPHandler
    handler.authorizer = authorizer
    handler.passive_ports = range(30000, 30010)


    #handler.masquerade_address = "127.0.0.1"  # Local testing
    handler.masquerade_address = "us-ftp-server.uksouth.azurecontainer.io"

    handler.uploader = uploader
    handler.upload_dir = UPLOAD_DIR
    handler.qc_engine = engine

    # -- Start the FTP Server -- #
    # probably need to specify the address?
    server = FTPServer(("0.0.0.0", FTP_PORT), handler)
    print(f"[INFO] FTP Server running on port {FTP_PORT}...", flush=True)
    server.serve_forever()

if __name__ == "__main__":
    main()
