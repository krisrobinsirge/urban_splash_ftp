import os
import threading
import traceback
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from pathlib import Path

class AzureUploader:
    """Handles uploads to Azure Blob Storage with SAS token or Managed Identity,
    supports dynamic container selection."""

    ## we will likely need to prefix the blobs with a site id or something
    def __init__(self, default_container="data"):
        # Load environment variables
        self.STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")
        self.CONTAINER_NAME_DEFAULT = os.getenv("CONTAINER_NAME", default_container)
        self.SAS_TOKEN = os.getenv("SAS_TOKEN", "").strip()

        print(self.STORAGE_ACCOUNT_NAME, self.CONTAINER_NAME_DEFAULT, self.SAS_TOKEN, flush=True)

        account_url = f"https://{self.STORAGE_ACCOUNT_NAME}.blob.core.windows.net"

        if self.SAS_TOKEN:
            # Ensure leading '?'
            if not self.SAS_TOKEN.startswith("?"):
                self.SAS_TOKEN = "?" + self.SAS_TOKEN
            print(f"[INFO] Using RAW SAS token for Azure Blob", flush=True)
            self.raw_blob_service_client = BlobServiceClient(account_url, credential=self.SAS_TOKEN)

        else:
            # Use Managed Identity / DefaultAzureCredential
            print(f"[INFO] Using DefaultAzureCredential (Managed Identity) for Azure Blob", flush=True)
            credential = DefaultAzureCredential()
            self.blob_service_client = BlobServiceClient(account_url, credential=credential)

    def upload_file(self, file_path,  file_type, site=None):
        """Uploads a file in a separate daemon thread.
            container_name: optional, defaults to self.CONTAINER_NAME_DEFAULT
            site: the site where data was recieved from
            file_type:  raw, clean, flagged

        """
        if site:
            target_container = self.CONTAINER_NAME_DEFAULT
            thread = threading.Thread(target=self._upload, args=(file_path, target_container, site, file_type), daemon=True)
            thread.start()
        else:
            print(f"[ERROR] Unknown site '{site}'", flush=True)

    def _upload(self, file_path, container_name, site, file_type):
        """
        Upload file to Azure Blob Storage.

        Resulting path:
        data/<site>/<raw|clean|flagged>/<filename>
        """
        filename = os.path.basename(file_path)
        blob_name = f"{site}/{file_type}/{filename}"

        print(
            f"[INFO] Uploading {file_path} -> "
            f"container='{container_name}', blob='{blob_name}'",
            flush=True
        )

        if container_name != "data":
            print(f"[ERROR] Unknown container '{container_name}'", flush=True)
            return

        try:
            container_client = self.raw_blob_service_client.get_container_client(container_name)

            with open(file_path, "rb") as data:
                container_client.upload_blob(
                    name=blob_name,
                    data=data,
                    overwrite=True
                )

            print(
                f"[SUCCESS] Uploaded to data/{blob_name}",
                flush=True
            )

        except Exception as e:
            print(
                f"[ERROR] Failed to upload {filename} to '{container_name}': {e}",
                flush=True
            )
            traceback.print_exc()

