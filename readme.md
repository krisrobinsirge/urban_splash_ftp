# Urban Splash Data Platform (Basic)

## 🚚 FTP Server Component

This component provides a lightweight, container-friendly FTP server used to receive inbound files from external systems.  
It is built using **pyftpdlib**, a pure-Python FTP implementation that allows the application to trigger custom Python logic whenever a file is received.

### ✨ Key Features

- **Embedded FTP server** (no external FTP daemon required)
- **Python callback hook** that fires immediately after an upload completes
- **Files stored temporarily** in the `uploads/` directory before further processing
- **Supports both authenticated and anonymous FTP connections**
- **Passive FTP ports** predefined for clean container + firewall behaviour
- Runs easily **locally** or **inside a Docker container**

---

## 🧰 How It Works

1. An FTP client connects to the server on **port 2121**  

2. Uploaded files are written to the working directory.

3. Files are automatically moved into the `/uploads` directory.

4. A Python callback (`on_file_received`) is triggered whenever a file is fully uploaded in UploadFTPHandler

5. This callback triggers the full processing pipeline (see "Processing Pipeline").

---

## 📁 Directory Structure

---

## ▶ Running the FTP Server (Local)


Install dependencies:
cd ftp_server
pip install -r requirements.txt // works differently in Windows (python -m  pip install -r requirements.txt)

Start the server from root directory

python -m ftp_server.server

Expected output:

FTP server running on port 2121
Connect with: ftp://user:12345@localhost:2121

## Testing the FTP Server

In a separate terminal, cd into tests and connect via FTP:

ftp localhost 2121 // works differently in Windows (ftp > open localhost 2121)

expected output:  
Connected to localhost.
220 pyftpdlib 2.1.0 ready.


When prompted, enter the credentials in the .env file

expected output:
230 Login successful.
Remote system type is UNIX.
Using binary mode to transfer files.

Upload a file:

ftp> put test_file.txt
ftp> quit

✔ Expected Behaviour

The uploaded file appears inside the uploads/ directory.

The server console prints a message similar to:

>>> CALLBACK FIRED
File received: uploads/test_file.txt (size: 128 bytes)


This confirms the FTP server is receiving files and the callback hook is working.


### Docker

.env files are cahced and do not always update on runs. 
remove the container if need be with docker rm -f ftp-server

docker build --no-cache -t ftp-server .

docker build -t ftp-server .
docker run --name ftp-server --env-file .env \
  -p 2121:2121 -p 30000-30010:30000-30010 \
  -v $(pwd)/uploads:/app/uploads \
  -v $(pwd)/raw_input:/app/raw_input \
  -v $(pwd)/output_data:/app/output_data \
  -v $(pwd)/archive:/app/archive \
  ftp-server



- push to azure container registry
docker login rightstep.azurecr.io
docker build -t rightstep.azurecr.io/urban_splash_ftp_server_v1.0 .
docker push rightstep.azurecr.io/urban_splash_ft_server_v1.0 

### run on vm
docker pull rightstep.azurecr.io/urban_splash_ftp_server_v1.0
docker run --name ftp-server --env-file /path/to/.env \
  -p 2121:2121 -p 30000-30010:30000-30010 \
  rightstep.azurecr.io/urban_splash_ftp_server_v1.0



## Azure cli
install:  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

az container logs --resource-group UrbanSplash --name us-ftp-server --container-name us-ftp-server
az container exec --resource-group UrbanSplash --name us-ftp-server --container-name us-ftp-server --exec-command "/bin/sh"


## Deployment

currently the ACI deployment is working but it is updating the system will create a new external IP (not ideal as changes sensor server configs)
the user name and password needs managing better 

latest deployment is for ACA without the env


## Data Cleaning Process

## Processing Pipeline

Triggered on every FTP upload:

1. Incoming Observator file lands in `uploads/`.
2. The file is copied into `raw_input/`.
3. ColiMinder fetcher runs and writes a ColiMinder CSV into `raw_input/`.
4. QC engine processes all files in `raw_input/`:
   - Outputs to `output_data/cleaned/` and `output_data/flagged/`.
5. Combiner runs on cleaned outputs and writes combined files to:
   - `output_data/combined/cleaned_and_combined_data_latest.csv`
   - `output_data/combined/cleaned_and_combined_and_aligned_data_latest.csv`
   - plus the two `*_general.csv` files for cumulative data
6. Files are uploaded to Azure:
   - Raw inputs -> `file_type=raw`
   - Cleaned/flagged -> `file_type=clean` / `file_type=flagged`
   - Combined outputs -> `file_type=combined`
7. On full success, `uploads/`, `raw_input/`, and `output_data/` are cleared.
8. On failed uploads, files are moved to `archive/<type>/`.

## ColiMinder Fetcher

The ColiMinder fetcher lives in `coliminder_fetcher/` and is configured via `.env`:

- `COLIMINDER_BASE_URL`
- `COLIMINDER_TIMESTAMP_FILENAME`
- `COLIMINDER_CSV_FILENAME`
- `COLIMINDER_USE_BASIC_AUTH`
- `COLIMINDER_BASIC_AUTH_USERNAME`
- `COLIMINDER_BASIC_AUTH_PASSWORD`
- `COLIMINDER_PARTIAL_DOWNLOAD_DELAY_SECONDS`
- `COLIMINDER_REQUEST_TIMEOUT_SECONDS`

#### Stage 2
- combine the two data sources (this relies on recieving the data at a similar time and also tracking what has been processed already)
