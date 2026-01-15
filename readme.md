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

5. This callback can:
   - Log file details  
   - Perform parsing, validation, or enrichment  
   - Trigger downstream processes  
   - Upload to cloud storage (Azure - raw and clean containers)

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
docker run --name ftp-server --env-file .env -p 2121:2121 -p 30000-30010:30000-30010 -v $(pwd)/uploads:/app/uploads ftp-server

- push to azure container registry
docker login rightstep.azurecr.io
docker build -t rightstep.azurecr.io/urban_splash_ft_server_v1.0 .
docker push rightstep.azurecr.io/urban_splash_ft_server_v1.0 


## Azure cli
install:  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

az container logs --resource-group UrbanSplash --name us-ftp-server --container-name us-ftp-server
az container exec --resource-group UrbanSplash --name us-ftp-server --container-name us-ftp-server --exec-command "/bin/sh"


## Deployment

currently the ACI deployment is working but it is updating the system will create a new external IP (not ideal as changes sensor server configs)
the user name and password needs managing better 

latest deployment is for ACA without the env


## Data Cleaning Process

- raw data from both servers are recieved
- Files are named -> raw_data_Observator_ddmmyyyy_to_ddmmyyyy  (this should be done at source really)
- step 1: flag data -> output as flagged csv currently but likely ok to be in memory
- step 2: clean the flagged data
- step 3: create csv and upload

#### Stage 2
- combine the two data sources (this relies on recieving the data at a similar time and also tracking what has been processed already)