FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire codebase (uploads excluded via .dockerignore)
COPY . .

# Ensure runtime directories exist
RUN mkdir -p /app/uploads /app/raw_input /app/output_data /app/archive

EXPOSE 2121
EXPOSE 30000-30010

# Run as module
CMD ["python", "-m", "ftp_server.server"]
