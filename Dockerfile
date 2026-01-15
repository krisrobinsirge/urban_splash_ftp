FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire codebase (uploads excluded via .dockerignore)
COPY . .

# Ensure uploads exists at runtime
RUN mkdir -p /app/uploads

EXPOSE 2121
EXPOSE 30000-30010

# Run as module
CMD ["python", "-m", "ftp_server.server"]
