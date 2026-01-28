FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire codebase (uploads excluded via .dockerignore)
COPY . .

# Create non-root user and ensure runtime directories exist
RUN useradd -m -u 10001 appuser \
    && mkdir -p /app/uploads /app/raw_input /app/output_data /app/archive /app/logs \
    && chown -R appuser:appuser /app

EXPOSE 2121
EXPOSE 30000-30010

# Run as non-root
USER 10001

# Run as module
CMD ["python", "-m", "ftp_server.server"]
