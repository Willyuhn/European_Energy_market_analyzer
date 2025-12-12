# Use Python 3.11 slim image for smaller footprint
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .

# Copy scripts folder (for daily update functionality)
COPY scripts/ ./scripts/

# Copy static folder (for profile picture and other assets)
# Copy the entire static directory and its contents
COPY static ./static/

# Verify static files were copied (fail build if missing)
# This will show in build logs and fail if file is missing
RUN echo "=== VERIFYING STATIC FILES ===" && \
    echo "Current directory:" && pwd && \
    echo "Contents of static folder:" && \
    ls -la ./static/ 2>&1 && \
    echo "File count:" && \
    find ./static -type f | wc -l && \
    echo "Checking for profile image..." && \
    (test -f ./static/250509_PGB9975_1.jpg && echo "✓ Profile image found!") || \
    (echo "✗ ERROR: Profile image NOT found!" && \
     echo "Files in static directory:" && \
     find ./static -type f && \
     exit 1)

# Expose port (Cloud Run uses PORT env variable)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" || exit 1

# Run the application
CMD ["python", "app.py"]
