# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Cài dependencies hệ thống
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    wget \
    curl \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements và cài Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy AI code
COPY analytics /app/analytics

# CMD mặc định
CMD ["uvicorn", "analytics.analytics_api:app", "--host", "0.0.0.0", "--port", "8002"]