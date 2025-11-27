FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY src/ src/
COPY scripts/ scripts/
COPY setup.py .

# Install package
RUN pip install -e .

# Expose API port
EXPOSE 8000

# Default command (override in k8s)
CMD ["python", "-m", "scripts.run_packer"]
