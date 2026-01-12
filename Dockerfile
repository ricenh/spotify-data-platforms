FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY src/ ./src/
COPY sql/ ./sql/
COPY run_pipeline.py .
COPY .env .

# Set Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "run_pipeline.py"]