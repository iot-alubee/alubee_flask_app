# Production Dockerfile for Cloud Run (no service account key file in image)
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Application code (bq_service_acc.json excluded via .dockerignore)
COPY . .

# Cloud Run sets PORT (default 8080)
ENV PORT=8080
EXPOSE 8080

# Run with gunicorn for production. Build must be from production folder so main.py and auth.py are in /app.
CMD exec gunicorn --bind 0.0.0.0:${PORT} --workers 1 --threads 8 --timeout 0 main:app
