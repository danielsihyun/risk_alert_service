FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first (layer caching — deps change less often than code)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app ./app
COPY mock_slack ./mock_slack
COPY README.md .

# Create directory for SQLite database (writable at runtime)
RUN mkdir -p /app/data
ENV DATABASE_URL=sqlite:////app/data/risk_alerts.db

EXPOSE 8000

# Health check for container orchestrators (ECS, Cloud Run, K8s)
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health').raise_for_status()"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
