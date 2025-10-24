# Steel City Ops Analytics — Dockerfile
# This builds the Python environment and ensures internal imports (like common.etl_utils) work correctly

FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

ENV PYTHONPATH=/app

# Install system dependencies needed for Postgres + Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    postgresql-client \
 && rm -rf /var/lib/apt/lists/*

# Copy project files into container
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose Streamlit port
EXPOSE 8501

# Default command to run the ETL + dashboard
CMD ["bash", "-c", "\
python pipelines/ingest/etl_bookings.py && \
python pipelines/ingest/etl_weather.py && \
python pipelines/ingest/etl_maintenance.py && \
python pipelines/transform/build_features.py && \
python analytics/train_maintenance_model.py && \
streamlit run analytics/dashboards/streamlit_app.py --server.port=8501 --server.address=0.0.0.0"]