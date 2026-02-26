# --------------------------------------------
# Valorant Game Data ETL Pipeline
# Dockerized Python application
# --------------------------------------------
FROM python:3.12-slim

# set the working directory
WORKDIR /app

# install dependencies first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy the application code
COPY etl/ ./etl/
COPY config/ ./config/

# create data directory for database and logs
RUN mkdir -p /app/data

# set environment variables
ENV PYTHONUNBUFFERED=1
ENV TZ=UTC

# health check to verify the database file exists after first run
HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
    CMD python -c "import os; assert os.path.exists('/app/data/valorant_etl.db')" || exit 1

# run the pipeline
# run once as per default and we will override with no args for the scheduled mode ie every 6 hours
ENTRYPOINT ["python", "etl/pipeline.py"]
CMD ["--once"]