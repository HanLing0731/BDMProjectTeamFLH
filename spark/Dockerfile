FROM bitnami/spark:3.4

# Switch to root to install system deps & Python packages
USER root

# 1) Create needed directories and set permissions
RUN mkdir -p \
      /.cache/kagglehub \
      /tmp/kaggle_cache \
      /data/unstructured/profile_photos \
      /data/delta/profile_photos && \
    chmod -R 777 /.cache /tmp/kaggle_cache /data

# 2) Install OS packages and Python libs
RUN apt-get update && \
    apt-get install -y curl python3-pip && \
    pip install --no-warn-script-location \
      pillow \
      kagglehub \
      importlib_metadata \
      delta-spark==2.4.0 \
      duckdb \
      pinecone \
      scikit-learn \
      numpy \
      pandas\
      minio && \
    rm -rf /var/lib/apt/lists/*

# 3) Download Delta Lake JARs
RUN curl -L https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar \
      -o /opt/bitnami/spark/jars/delta-core.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar \
      -o /opt/bitnami/spark/jars/delta-storage.jar

# 4) Create sparkuser and fix ownership
RUN useradd -u 1001 -m sparkuser && \
    chown -R sparkuser:sparkuser \
      /.cache \
      /tmp/kaggle_cache \
      /data \
      /opt/bitnami/spark

USER sparkuser

# 5) Cache settings for KaggleHub
ENV KAGGLE_HUB_CACHE=/tmp/kaggle_cache \
    XDG_CACHE_HOME=/tmp

# 6) Copy only the entry-point script; 
COPY ingest_data.py /app/ingest_data.py

# 7) Default command
CMD ["spark-submit", "--packages", "io.delta:delta-core_2.12:2.4.0", "/app/ingest_data.py"]
