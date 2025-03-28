FROM bitnami/spark:3.4

# First create all directories
USER root
RUN mkdir -p \
    /.cache/kagglehub \
    /tmp/kaggle_cache \
    /data/unstructured/profile_photos /data/delta/profile_photos && \
    chmod -R 777 /.cache /tmp/kaggle_cache /data

# Then install packages
RUN apt-get update && \
    apt-get install -y curl python3-pip && \
    pip install --no-warn-script-location pillow kagglehub importlib_metadata delta-spark==2.4.0 && \
    rm -rf /var/lib/apt/lists/*

# Download Delta Lake jars in same RUN command
RUN curl -L https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar \
    -o /opt/bitnami/spark/jars/delta-core.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar \
    -o /opt/bitnami/spark/jars/delta-storage.jar

# User setup as final step
RUN useradd -u 1001 -m sparkuser && \
    chown -R sparkuser:sparkuser /.cache /tmp/kaggle_cache /data /opt/bitnami/spark

USER sparkuser
ENV KAGGLE_HUB_CACHE=/tmp/kaggle_cache
ENV XDG_CACHE_HOME=/tmp

COPY ingest_data.py /app/
CMD ["spark-submit", "--packages", "io.delta:delta-core_2.12:2.4.0", "/app/ingest_data.py"]