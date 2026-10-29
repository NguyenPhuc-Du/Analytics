FROM apache/spark:3.5.3-python3

# PySpark ML (KMeans / StandardScaler) import numpy on the driver.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip \
    && pip3 install --no-cache-dir numpy \
    && rm -rf /var/lib/apt/lists/*
USER 0
