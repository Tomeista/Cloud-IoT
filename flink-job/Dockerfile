FROM flink:1.18-java11

# Install Python (minimal, no recommended extras)
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink (version aligned with the Flink base image) and prune the
# Flink runtime bundled inside the PyFlink wheel. At runtime the cluster uses
# FLINK_HOME=/opt/flink from the base image, so pyflink's own lib/opt/examples
# copies are redundant and only add several hundred MB.
COPY requirements.txt /opt/flink/job/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/job/requirements.txt && \
    PYFLINK_DIR="$(python3 -c 'import os, pyflink; print(os.path.dirname(pyflink.__file__))')" && \
    rm -rf "$PYFLINK_DIR/lib" "$PYFLINK_DIR/opt" "$PYFLINK_DIR/examples" \
    "$PYFLINK_DIR/log" "$PYFLINK_DIR/licenses"

# Download Kafka connector JAR
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# Copy job files
COPY job.py /opt/flink/job/job.py
COPY metadata.json /opt/flink/job/metadata.json

ENV METADATA_PATH=/opt/flink/job/metadata.json
