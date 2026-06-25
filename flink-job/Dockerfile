FROM flink:1.18-java11

# Install Python
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-venv && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink and dependencies
COPY requirements.txt /opt/flink/job/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/job/requirements.txt

# Download Kafka connector JAR
RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# Copy job files
COPY job.py /opt/flink/job/job.py
COPY metadata.json /opt/flink/job/metadata.json

ENV METADATA_PATH=/opt/flink/job/metadata.json
