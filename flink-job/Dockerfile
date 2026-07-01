FROM flink:1.18-java11

# Install Python 3 and pip
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 python3-pip python3-dev && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Install PyFlink
RUN pip3 install --no-cache-dir apache-flink==1.18.1

# Download Kafka SQL connector
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# Copy job files
COPY job.py /opt/flink/job.py
COPY sensor_metadata.json /opt/flink/sensor_metadata.json
