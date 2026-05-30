FROM flink:1.17.1-scala_2.12

USER root

RUN apt-get update -y \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        python3 \
        python3-dev \
        python3-pip \
        python-is-python3 \
        wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.17.1

RUN wget -q -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.0.1-1.17/flink-connector-kafka-3.0.1-1.17.jar \
    && wget -q -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar \
    && wget -q -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.1-1.17/flink-connector-jdbc-3.1.1-1.17.jar \
    && wget -q -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

WORKDIR /opt/flink

RUN mkdir -p /opt/flink/jobs

COPY stream_to_postgres.py /opt/flink/jobs/stream_to_postgres.py

USER flink
