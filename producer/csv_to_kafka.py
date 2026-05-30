import csv
import json
import os
import re
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError


BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "pet_sales_events")
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
PUBLISH_DELAY_MS = int(os.environ.get("PUBLISH_DELAY_MS", "0"))
FLUSH_EVERY = int(os.environ.get("FLUSH_EVERY", "500"))

INTEGER_FIELDS = {
    "id",
    "customer_age",
    "product_quantity",
    "sale_customer_id",
    "sale_seller_id",
    "sale_product_id",
    "sale_quantity",
    "product_reviews",
}

DECIMAL_FIELDS = {
    "product_price",
    "sale_total_price",
    "product_weight",
    "product_rating",
}


def csv_sort_key(path):
    match = re.search(r"\((\d+)\)", path.name)
    return int(match.group(1)) if match else 0


def list_input_files():
    files = sorted(DATA_DIR.glob("*.csv"), key=csv_sort_key)
    if not files:
        raise FileNotFoundError(f"No CSV files were found in {DATA_DIR}")
    return files


def normalize_value(field, value):
    if value is None:
        return None

    value = value.strip()
    if value == "":
        return None

    if field in INTEGER_FIELDS:
        return int(value)
    if field in DECIMAL_FIELDS:
        return float(value)
    return value


def normalize_row(row, source_file, record_no):
    event = {
        "source_file": source_file,
        "source_record_no": record_no,
    }
    event.update({field: normalize_value(field, value) for field, value in row.items()})
    return event


def create_topic():
    topic = NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)
    while True:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id="pet-sales-topic-admin",
                request_timeout_ms=10000,
            )
            try:
                admin.create_topics([topic], timeout_ms=10000)
                print(f"Created Kafka topic: {TOPIC}")
            except TopicAlreadyExistsError:
                print(f"Kafka topic already exists: {TOPIC}")
            finally:
                admin.close()
            return
        except NoBrokersAvailable:
            print("Kafka is not ready yet, retrying in 2 seconds...")
            time.sleep(2)


def create_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda item: json.dumps(
                    item, ensure_ascii=False
                ).encode("utf-8"),
                linger_ms=25,
                retries=5,
            )
        except NoBrokersAvailable:
            print("Producer cannot reach Kafka, retrying in 2 seconds...")
            time.sleep(2)


def publish_file(producer, path):
    sent = 0
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        for record_no, row in enumerate(reader, start=1):
            producer.send(TOPIC, normalize_row(row, path.name, record_no))
            sent += 1

            if sent % FLUSH_EVERY == 0:
                producer.flush()
                print(f"{path.name}: sent {sent} records")

            if PUBLISH_DELAY_MS > 0:
                time.sleep(PUBLISH_DELAY_MS / 1000)

    producer.flush()
    print(f"{path.name}: finished, sent {sent} records")
    return sent


def main():
    files = list_input_files()
    print(f"CSV input directory: {DATA_DIR}")
    print("Files to publish: " + ", ".join(path.name for path in files))

    create_topic()
    producer = create_producer()

    total = 0
    try:
        for path in files:
            total += publish_file(producer, path)
    finally:
        producer.flush()
        producer.close()

    print(f"Done. Published {total} records to topic {TOPIC}.")


if __name__ == "__main__":
    main()
