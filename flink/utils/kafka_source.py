"""
Kafka consumer factory — shared by all streaming jobs.
In production (AWS) this would be replaced by PyFlink KafkaSource with watermark strategy.
"""

import json
import os

from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def make_consumer(topic: str, group_id: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([topic])
    return consumer


def poll_messages(consumer: Consumer, timeout: float = 1.0):
    """Yield decoded JSON dicts from Kafka topic."""
    while True:
        msg = consumer.poll(timeout)
        if msg is None:
            yield None  # heartbeat so caller can check windows
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                raise RuntimeError(f"Kafka error: {msg.error()}")
            continue
        try:
            yield json.loads(msg.value().decode("utf-8"))
        except Exception:
            continue
