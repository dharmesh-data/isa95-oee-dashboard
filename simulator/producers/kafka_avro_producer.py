import asyncio
import json
import logging
from typing import Any, Dict, Optional

from confluent_kafka import Producer

logger = logging.getLogger(__name__)


class KafkaAvroProducer:
    """
    Async Kafka producer using plain JSON serialization for local dev.
    In production (AWS) swap for AvroSerializer + Schema Registry.
    Schemas in schemas/*.avsc document the contract — enforced by Schema Registry on AWS.
    """

    def __init__(self, bootstrap_servers: str, schema_registry_url: str):
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "linger.ms": 5,
                "batch.size": 16384,
            }
        )
        self._loop = asyncio.get_event_loop()

    def _delivery_report(self, err, msg):
        if err:
            logger.error("Delivery failed for %s: %s", msg.topic(), err)

    async def send(self, topic: str, value: Dict[str, Any], key: Optional[str] = None):
        payload = json.dumps(value).encode("utf-8")
        key_bytes = key.encode("utf-8") if key else None

        await self._loop.run_in_executor(
            None,
            lambda: self._producer.produce(
                topic=topic,
                key=key_bytes,
                value=payload,
                on_delivery=self._delivery_report,
            ),
        )
        self._producer.poll(0)

    def flush(self):
        self._producer.flush()
