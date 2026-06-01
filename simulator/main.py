import asyncio
import logging
import os
import signal
import sys

from simulator.config import LINE_CONFIG
from simulator.models.production_line import ProductionLine
from simulator.producers.kafka_avro_producer import KafkaAvroProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("simulator")


def get_config() -> dict:
    return {
        "bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "schema_registry_url": os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        "lines": os.environ.get("LINES", "LINE-A,LINE-B,LINE-C").split(","),
        "cycle_sleep_s": 1.0 / max(1, int(os.environ.get("EVENTS_PER_SECOND", "10")) // 15),
    }


async def run():
    cfg = get_config()
    logger.info("Starting ISA-95 Manufacturing Simulator")
    logger.info("Lines: %s", cfg["lines"])
    logger.info("Kafka: %s", cfg["bootstrap_servers"])
    logger.info("Schema Registry: %s", cfg["schema_registry_url"])

    producer = KafkaAvroProducer(
        bootstrap_servers=cfg["bootstrap_servers"],
        schema_registry_url=cfg["schema_registry_url"],
    )

    lines = [ProductionLine(line_id) for line_id in cfg["lines"] if line_id in LINE_CONFIG]

    if not lines:
        logger.error("No valid lines configured. Check LINES env var.")
        sys.exit(1)

    logger.info("Running %d production lines", len(lines))

    try:
        await asyncio.gather(
            *[line.run(producer, cycle_sleep_s=cfg["cycle_sleep_s"]) for line in lines]
        )
    except asyncio.CancelledError:
        logger.info("Simulator shutting down...")
    finally:
        producer.flush()
        logger.info("Producer flushed. Goodbye.")


def main():
    loop = asyncio.get_event_loop()

    def shutdown():
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    try:
        loop.run_until_complete(run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
