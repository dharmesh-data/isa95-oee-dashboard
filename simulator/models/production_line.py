import asyncio
import logging
from typing import Any, Dict, List

from simulator.config import LINE_CONFIG, MACHINES_PER_LINE
from simulator.models.work_unit import WorkUnit

logger = logging.getLogger(__name__)


class ProductionLine:
    """
    ISA-95 Work Center — manages N Work Units concurrently.
    Collects events from all units and hands them to producers.
    """

    def __init__(self, line_id: str):
        self.line_id = line_id
        cfg = LINE_CONFIG[line_id]
        self.work_units: List[WorkUnit] = [
            WorkUnit(line_id, f"WU-{i:02d}", cfg) for i in range(1, MACHINES_PER_LINE + 1)
        ]

    async def run(self, producer, cycle_sleep_s: float = 1.0):
        """
        Main loop. Each iteration = one tick across all work units.
        Events published to Kafka after each tick.
        """
        logger.info("Starting production line %s (%d units)", self.line_id, len(self.work_units))
        while True:
            line_events: List[Dict[str, Any]] = []
            equipment_events: List[Dict[str, Any]] = []
            quality_events: List[Dict[str, Any]] = []

            # Tick all work units
            await asyncio.gather(
                *[
                    wu.tick(line_events, equipment_events, quality_events, cycle_sleep_s)
                    for wu in self.work_units
                ]
            )

            # Publish
            for event in line_events:
                await producer.send("line-events", event, key=self.line_id)

            for event in equipment_events:
                await producer.send("equipment-status", event, key=event["equipment_id"])

            for event in quality_events:
                await producer.send("quality-metrics", event, key=self.line_id)

            if line_events:
                logger.debug(
                    "%s: %d line events, %d equipment, %d quality",
                    self.line_id,
                    len(line_events),
                    len(equipment_events),
                    len(quality_events),
                )

            await asyncio.sleep(cycle_sleep_s)
