"""
Job 1: OEE Calculator — tumbling 60s window per line_id.
Computes Availability × Performance × Quality → writes to oee_metrics.

Production mapping:
  - Consumer group  → Flink KafkaSource
  - Manual window   → TumblingEventTimeWindows.of(Time.seconds(60))
  - Watermark       → WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(45))
  - TimescaleSink   → JDBC sink with UPSERT on (time, line_id) for exactly-once
"""

import logging
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone

from flink.utils.kafka_source import make_consumer, poll_messages
from flink.utils.timescale_sink import TimescaleSink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] oee_calculator — %(message)s",
)
log = logging.getLogger(__name__)

WINDOW_SIZE_S = 60
PLANNED_TIME_S = 3600

LINE_CONFIG = {
    "LINE-A": {"ideal_cycle_time_ms": 4800},
    "LINE-B": {"ideal_cycle_time_ms": 6200},
    "LINE-C": {"ideal_cycle_time_ms": 3500},
}

_running = True


def handle_signal(sig, frame):
    global _running
    _running = False


def compute_oee(line_id: str, events: list, window_end: datetime) -> dict:
    downtime_s = sum(
        e.get("downtime_s", 0)
        for e in events
        if e.get("event_type") in ("FAULT", "STOP", "PLANNED_STOP", "CHANGEOVER")
    )
    availability = max(0.0, (PLANNED_TIME_S - downtime_s) / PLANNED_TIME_S)

    cycle_events = [e for e in events if e.get("event_type") == "CYCLE_COMPLETE"]
    total_units = sum(e.get("units_produced", 0) for e in cycle_events)
    good_units = sum(e.get("units_produced", 0) - e.get("units_rejected", 0) for e in cycle_events)

    ideal_ms = LINE_CONFIG.get(line_id, {}).get("ideal_cycle_time_ms", 5000)
    operating_ms = max(1, (PLANNED_TIME_S - downtime_s) * 1000)
    performance = min(1.0, (total_units * ideal_ms) / operating_ms) if total_units > 0 else 0.0
    quality = (good_units / total_units) if total_units > 0 else 0.0
    oee = availability * performance * quality

    return {
        "time": window_end.isoformat(),
        "line_id": line_id,
        "availability": round(availability, 4),
        "performance": round(performance, 4),
        "quality": round(quality, 4),
        "oee": round(oee, 4),
        "total_units": total_units,
        "good_units": good_units,
    }


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    log.info("Starting OEE Calculator (60s tumbling windows)")
    consumer = make_consumer("line-events", "flink-oee-calculator")
    sink = TimescaleSink("oee_metrics")

    # Buffers keyed by line_id
    windows: dict = defaultdict(list)
    window_start = time.time()

    try:
        for event in poll_messages(consumer):
            if not _running:
                break

            if event is not None:
                line_id = event.get("line_id", "UNKNOWN")
                windows[line_id].append(event)

            # Close window every WINDOW_SIZE_S seconds (wall-clock tumbling)
            now = time.time()
            if now - window_start >= WINDOW_SIZE_S:
                window_end = datetime.fromtimestamp(now, tz=timezone.utc)
                for line_id, events in windows.items():
                    if events:
                        result = compute_oee(line_id, events, window_end)
                        sink.invoke(result, None)
                        log.info(
                            "OEE %s: %.1f%% (A=%.2f P=%.2f Q=%.2f, %d units)",
                            line_id,
                            result["oee"] * 100,
                            result["availability"],
                            result["performance"],
                            result["quality"],
                            result["total_units"],
                        )
                sink._flush()
                windows.clear()
                window_start = now
    finally:
        sink.close()
        consumer.close()
        log.info("OEE Calculator stopped.")


if __name__ == "__main__":
    main()
