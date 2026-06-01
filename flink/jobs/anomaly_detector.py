"""
Job 2: Equipment Anomaly Detector.
Pattern: temperature > 85°C OR vibration > 120Hz followed by FAULT within 30s → alert.

Production mapping:
  - KeyedProcessFunction keyed by equipment_id
  - ValueState stores precursor event + timestamp
  - on_timer cleans up stale state after 30s window
"""

import logging
import signal
import time

from flink.utils.kafka_source import make_consumer, poll_messages
from flink.utils.timescale_sink import TimescaleSink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] anomaly_detector — %(message)s",
)
log = logging.getLogger(__name__)

HIGH_TEMP = 85.0
HIGH_VIBRATION = 120.0
PATTERN_WINDOW_S = 30

_running = True


def handle_signal(sig, frame):
    global _running
    _running = False


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    log.info("Starting Equipment Anomaly Detector")
    consumer = make_consumer("equipment-status", "flink-anomaly-detector")
    sink = TimescaleSink("alerts")

    # Per-equipment precursor state: {equipment_id: {"time": float, "temp": float, "vib": float}}
    precursors: dict = {}

    try:
        for event in poll_messages(consumer):
            if not _running:
                break
            if event is None:
                # Expire stale precursors
                now = time.time()
                expired = [
                    eid for eid, p in precursors.items() if now - p["time"] > PATTERN_WINDOW_S
                ]
                for eid in expired:
                    del precursors[eid]
                continue

            equipment_id = event.get("equipment_id", "UNKNOWN")
            status = event.get("status", "")
            temp = float(event.get("temperature_c") or 0)
            vib = float(event.get("vibration_hz") or 0)
            now = time.time()

            is_high_sensor = temp > HIGH_TEMP or vib > HIGH_VIBRATION

            if is_high_sensor and status != "FAULT":
                precursors[equipment_id] = {"time": now, "temp": temp, "vib": vib}

            elif status == "FAULT" and equipment_id in precursors:
                p = precursors[equipment_id]
                if now - p["time"] <= PATTERN_WINDOW_S:
                    from datetime import datetime, timezone

                    alert = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "equipment_id": equipment_id,
                        "line_id": event.get("line_id", ""),
                        "alert_type": "THERMAL_FAULT_PRECURSOR",
                        "severity": "HIGH",
                        "temperature_c": p["temp"],
                        "fault_code": event.get("fault_code"),
                    }
                    sink.invoke(alert, None)
                    sink._flush()
                    log.warning(
                        "ALERT %s — %s temp=%.1f°C fault=%s",
                        alert["line_id"],
                        equipment_id,
                        p["temp"],
                        event.get("fault_code"),
                    )
                del precursors[equipment_id]

    finally:
        sink.close()
        consumer.close()
        log.info("Anomaly Detector stopped.")


if __name__ == "__main__":
    main()
