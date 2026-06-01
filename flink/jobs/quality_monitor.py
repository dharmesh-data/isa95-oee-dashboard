"""
Job 3: Quality SPC Monitor — sliding 10-min / 1-min window per line_id.
Computes rolling Cpk, mean, stddev → writes to quality_stats.

Production mapping:
  - SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1))
  - ProcessWindowFunction computes Cpk over window elements
"""

import logging
import signal
import statistics
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

from flink.utils.kafka_source import make_consumer, poll_messages
from flink.utils.timescale_sink import TimescaleSink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] quality_monitor — %(message)s",
)
log = logging.getLogger(__name__)

WINDOW_SIZE_S = 600  # 10 minutes
SLIDE_INTERVAL_S = 60  # emit every 1 minute

_running = True


def handle_signal(sig, frame):
    global _running
    _running = False


def compute_cpk(events: list) -> dict | None:
    if len(events) < 2:
        return None

    means = [float(e.get("measured_mean", 0)) for e in events]
    stddevs = [
        float(e.get("measured_stddev", 0)) for e in events if float(e.get("measured_stddev", 0)) > 0
    ]

    if not stddevs:
        return None

    rolling_mean = statistics.mean(means)
    rolling_std = statistics.mean(stddevs)

    last = events[-1]
    spec_upper = float(last.get("spec_upper", 0))
    spec_lower = float(last.get("spec_lower", 0))

    cpk = 0.0
    if rolling_std > 0 and spec_upper > spec_lower:
        cpk = min(
            (spec_upper - rolling_mean) / (3 * rolling_std),
            (rolling_mean - spec_lower) / (3 * rolling_std),
        )

    return {
        "rolling_cpk": round(cpk, 3),
        "rolling_mean": round(rolling_mean, 4),
        "rolling_stddev": round(rolling_std, 4),
        "window_batches": len(events),
    }


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    log.info("Starting Quality SPC Monitor (10-min sliding / 1-min slide)")
    consumer = make_consumer("quality-metrics", "flink-quality-monitor")
    sink = TimescaleSink("quality_stats")

    # Ring buffer per line: (timestamp, event)
    buffers: dict = defaultdict(lambda: deque())
    last_emit = time.time()

    try:
        for event in poll_messages(consumer):
            if not _running:
                break

            now = time.time()

            if event is not None:
                line_id = event.get("line_id", "UNKNOWN")
                buffers[line_id].append((now, event))

            # Slide every SLIDE_INTERVAL_S
            if now - last_emit >= SLIDE_INTERVAL_S:
                window_end = datetime.fromtimestamp(now, tz=timezone.utc)
                cutoff = now - WINDOW_SIZE_S

                for line_id, buf in buffers.items():
                    # Drop events older than window
                    while buf and buf[0][0] < cutoff:
                        buf.popleft()

                    events = [e for _, e in buf]
                    stats = compute_cpk(events)
                    if stats:
                        result = {
                            "time": window_end.isoformat(),
                            "line_id": line_id,
                            **stats,
                        }
                        sink.invoke(result, None)
                        log.info(
                            "SPC %s: Cpk=%.3f mean=%.4f batches=%d",
                            line_id,
                            stats["rolling_cpk"],
                            stats["rolling_mean"],
                            stats["window_batches"],
                        )

                sink._flush()
                last_emit = now

    finally:
        sink.close()
        consumer.close()
        log.info("Quality Monitor stopped.")


if __name__ == "__main__":
    main()
