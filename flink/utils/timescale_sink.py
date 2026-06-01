import os

import psycopg2
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": os.environ.get("TIMESCALE_HOST", "localhost"),
    "port": int(os.environ.get("TIMESCALE_PORT", "5432")),
    "dbname": os.environ.get("TIMESCALE_DB", "manufacturing"),
    "user": os.environ.get("TIMESCALE_USER", "postgres"),
    "password": os.environ.get("TIMESCALE_PASSWORD", "changeme"),
}

OEE_UPSERT = """
    INSERT INTO oee_metrics (time, line_id, availability, performance, quality, oee, total_units, good_units)
    VALUES %s
    ON CONFLICT (time, line_id) DO UPDATE SET
        availability = EXCLUDED.availability,
        performance  = EXCLUDED.performance,
        quality      = EXCLUDED.quality,
        oee          = EXCLUDED.oee,
        total_units  = EXCLUDED.total_units,
        good_units   = EXCLUDED.good_units
"""

QUALITY_UPSERT = """
    INSERT INTO quality_stats (time, line_id, rolling_cpk, rolling_mean, rolling_stddev, window_batches)
    VALUES %s
    ON CONFLICT (time, line_id) DO UPDATE SET
        rolling_cpk    = EXCLUDED.rolling_cpk,
        rolling_mean   = EXCLUDED.rolling_mean,
        rolling_stddev = EXCLUDED.rolling_stddev,
        window_batches = EXCLUDED.window_batches
"""

ALERT_INSERT = """
    INSERT INTO alerts (timestamp, equipment_id, line_id, alert_type, severity, temperature_c, fault_code)
    VALUES %s
"""


class TimescaleSink:
    """
    Batching sink to TimescaleDB. Uses UPSERT on (time, line_id) for exactly-once safety.
    Re-opens connection if lost (Flink restarts frequently during dev).
    """

    def __init__(self, table: str):
        self.table = table
        self._conn = None
        self._buffer = []
        self._batch_size = 50

    def _get_conn(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**DB_CONFIG)
            self._conn.autocommit = False
        return self._conn

    def _flush(self):
        if not self._buffer:
            return
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                if self.table == "oee_metrics":
                    execute_values(cur, OEE_UPSERT, self._buffer)
                elif self.table == "quality_stats":
                    execute_values(cur, QUALITY_UPSERT, self._buffer)
                elif self.table == "alerts":
                    execute_values(cur, ALERT_INSERT, self._buffer)
            conn.commit()
            self._buffer.clear()
        except Exception as e:
            conn.rollback()
            raise e

    def invoke(self, value, context):
        if self.table == "oee_metrics":
            self._buffer.append(
                (
                    value["time"],
                    value["line_id"],
                    value["availability"],
                    value["performance"],
                    value["quality"],
                    value["oee"],
                    value["total_units"],
                    value["good_units"],
                )
            )
        elif self.table == "quality_stats":
            self._buffer.append(
                (
                    value["time"],
                    value["line_id"],
                    value["rolling_cpk"],
                    value["rolling_mean"],
                    value["rolling_stddev"],
                    value["window_batches"],
                )
            )
        elif self.table == "alerts":
            self._buffer.append(
                (
                    value["timestamp"],
                    value["equipment_id"],
                    value["line_id"],
                    value["alert_type"],
                    value["severity"],
                    value.get("temperature_c"),
                    value.get("fault_code"),
                )
            )

        if len(self._buffer) >= self._batch_size:
            self._flush()

    def close(self):
        self._flush()
        if self._conn and not self._conn.closed:
            self._conn.close()
