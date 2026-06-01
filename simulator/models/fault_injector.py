import random
from datetime import datetime

from simulator.config import FAULT_CONFIG

DOWNTIME_REASONS = [
    "MECHANICAL_FAILURE",
    "ELECTRICAL_FAULT",
    "MATERIAL_SHORTAGE",
    "OPERATOR_ABSENCE",
    "QUALITY_HOLD",
]

FAULT_CODES = {
    "MECHANICAL_FAILURE": ["E-042", "E-043", "E-101", "E-102"],
    "ELECTRICAL_FAULT": ["E-201", "E-202", "E-250"],
    "MATERIAL_SHORTAGE": ["E-301"],
    "OPERATOR_ABSENCE": ["E-401"],
    "QUALITY_HOLD": ["E-501", "E-502"],
}


class FaultInjector:
    def __init__(self, line_id: str):
        self.line_id = line_id
        self._in_fault_cluster = False
        self._cluster_remaining = 0

    def should_fault(self, temperature_c: float, vibration_hz: float) -> bool:
        rate = FAULT_CONFIG["base_fault_rate"]

        # Monday morning cold-start effect
        now = datetime.now()
        if now.weekday() == 0 and now.hour < 9:
            rate *= FAULT_CONFIG["monday_morning_multiplier"]

        # Shift changeover window
        minute = now.hour * 60 + now.minute
        changeover_minutes = [6 * 60, 14 * 60, 22 * 60]
        if any(abs(minute - cm) < 15 for cm in changeover_minutes):
            rate *= FAULT_CONFIG["shift_changeover_multiplier"]

        # Sensor-driven fault triggers
        if temperature_c > FAULT_CONFIG["high_temp_fault_threshold"]:
            rate *= 3.0
        if vibration_hz > FAULT_CONFIG["high_vibration_threshold"]:
            rate *= 2.5

        # Fault clustering: active cluster increases probability
        if self._in_fault_cluster:
            rate *= 2.0
            self._cluster_remaining -= 1
            if self._cluster_remaining <= 0:
                self._in_fault_cluster = False

        triggered = random.random() < rate

        if triggered and random.random() < FAULT_CONFIG["fault_cluster_probability"]:
            self._in_fault_cluster = True
            self._cluster_remaining = random.randint(2, 5)

        return triggered

    def get_downtime_reason(self, temperature_c: float, vibration_hz: float) -> str:
        if temperature_c > FAULT_CONFIG["high_temp_fault_threshold"]:
            return "MECHANICAL_FAILURE"
        if vibration_hz > FAULT_CONFIG["high_vibration_threshold"]:
            return "MECHANICAL_FAILURE"
        return random.choice(DOWNTIME_REASONS)

    def get_fault_code(self, reason: str) -> str:
        codes = FAULT_CODES.get(reason, ["E-999"])
        return random.choice(codes)

    def get_mttr_seconds(self, reason: str) -> int:
        minutes = FAULT_CONFIG["mttr_minutes"].get(reason, 30)
        # Add ±20% jitter
        jitter = random.uniform(0.8, 1.2)
        return int(minutes * 60 * jitter)
