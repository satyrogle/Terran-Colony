from __future__ import annotations

import logging
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TriangleFuzzySet:
    a: float
    b: float
    c: float

    def value(self, x: float) -> float:
        if x < self.a or x > self.c:
            return 0.0
        if x == self.b:
            return 1.0
        if x < self.b:
            denominator = self.b - self.a
            if denominator == 0:
                return 1.0
            return (x - self.a) / denominator
        denominator = self.c - self.b
        if denominator == 0:
            return 1.0
        return (self.c - x) / denominator


class PIDGuardrailController:
    """Observe-only PID controller for resource trajectory monitoring."""

    def __init__(self, kp: float, ki: float, kd: float, setpoint: float):
        self.kp = kp
        self.ki = ki
        self.kd = kd
        self.setpoint = setpoint
        self._state_by_aggregate: dict[str, dict[str, float]] = {}
        self._instability_sets = {
            "stable": TriangleFuzzySet(0.0, 0.0, 0.35),
            "drifting": TriangleFuzzySet(0.2, 0.55, 0.95),
            "volatile": TriangleFuzzySet(0.75, 1.3, 2.5),
        }
        self._latest_state = self._stable_state()
        self._latest_state_by_aggregate: dict[str, dict] = {}

    def _stable_state(self) -> dict:
        return {
            "label": "stable",
            "degree": 1.0,
            "membership": {"stable": 1.0, "drifting": 0.0, "volatile": 0.0},
            "control_signal_abs": 0.0,
        }

    def observe_resource_change(self, current_utilization: float, aggregate_id: str) -> float:
        current_time = time.time()
        state = self._state_by_aggregate.setdefault(
            aggregate_id,
            {
                "previous_error": 0.0,
                "integral": 0.0,
                "last_time": current_time,
            },
        )
        dt = current_time - state["last_time"]
        if dt <= 0:
            dt = 1e-4

        error = self.setpoint - current_utilization
        state["integral"] += error * dt
        derivative = (error - state["previous_error"]) / dt

        u_t = (self.kp * error) + (self.ki * state["integral"]) + (self.kd * derivative)
        fuzzy_state = self.classify_instability(u_t)
        self._latest_state_by_aggregate[aggregate_id] = fuzzy_state
        self._latest_state = fuzzy_state

        state["previous_error"] = error
        state["last_time"] = current_time

        logger.info(
            "[PID Observe] Aggregate: %s | u(t): %.4f | Error: %.4f | State: %s(%.2f)",
            aggregate_id,
            u_t,
            error,
            fuzzy_state["label"],
            fuzzy_state["degree"],
        )
        return u_t

    def classify_instability(self, control_signal: float) -> dict:
        control_abs = abs(control_signal)
        capped = min(control_abs, 2.5)
        membership = {
            label: max(0.0, min(1.0, fuzzy_set.value(capped)))
            for label, fuzzy_set in self._instability_sets.items()
        }
        severity_rank = {"stable": 0, "drifting": 1, "volatile": 2}
        label = max(
            membership.items(),
            key=lambda item: (item[1], severity_rank[item[0]]),
        )[0]
        return {
            "label": label,
            "degree": membership[label],
            "membership": membership,
            "control_signal_abs": control_abs,
        }

    def get_latest_state(self, aggregate_id: str | None = None) -> dict:
        if aggregate_id is None:
            return dict(self._latest_state)
        return dict(self._latest_state_by_aggregate.get(aggregate_id, self._stable_state()))
