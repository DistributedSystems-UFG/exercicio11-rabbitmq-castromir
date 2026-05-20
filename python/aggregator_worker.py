"""
Agregador em janela móvel: consome leituras de uma fila de sensor,
calcula média na janela e publica agregados (cópia para filas de persistência e alertas).
"""
from __future__ import annotations

import logging
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any

import rabbitpy

from const import EXCHANGE_TEMPERATURE, RK_AGGREGATE, WINDOW_HOURS
import json

from mq_common import amqp_url, declare_topology, publish_json


def _parse_ts(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def _trim_window(
    samples: deque[tuple[datetime, float]], end: datetime, window: timedelta
) -> None:
    cutoff = end - window
    while samples and samples[0][0] < cutoff:
        samples.popleft()


def run_aggregator(queue_name: str, label: str) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    window = timedelta(hours=WINDOW_HOURS)
    samples: deque[tuple[datetime, float]] = deque()

    with rabbitpy.Connection(amqp_url()) as conn:
        with conn.channel() as channel:
            exchange = declare_topology(channel)

            def on_reading(row: dict[str, Any]) -> None:
                nonlocal samples
                sensor_id = row["sensor_id"]
                celsius = float(row["celsius"])
                observed = _parse_ts(row["observed_at"])

                samples.append((observed, celsius))
                _trim_window(samples, observed, window)
                if not samples:
                    return

                window_start = samples[0][0]
                window_end = samples[-1][0]
                vals = [t for _, t in samples]
                avg = sum(vals) / len(vals)
                out = {
                    "sensor_id": sensor_id,
                    "zone": row.get("zone", label),
                    "window_start_iso": window_start.astimezone(timezone.utc).isoformat(),
                    "window_end_iso": window_end.astimezone(timezone.utc).isoformat(),
                    "avg_celsius": round(avg, 3),
                    "sample_count": len(samples),
                    "computed_at_iso": datetime.now(timezone.utc).isoformat(),
                }
                publish_json(channel, exchange, RK_AGGREGATE, out)
                logging.info("[%s] Agregado publicado: %s", label, out)

            print(f"[{label}] Consumindo fila {queue_name} → rk {RK_AGGREGATE}")
            queue = rabbitpy.Queue(channel, queue_name, durable=True, auto_delete=False)
            for message in queue:
                row = json.loads(message.body.decode("utf-8"))
                try:
                    on_reading(row)
                    message.ack()
                except Exception:
                    message.reject(requeue=True)
                    raise


if __name__ == "__main__":
    raise SystemExit("Use aggregator_indoor.py ou aggregator_outdoor.py")
