"""
Detecção de alertas: consome agregados, compara com limiar por zona
e publica alertas críticos na fila temperature.alerts.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import rabbitpy

from const import (
    ALERT_THRESHOLD_INDOOR_C,
    ALERT_THRESHOLD_OUTDOOR_C,
    QUEUE_AGGREGATES_ALERT_CHECK,
    RK_ALERT,
)
from mq_common import amqp_url, declare_topology, publish_json


def _threshold_for(zone: str) -> float:
    if zone == "outdoor":
        return ALERT_THRESHOLD_OUTDOOR_C
    return ALERT_THRESHOLD_INDOOR_C


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(f"Detecção de alertas ← fila {QUEUE_AGGREGATES_ALERT_CHECK}")

    with rabbitpy.Connection(amqp_url()) as conn:
        with conn.channel() as channel:
            exchange = declare_topology(channel)
            queue = rabbitpy.Queue(
                channel, QUEUE_AGGREGATES_ALERT_CHECK, durable=True, auto_delete=False
            )
            for message in queue:
                agg = json.loads(message.body.decode("utf-8"))
                try:
                    zone = agg.get("zone", "indoor")
                    threshold = _threshold_for(zone)
                    avg = float(agg["avg_celsius"])
                    if avg >= threshold:
                        alert = {
                            "sensor_id": agg["sensor_id"],
                            "zone": zone,
                            "avg_celsius": avg,
                            "threshold_celsius": threshold,
                            "severity": "critical",
                            "message": (
                                f"Média {avg}°C >= limiar {threshold}°C ({zone}) "
                                f"na janela {agg['window_start_iso']} — {agg['window_end_iso']}"
                            ),
                            "raised_at_iso": datetime.now(timezone.utc).isoformat(),
                        }
                        publish_json(channel, exchange, RK_ALERT, alert)
                        logging.info("Alerta publicado: %s", alert["message"])
                    message.ack()
                except Exception:
                    message.reject(requeue=True)
                    raise


if __name__ == "__main__":
    main()
