"""
Notificador de alertas: consome a fila temperature.alerts e simula
o envio de notificação (latência de canal externo, log estruturado).
"""
from __future__ import annotations

import logging
import time

from const import QUEUE_ALERTS
from mq_common import consume_json


def _simulate_notify(alert: dict) -> None:
    # Simula SMS/e-mail/webhook com latência de rede
    time.sleep(0.4)
    logging.info(
        "[NOTIFY] %s | sensor=%s avg=%.2f°C limiar=%.2f°C",
        alert.get("message"),
        alert.get("sensor_id"),
        float(alert.get("avg_celsius", 0)),
        float(alert.get("threshold_celsius", 0)),
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(f"Notificador ← fila {QUEUE_ALERTS} (Ctrl+C para sair)")
    consume_json(QUEUE_ALERTS, _simulate_notify)


if __name__ == "__main__":
    main()
