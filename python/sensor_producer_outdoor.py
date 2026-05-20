"""
Produtor outdoor: simula sensor externo (maior amplitude) e publica leituras
na fila sensor.readings.outdoor.
"""
from __future__ import annotations

import random
import time
from datetime import datetime, timezone

import rabbitpy

from const import (
    EXCHANGE_TEMPERATURE,
    QUEUE_SENSOR_OUTDOOR,
    RK_SENSOR_OUTDOOR,
    SENSOR_ID_OUTDOOR,
    SENSOR_PUBLISH_DELTA_C,
)
from mq_common import amqp_url, declare_topology, publish_json


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    with rabbitpy.Connection(amqp_url()) as conn:
        with conn.channel() as channel:
            exchange = declare_topology(channel)
            temp_c = 28.0 + random.uniform(-1.0, 1.0)
            last_sent: float | None = None

            print(f"[outdoor] {SENSOR_ID_OUTDOOR} → fila {QUEUE_SENSOR_OUTDOOR} (rk={RK_SENSOR_OUTDOOR})")
            print(f"Publica quando |Δ| >= {SENSOR_PUBLISH_DELTA_C} °C (Ctrl+C para sair)")

            try:
                while True:
                    drift = random.uniform(-0.8, 0.9)
                    temp_c = max(-5.0, min(45.0, temp_c + drift))

                    if last_sent is None or abs(temp_c - last_sent) >= SENSOR_PUBLISH_DELTA_C:
                        payload = {
                            "sensor_id": SENSOR_ID_OUTDOOR,
                            "zone": "outdoor",
                            "celsius": round(temp_c, 2),
                            "observed_at": _now_iso(),
                        }
                        publish_json(channel, exchange, RK_SENSOR_OUTDOOR, payload)
                        print(f"Enviado: {payload}")
                        last_sent = temp_c

                    time.sleep(random.uniform(0.6, 1.4))
            except KeyboardInterrupt:
                print("\nEncerrando produtor outdoor.")


if __name__ == "__main__":
    main()
