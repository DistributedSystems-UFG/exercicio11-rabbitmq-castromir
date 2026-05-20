"""
Produtor indoor: simula sensor de temperatura interna e publica leituras
na fila sensor.readings.indoor quando há variação significativa.
"""
from __future__ import annotations

import random
import time
from datetime import datetime, timezone

import rabbitpy

from const import (
    EXCHANGE_TEMPERATURE,
    QUEUE_SENSOR_INDOOR,
    RK_SENSOR_INDOOR,
    SENSOR_ID_INDOOR,
    SENSOR_PUBLISH_DELTA_C,
)
from mq_common import amqp_url, declare_topology, publish_json


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    with rabbitpy.Connection(amqp_url()) as conn:
        with conn.channel() as channel:
            exchange = declare_topology(channel)
            temp_c = 22.0 + random.uniform(-0.2, 0.2)
            last_sent: float | None = None

            print(f"[indoor] {SENSOR_ID_INDOOR} → fila {QUEUE_SENSOR_INDOOR} (rk={RK_SENSOR_INDOOR})")
            print(f"Publica quando |Δ| >= {SENSOR_PUBLISH_DELTA_C} °C (Ctrl+C para sair)")

            try:
                while True:
                    drift = random.uniform(-0.3, 0.35)
                    temp_c = max(15.0, min(35.0, temp_c + drift))

                    if last_sent is None or abs(temp_c - last_sent) >= SENSOR_PUBLISH_DELTA_C:
                        payload = {
                            "sensor_id": SENSOR_ID_INDOOR,
                            "zone": "indoor",
                            "celsius": round(temp_c, 2),
                            "observed_at": _now_iso(),
                        }
                        publish_json(channel, exchange, RK_SENSOR_INDOOR, payload)
                        print(f"Enviado: {payload}")
                        last_sent = temp_c

                    time.sleep(random.uniform(0.5, 1.0))
            except KeyboardInterrupt:
                print("\nEncerrando produtor indoor.")


if __name__ == "__main__":
    main()
