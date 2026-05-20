"""
Consulta local ao SQLite (equivalente ao cliente REST/gRPC dos exercícios 9–10).
Execute após persist_consumer ter gravado agregados.
"""
from __future__ import annotations

from const import SENSOR_ID_INDOOR, SENSOR_ID_OUTDOOR
from db_store import AggregateStore


def main() -> None:
    store = AggregateStore()
    print(f"Banco: {store.path}\n")

    for sensor_id in (SENSOR_ID_INDOOR, SENSOR_ID_OUTDOOR):
        row = store.get_latest(sensor_id)
        if row is None:
            print(f"{sensor_id}: (sem dados)")
        else:
            print(
                f"{sensor_id}: avg={row['avg_celsius']}°C "
                f"amostras={row['sample_count']} em {row['computed_at_iso']}"
            )

    print("\nÚltimo agregado por sensor:")
    for row in store.list_latest_per_sensor():
        print(
            f" - {row['sensor_id']}: {row['avg_celsius']}°C "
            f"({row['sample_count']} amostras)"
        )


if __name__ == "__main__":
    main()
