"""
Persistência: consome agregados da fila dedicada e grava em SQLite
(tarefa de armazenamento, equivalente ao api_server Kafka no Ex. 09).
"""
from __future__ import annotations

import logging

from const import QUEUE_AGGREGATES_PERSIST
from db_store import AggregateStore
from mq_common import consume_json


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    store = AggregateStore()
    print(f"Persistência em {store.path} ← fila {QUEUE_AGGREGATES_PERSIST}")

    def handle(agg: dict) -> None:
        row_id = store.insert(
            sensor_id=agg["sensor_id"],
            window_start_iso=agg["window_start_iso"],
            window_end_iso=agg["window_end_iso"],
            avg_celsius=float(agg["avg_celsius"]),
            sample_count=int(agg["sample_count"]),
            computed_at_iso=agg["computed_at_iso"],
        )
        logging.info(
            "SQLite id=%s sensor=%s avg=%s",
            row_id,
            agg["sensor_id"],
            agg["avg_celsius"],
        )

    consume_json(QUEUE_AGGREGATES_PERSIST, handle)


if __name__ == "__main__":
    main()
