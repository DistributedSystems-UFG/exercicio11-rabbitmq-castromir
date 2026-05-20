"""Agregador dedicado às leituras indoor."""
from const import QUEUE_SENSOR_INDOOR
from aggregator_worker import run_aggregator

if __name__ == "__main__":
    run_aggregator(QUEUE_SENSOR_INDOOR, "indoor")
