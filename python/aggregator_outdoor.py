"""Agregador dedicado às leituras outdoor."""
from const import QUEUE_SENSOR_OUTDOOR
from aggregator_worker import run_aggregator

if __name__ == "__main__":
    run_aggregator(QUEUE_SENSOR_OUTDOOR, "outdoor")
