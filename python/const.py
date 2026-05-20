import os

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "myuser")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "abc123")
RABBITMQ_VHOST = os.environ.get("RABBITMQ_VHOST", "/")

EXCHANGE_TEMPERATURE = "temperature.topic"

QUEUE_SENSOR_INDOOR = "sensor.readings.indoor"
QUEUE_SENSOR_OUTDOOR = "sensor.readings.outdoor"
QUEUE_AGGREGATES_PERSIST = "temperature.aggregates.persist"
QUEUE_AGGREGATES_ALERT_CHECK = "temperature.aggregates.alerts-check"
QUEUE_ALERTS = "temperature.alerts"

RK_SENSOR_INDOOR = "sensor.indoor"
RK_SENSOR_OUTDOOR = "sensor.outdoor"
RK_AGGREGATE = "aggregate.report"
RK_ALERT = "alert.critical"

WINDOW_HOURS = float(os.environ.get("AGG_WINDOW_HOURS", "2"))
SENSOR_PUBLISH_DELTA_C = float(os.environ.get("SENSOR_DELTA_C", "0.5"))

ALERT_THRESHOLD_INDOOR_C = float(os.environ.get("ALERT_THRESHOLD_INDOOR_C", "26.0"))
ALERT_THRESHOLD_OUTDOOR_C = float(os.environ.get("ALERT_THRESHOLD_OUTDOOR_C", "32.0"))

DB_PATH = os.environ.get("TEMPERATURE_DB_PATH", "temperature.db")

SENSOR_ID_INDOOR = "sensor-lab-a1-indoor"
SENSOR_ID_OUTDOOR = "sensor-lab-b2-outdoor"
