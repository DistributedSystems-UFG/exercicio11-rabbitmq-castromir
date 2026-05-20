"""Utilitários AMQP compartilhados (topologia, publicação e consumo JSON)."""
from __future__ import annotations

import json
from typing import Any, Callable
from urllib.parse import quote

import rabbitpy

from const import (
    EXCHANGE_TEMPERATURE,
    QUEUE_AGGREGATES_ALERT_CHECK,
    QUEUE_AGGREGATES_PERSIST,
    QUEUE_ALERTS,
    QUEUE_SENSOR_INDOOR,
    QUEUE_SENSOR_OUTDOOR,
    RABBITMQ_HOST,
    RABBITMQ_PASS,
    RABBITMQ_PORT,
    RABBITMQ_USER,
    RABBITMQ_VHOST,
    RK_AGGREGATE,
    RK_ALERT,
    RK_SENSOR_INDOOR,
    RK_SENSOR_OUTDOOR,
)


def amqp_url() -> str:
    user = quote(RABBITMQ_USER, safe="")
    password = quote(RABBITMQ_PASS, safe="")
    vhost = quote(RABBITMQ_VHOST, safe="")
    return f"amqp://{user}:{password}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/{vhost}"


def declare_topology(channel: rabbitpy.Channel) -> rabbitpy.Exchange:
    exchange = rabbitpy.Exchange(
        channel, EXCHANGE_TEMPERATURE, exchange_type="topic", durable=True
    )
    exchange.declare()

    indoor = rabbitpy.Queue(channel, QUEUE_SENSOR_INDOOR, durable=True, auto_delete=False)
    indoor.declare()
    indoor.bind(exchange, RK_SENSOR_INDOOR)

    outdoor = rabbitpy.Queue(channel, QUEUE_SENSOR_OUTDOOR, durable=True, auto_delete=False)
    outdoor.declare()
    outdoor.bind(exchange, RK_SENSOR_OUTDOOR)

    persist_q = rabbitpy.Queue(
        channel, QUEUE_AGGREGATES_PERSIST, durable=True, auto_delete=False
    )
    persist_q.declare()
    persist_q.bind(exchange, RK_AGGREGATE)

    alert_check_q = rabbitpy.Queue(
        channel, QUEUE_AGGREGATES_ALERT_CHECK, durable=True, auto_delete=False
    )
    alert_check_q.declare()
    alert_check_q.bind(exchange, RK_AGGREGATE)

    alerts_q = rabbitpy.Queue(channel, QUEUE_ALERTS, durable=True, auto_delete=False)
    alerts_q.declare()
    alerts_q.bind(exchange, RK_ALERT)

    return exchange


def publish_json(
    channel: rabbitpy.Channel,
    exchange: rabbitpy.Exchange,
    routing_key: str,
    payload: dict[str, Any],
) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    message = rabbitpy.Message(channel, body, {"content_type": "application/json"})
    message.publish(exchange, routing_key)


def consume_json(
    queue_name: str,
    handler: Callable[[dict[str, Any]], None],
    *,
    setup: bool = True,
) -> None:
    with rabbitpy.Connection(amqp_url()) as conn:
        with conn.channel() as channel:
            if setup:
                declare_topology(channel)
            queue = rabbitpy.Queue(channel, queue_name, durable=True, auto_delete=False)
            for message in queue:
                payload = json.loads(message.body.decode("utf-8"))
                try:
                    handler(payload)
                    message.ack()
                except Exception:
                    message.reject(requeue=True)
                    raise
