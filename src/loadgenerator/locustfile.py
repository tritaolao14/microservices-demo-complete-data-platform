#!/usr/bin/python
#
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import json
import logging
import os
import random
import time
import uuid

import pg8000.native
from faker import Faker
from locust import FastHttpUser, TaskSet, between

fake = Faker()
logger = logging.getLogger("loadgenerator")
_UTC = datetime.timezone(datetime.timedelta(0))

KAFKA_ORDER_VARIANTS = os.getenv("KAFKA_ORDER_VARIANTS", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
KAFKA_RETRY_TOPIC = os.getenv("KAFKA_RETRY_TOPIC", "orders_retry")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "orders_dlq")
KAFKA_SEND_TIMEOUT_SECONDS = float(os.getenv("KAFKA_SEND_TIMEOUT_SECONDS", "5"))

try:
    KAFKA_VARIANT_TASK_WEIGHT = max(0, int(os.getenv("KAFKA_VARIANT_TASK_WEIGHT", "1")))
except ValueError:
    KAFKA_VARIANT_TASK_WEIGHT = 1

products = ['0PUK6V6EV0']  # Fallback
try:
    conn = pg8000.native.Connection(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=5432,
        user=os.environ.get("POSTGRES_USER", "boutique"),
        password=os.environ.get("POSTGRES_PASSWORD", "boutique_pass"),
        database=os.environ.get("POSTGRES_DB", "product_catalog"),
    )
    rows = conn.run("SELECT id FROM products")
    fetched_products = [row[0] for row in rows]
    if fetched_products:
        products = fetched_products
    conn.close()
except Exception as e:
    print(f"Failed to fetch products from DB: {e}")

CURRENCIES = ['EUR', 'USD', 'JPY', 'CAD', 'GBP', 'TRY']

ORDERS_VARIANTS = (
    "valid",
    "missing_order_id",
    "missing_items",
    "missing_timestamp",
    "missing_product_id",
    "numeric_overflow",
    "string_too_long",
    "bad_timestamp",
)
RETRY_VARIANTS = {
    "retry_missing_timestamp": ("missing_timestamp", "Retryable DB: simulated missing timestamp", 1),
    "retry_missing_order_id": ("missing_order_id", "Retryable DB: simulated missing order_id", 2),
    "retry_numeric_overflow": ("numeric_overflow", "Retryable DB: simulated numeric overflow", 1),
}
DLQ_VARIANTS = {
    "dlq_missing_timestamp": ("missing_timestamp", "InvalidOrder: Missing timestamp", 3),
    "dlq_numeric_overflow": ("numeric_overflow", "NonRetryable DB: simulated numeric field overflow", 3),
}
VARIANT_WEIGHTS = {
    "valid": 2,
    "missing_order_id": 1,
    "missing_items": 1,
    "missing_timestamp": 1,
    "missing_product_id": 1,
    "numeric_overflow": 1,
    "string_too_long": 1,
    "bad_timestamp": 1,
    "retry_missing_timestamp": 1,
    "retry_missing_order_id": 1,
    "retry_numeric_overflow": 1,
    "dlq_missing_timestamp": 1,
    "dlq_numeric_overflow": 1,
}

_kafka_producer = None
_kafka_disabled_until = 0.0


def _now_utc():
    return datetime.datetime.now(_UTC)


def _money(units=None, nanos=None):
    return {
        "currency_code": random.choice(CURRENCIES),
        "units": units if units is not None else random.randint(1, 200),
        "nanos": nanos if nanos is not None else random.randint(0, 999_999_999),
    }


def _address():
    return {
        "street_address": fake.street_address(),
        "city": fake.city(),
        "state": fake.state_abbr(),
        "country": fake.country(),
        "zip_code": random.randint(10000, 99999),
    }


def _item(product_id=None, quantity=None, cost=None):
    return {
        "item": {
            "product_id": product_id if product_id is not None else random.choice(products),
            "quantity": quantity if quantity is not None else random.randint(1, 10),
        },
        "cost": cost if cost is not None else _money(),
    }


def _base_order(kind: str):
    event_id = f"lg-{kind}-{uuid.uuid4().hex[:12]}"
    items = [_item() for _ in range(random.randint(1, 3))]
    order = {
        "order_id": event_id,
        "shipping_tracking_id": uuid.uuid4().hex,
        "shipping_cost": _money(units=random.randint(1, 25)),
        "shipping_address": _address(),
        "items": items,
        "timestamp": _now_utc().isoformat().replace("+00:00", "Z"),
    }
    return order, event_id


def build_order(kind="valid"):
    order, event_id = _base_order(kind)
    if kind == "missing_order_id":
        order["order_id"] = ""
    elif kind == "missing_items":
        order["items"] = []
    elif kind == "missing_timestamp":
        order["timestamp"] = ""
    elif kind == "missing_product_id":
        order["items"][0]["item"]["product_id"] = ""
    elif kind == "numeric_overflow":
        order["items"] = [order["items"][0]]
        order["items"][0]["item"]["quantity"] = 1
        order["items"][0]["cost"] = _money(units=10**12, nanos=0)
    elif kind == "string_too_long":
        order["items"][0]["item"]["product_id"] = "P" * 300
    elif kind == "bad_timestamp":
        order["timestamp"] = "2026-13-40T99:99:99Z"
    return order, event_id


def _wrap(original, error, attempt):
    return {
        "original": original,
        "error": error,
        "attempt": attempt,
        "timestamp": _now_utc().isoformat(),
    }


def _variant_message(kind):
    if kind in ORDERS_VARIANTS:
        order, event_id = build_order(kind)
        return KAFKA_TOPIC, order, event_id, None
    if kind in RETRY_VARIANTS:
        source, error, attempt = RETRY_VARIANTS[kind]
        original, event_id = build_order(source)
        return (
            KAFKA_RETRY_TOPIC,
            _wrap(original, error, attempt),
            event_id,
            [("x-retry-count", str(attempt).encode("utf-8"))],
        )
    if kind in DLQ_VARIANTS:
        source, error, attempt = DLQ_VARIANTS[kind]
        original, event_id = build_order(source)
        return KAFKA_DLQ_TOPIC, _wrap(original, error, attempt), event_id, None
    return None, None, None, None


def _get_kafka_producer():
    global _kafka_producer, _kafka_disabled_until
    if _kafka_producer is not None:
        return _kafka_producer
    if time.monotonic() < _kafka_disabled_until:
        return None
    try:
        from kafka import KafkaProducer

        _kafka_producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=2,
            request_timeout_ms=15000,
            api_version_auto_timeout_ms=5000,
        )
        logger.info("Kafka producer ready for %s", KAFKA_BROKER)
        return _kafka_producer
    except Exception as exc:  # noqa: BLE001
        logger.warning("Kafka producer unavailable: %s", exc)
        _kafka_disabled_until = time.monotonic() + 60
        return None


def _publish(topic, payload, key, headers):
    global _kafka_disabled_until
    producer = _get_kafka_producer()
    if producer is None:
        return False
    try:
        future = producer.send(topic, value=payload, key=key, headers=headers)
        future.get(timeout=KAFKA_SEND_TIMEOUT_SECONDS)
        producer.flush(timeout=KAFKA_SEND_TIMEOUT_SECONDS)
        logger.info("published %s to %s", key, topic)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.error("failed to publish to %s: %s", topic, exc)
        _kafka_disabled_until = time.monotonic() + 60
        return False


def kafka_order_variant(l):
    if not KAFKA_ORDER_VARIANTS:
        return
    kind = random.choices(list(VARIANT_WEIGHTS), weights=list(VARIANT_WEIGHTS.values()))[0]
    topic, payload, key, headers = _variant_message(kind)
    if topic is None:
        return
    _publish(topic, payload, key=key, headers=headers)


def index(l):
    l.client.get("/")


def setCurrency(l):
    currencies = ['EUR', 'USD', 'JPY', 'CAD', 'GBP', 'TRY']
    l.client.post("/setCurrency",
        {'currency_code': random.choice(currencies)})


def browseProduct(l):
    l.client.get("/product/" + random.choice(products))


def viewCart(l):
    l.client.get("/cart")


def addToCart(l):
    product = random.choice(products)
    l.client.get("/product/" + product)
    l.client.post("/cart", {
        'product_id': product,
        'quantity': random.randint(1,10)})


def empty_cart(l):
    l.client.post('/cart/empty')


def checkout(l):
    addToCart(l)
    current_year = datetime.datetime.now().year+1
    l.client.post("/cart/checkout", {
        'email': fake.email(),
        'street_address': fake.street_address(),
        'zip_code': fake.zipcode(),
        'city': fake.city(),
        'state': fake.state_abbr(),
        'country': fake.country(),
        'credit_card_number': fake.credit_card_number(card_type="visa"),
        'credit_card_expiration_month': random.randint(1, 12),
        'credit_card_expiration_year': random.randint(current_year, current_year + 70),
        'credit_card_cvv': f"{random.randint(100, 999)}",
    })


def logout(l):
    l.client.get('/logout')


user_tasks = {
    index: 1,
    setCurrency: 2,
    browseProduct: 10,
    addToCart: 2,
    viewCart: 3,
    checkout: 1,
}
if KAFKA_ORDER_VARIANTS and KAFKA_VARIANT_TASK_WEIGHT:
    user_tasks[kafka_order_variant] = KAFKA_VARIANT_TASK_WEIGHT

class UserBehavior(TaskSet):

    def on_start(self):
        index(self)

    tasks = user_tasks

class WebsiteUser(FastHttpUser):
    tasks = [UserBehavior]
    wait_time = between(1, 10)
