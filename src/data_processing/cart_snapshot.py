"""Snapshot Redis cart contents into Postgres for the BI dashboard (DPFMD-40).

cartservice stores carts in Redis as protobuf ``Hipstershop.Cart`` messages
(``protos/demo.proto``) and emits no Kafka events, so the data platform has no
add-to-cart history. This job samples the current carts instead: one row per
product per snapshot in ``analytics.cart_snapshots``. The Superset dataset
compares those open cart units with paid units from the Gold fact and labels
the result as a proxy, not a session funnel.

The protobuf decoder is deliberately minimal: Cart has one repeated message
field (items) and CartItem has one string and one int32. Pulling in protoc for
two messages would add a code-generation step to the data_processing image.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import psycopg2
import psycopg2.extras
import redis

logger = logging.getLogger("cart-snapshot")

RETENTION_DAYS = int(os.environ.get("SNAPSHOT_RETENTION_DAYS", "7"))
SCAN_COUNT = int(os.environ.get("REDIS_SCAN_COUNT", "500"))

CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS analytics"

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS analytics.cart_snapshots (
    snapshot_ts TIMESTAMPTZ NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    units_in_cart BIGINT NOT NULL,
    carts_with_product BIGINT NOT NULL,
    PRIMARY KEY (snapshot_ts, product_id)
)
"""

CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_cart_snapshots_snapshot_ts
    ON analytics.cart_snapshots(snapshot_ts)
"""

INSERT_ROWS = """
INSERT INTO analytics.cart_snapshots (
    snapshot_ts, product_id, units_in_cart, carts_with_product
) VALUES %s
ON CONFLICT (snapshot_ts, product_id) DO UPDATE SET
    units_in_cart = EXCLUDED.units_in_cart,
    carts_with_product = EXCLUDED.carts_with_product
"""

DELETE_OLD = """
DELETE FROM analytics.cart_snapshots WHERE snapshot_ts < %s
"""


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        if offset >= len(data):
            raise ValueError("truncated varint")
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")


def _fields(data: bytes):
    offset = 0
    while offset < len(data):
        key, offset = _read_varint(data, offset)
        field, wire = key >> 3, key & 0x07
        if wire == 0:
            value, offset = _read_varint(data, offset)
        elif wire == 1:
            if offset + 8 > len(data):
                raise ValueError("truncated fixed64")
            value, offset = data[offset : offset + 8], offset + 8
        elif wire == 2:
            length, offset = _read_varint(data, offset)
            if offset + length > len(data):
                raise ValueError("truncated bytes")
            value, offset = data[offset : offset + length], offset + length
        elif wire == 5:
            if offset + 4 > len(data):
                raise ValueError("truncated fixed32")
            value, offset = data[offset : offset + 4], offset + 4
        else:
            raise ValueError(f"unsupported wire type {wire}")
        yield field, wire, value


def _decode_item(blob: bytes) -> tuple[str, int]:
    product_id = ""
    quantity = 0
    for field, wire, value in _fields(blob):
        if field == 1 and wire == 2:
            product_id = value.decode("utf-8")
        elif field == 2 and wire == 0:
            quantity = value
    return product_id, quantity


def decode_cart(blob: bytes) -> list[tuple[str, int]]:
    """Decode a Hipstershop.Cart protobuf into (product_id, quantity) pairs."""
    items = []
    for field, wire, value in _fields(blob):
        if field == 2 and wire == 2:
            items.append(_decode_item(value))
    return items


def _redis_client() -> redis.Redis:
    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "redis-cart"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
        db=int(os.environ.get("REDIS_DB", "0")),
        socket_connect_timeout=5,
        socket_timeout=10,
    )


def _postgres_connection():
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "postgres"),
        port=os.environ.get("PG_PORT", "5432"),
        dbname=os.environ.get("PG_DATABASE", "product_catalog"),
        user=os.environ.get("PG_USER", "boutique"),
        password=os.environ.get("PG_PASSWORD", "boutique_pass"),
    )


def _snapshot(client: redis.Redis) -> dict[str, list[int]]:
    aggregate: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    cursor = 0
    carts = 0
    skipped = 0

    while True:
        cursor, keys = client.scan(cursor=cursor, count=SCAN_COUNT)
        if keys:
            for key, value in zip(keys, client.mget(keys)):
                if not value:
                    continue
                try:
                    items = decode_cart(value)
                except (IndexError, ValueError) as error:
                    logger.warning("skipping malformed cart %r: %s", key, error)
                    skipped += 1
                    continue
                for product_id, quantity in items:
                    if not product_id or quantity <= 0:
                        continue
                    aggregate[product_id][0] += quantity
                    aggregate[product_id][1] += 1
                carts += 1
        if cursor == 0:
            break

    logger.info(
        "scanned %d cart(s), %d empty/malformed skipped, %d product(s)",
        carts,
        skipped,
        len(aggregate),
    )
    return aggregate


def _persist(conn, snapshot_ts: datetime, aggregate: dict[str, list[int]]) -> None:
    rows = [
        (snapshot_ts, product_id, units, carts)
        for product_id, (units, carts) in aggregate.items()
    ]
    cutoff = snapshot_ts - timedelta(days=RETENTION_DAYS)

    with conn.cursor() as cursor:
        cursor.execute(CREATE_SCHEMA)
        cursor.execute(CREATE_TABLE)
        cursor.execute(CREATE_INDEX)
        if rows:
            psycopg2.extras.execute_values(cursor, INSERT_ROWS, rows)
        cursor.execute(DELETE_OLD, (cutoff,))
    conn.commit()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    client = _redis_client()
    conn = _postgres_connection()
    try:
        snapshot_ts = datetime.now(UTC).replace(microsecond=0)
        aggregate = _snapshot(client)
        _persist(conn, snapshot_ts, aggregate)
    finally:
        client.close()
        conn.close()

    print(
        f"cart snapshot {snapshot_ts.isoformat()}: "
        f"{len(aggregate)} product(s), {sum(units for units, _ in aggregate.values())} unit(s)",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
