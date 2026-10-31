"""Great Expectations checkpoint for gold.fact_orders_items (DPFMD-39).

The checkpoint validates three independently useful things and exits non-zero
when any expectation fails, so the CronJob records a failed Job:

1. Trino `iceberg.gold.fact_orders_items` in the reconciliation window has no
   null keys, no duplicate order-item grain, non-negative prices and the
   expected status.
2. Postgres `analytics.order_items` in the same window has the exact reference
   totals from the reconciliation contract (docs/data-engineering-patterns.md).
3. Trino and Postgres agree: fact totals equal the contract and both deltas are
   zero, so the two systems cannot drift independently.

Trino query results are materialized into a temporary SQLite database before
validation. GX 1.3's Trino SQLAlchemy dialect does not expose the query's column
names to the batch ("The column ... in BatchData does not exist"), while the
same rows validate correctly when read through SQLite. The Trino engine still
executes every query, so the data under test is the live Iceberg/Postgres data.
"""

from __future__ import annotations

import os
import re
import shutil
import sqlite3
import sys
import tempfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote

import great_expectations as gx
import pandas as pd
from sqlalchemy import create_engine

WINDOW_START_DEFAULT = "2026-10-11"
WINDOW_END_DEFAULT = "2026-10-15"

EXPECTED_ITEMS = 67_013
EXPECTED_UNITS = 451_674
EXPECTED_REVENUE_CENTS = 33_652_418_088  # 336,524,180.88 currency units

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _window(name: str, default: str) -> str:
    value = os.environ.get(name, default)
    if not _DATE_RE.match(value):
        raise ValueError(f"{name} must be YYYY-MM-DD, got {value!r}")
    return value


def _trino_connection_string() -> str:
    scheme = os.environ.get("TRINO_HTTP_SCHEME", "http")
    connection = (
        f"trino://{os.environ.get('TRINO_USER', 'dbt')}"
        f"@{os.environ.get('TRINO_HOST', 'trino')}:{os.environ.get('TRINO_PORT', '8080')}"
        f"/{os.environ.get('TRINO_CATALOG', 'iceberg')}"
        f"/{os.environ.get('TRINO_SCHEMA', 'gold')}"
    )
    if scheme != "http":
        connection += f"?http_scheme={scheme}"
    return connection


def _postgres_connection_string() -> str:
    user = quote(os.environ.get("PG_USER", "boutique"), safe="")
    password = quote(os.environ.get("PG_PASSWORD", "boutique_pass"), safe="")
    host = os.environ.get("PG_HOST", "postgres")
    port = os.environ.get("PG_PORT", "5432")
    database = os.environ.get("PG_DATABASE", "product_catalog")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def _fact_rows_query(start: str, end: str) -> str:
    catalog = os.environ.get("TRINO_CATALOG", "iceberg")
    return f"""
        SELECT order_id, product_id, quantity, unit_price, total_price,
               status, event_date
        FROM {catalog}.gold.fact_orders_items
        WHERE event_date BETWEEN DATE '{start}' AND DATE '{end}'
    """


def _postgres_reference_query(start: str, end: str) -> str:
    return f"""
        SELECT count(*) AS reference_items,
               CAST(COALESCE(sum(quantity), 0) AS BIGINT) AS reference_units,
               CAST(COALESCE(sum(total_price * 100), 0) AS BIGINT)
                   AS reference_revenue_cents
        FROM analytics.order_items
        WHERE CAST(event_timestamp AS date) BETWEEN DATE '{start}' AND DATE '{end}'
    """


def _reconcile_query(start: str, end: str) -> str:
    catalog = os.environ.get("TRINO_CATALOG", "iceberg")
    postgres_catalog = os.environ.get("TRINO_POSTGRES_CATALOG", "postgres")
    return f"""
        SELECT
            f.fact_items,
            f.fact_units,
            f.fact_revenue_cents,
            r.reference_items,
            r.reference_units,
            r.reference_revenue_cents,
            f.fact_items - r.reference_items AS items_delta,
            f.fact_units - r.reference_units AS units_delta,
            f.fact_revenue_cents - r.reference_revenue_cents AS revenue_cents_delta,
            f.fact_price_mismatch_rows
        FROM (
            SELECT
                count(*) AS fact_items,
                CAST(COALESCE(sum(quantity), 0) AS BIGINT) AS fact_units,
                CAST(COALESCE(sum(total_price * 100), 0) AS BIGINT) AS fact_revenue_cents,
                -- unit_price is stored rounded to 2 dp while total_price is
                -- computed from the unrounded price, so a legitimate row can
                -- differ by up to half a cent per unit.
                count_if(
                    abs(total_price - quantity * unit_price)
                        > quantity * DECIMAL '0.005' + DECIMAL '0.01'
                ) AS fact_price_mismatch_rows
            FROM {catalog}.gold.fact_orders_items
            WHERE event_date BETWEEN DATE '{start}' AND DATE '{end}'
        ) f
        CROSS JOIN (
            SELECT
                count(*) AS reference_items,
                CAST(COALESCE(sum(quantity), 0) AS BIGINT) AS reference_units,
                CAST(COALESCE(sum(total_price * 100), 0) AS BIGINT) AS reference_revenue_cents
            FROM {postgres_catalog}.analytics.order_items
            WHERE CAST(event_timestamp AS date) BETWEEN DATE '{start}' AND DATE '{end}'
        ) r
    """


def _exact(column: str, value: int):
    return gx.expectations.ExpectColumnValuesToBeBetween(
        column=column, min_value=value, max_value=value
    )


def _fact_expectations():
    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
        for column in (
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "total_price",
            "status",
            "event_date",
        )
    ]
    expectations.extend(
        [
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["order_id", "product_id"]
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="quantity", min_value=1
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="unit_price", min_value=0
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="total_price", min_value=0
            ),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="status", value_set=["SUCCEED"]
            ),
        ]
    )
    return expectations


def _reference_expectations():
    return [
        _exact("reference_items", EXPECTED_ITEMS),
        _exact("reference_units", EXPECTED_UNITS),
        _exact("reference_revenue_cents", EXPECTED_REVENUE_CENTS),
    ]


def _reconcile_expectations():
    return [
        _exact("fact_items", EXPECTED_ITEMS),
        _exact("fact_units", EXPECTED_UNITS),
        _exact("fact_revenue_cents", EXPECTED_REVENUE_CENTS),
        _exact("items_delta", 0),
        _exact("units_delta", 0),
        _exact("revenue_cents_delta", 0),
        _exact("fact_price_mismatch_rows", 0),
    ]


def _add_suite(context, name: str, expectations):
    suite = context.suites.add(gx.ExpectationSuite(name=name))
    for expectation in expectations:
        suite.add_expectation(expectation)
    return suite


def _batch_definition(data_source, name: str, query: str):
    asset = data_source.add_query_asset(name=name, query=query)
    return asset.add_batch_definition_whole_table(name)


def _sqlite_value(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _materialize(engine, query: str, database: Path, table: str) -> None:
    frame = pd.read_sql_query(query, engine)
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = frame[column].map(_sqlite_value)
    with sqlite3.connect(database) as connection:
        frame.to_sql(table, connection, index=False, if_exists="replace")


def _build_context(project_root: Path):
    context_dir = project_root / "great_expectations"
    context_dir.mkdir()
    shutil.copyfile(Path(__file__).with_name("gx.yaml"), context_dir / "gx.yaml")
    return gx.get_context(mode="file", project_root_dir=str(project_root))


def main() -> int:
    start = _window("GE_WINDOW_START", WINDOW_START_DEFAULT)
    end = _window("GE_WINDOW_END", WINDOW_END_DEFAULT)

    project_root = Path(tempfile.mkdtemp(prefix="ge-fact-orders-items-"))
    context = _build_context(project_root)
    snapshot = project_root / "trino_snapshot.sqlite"

    trino_engine = create_engine(_trino_connection_string())
    _materialize(
        trino_engine,
        _fact_rows_query(start, end),
        snapshot,
        "fact_orders_items_window",
    )
    _materialize(
        trino_engine,
        _reconcile_query(start, end),
        snapshot,
        "fact_vs_postgres_window",
    )
    trino_engine.dispose()

    trino = context.data_sources.add_sql(
        name="trino_iceberg", connection_string=f"sqlite:///{snapshot}"
    )
    postgres = context.data_sources.add_sql(
        name="postgres_reference", connection_string=_postgres_connection_string()
    )

    fact_batch = _batch_definition(
        trino, "fact_orders_items_window", "SELECT * FROM fact_orders_items_window"
    )
    reference_batch = _batch_definition(
        postgres, "order_items_window", _postgres_reference_query(start, end)
    )
    reconcile_batch = _batch_definition(
        trino, "fact_vs_postgres_window", "SELECT * FROM fact_vs_postgres_window"
    )

    fact_validation = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="fact_orders_items_window",
            data=fact_batch,
            suite=_add_suite(
                context, "fact_orders_items_window", _fact_expectations()
            ),
        )
    )
    reference_validation = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="postgres_order_items_window",
            data=reference_batch,
            suite=_add_suite(
                context, "postgres_order_items_window", _reference_expectations()
            ),
        )
    )
    reconcile_validation = context.validation_definitions.add(
        gx.ValidationDefinition(
            name="fact_vs_postgres_window",
            data=reconcile_batch,
            suite=_add_suite(
                context, "fact_vs_postgres_window", _reconcile_expectations()
            ),
        )
    )

    checkpoint = context.checkpoints.add(
        gx.Checkpoint(
            name="fact_orders_items_awap",
            validation_definitions=[
                fact_validation,
                reference_validation,
                reconcile_validation,
            ],
        )
    )

    result = checkpoint.run()
    print(result.describe())
    if not result.success:
        print("Great Expectations checkpoint FAILED", file=sys.stderr)
        return 1
    print("Great Expectations checkpoint PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
