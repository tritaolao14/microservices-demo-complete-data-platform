"""Bootstrap the Superset metadata for the BI dashboard (DPFMD-40).

The Deployment's initContainer runs this after ``superset db upgrade`` and
``superset init``. Every object is looked up by a stable name and updated in
place, so a pod restart or an image rollout does not create duplicates.

The three datasets are SQL Lab views over Trino:

* ``bi_revenue_by_hour`` reads ``iceberg.gold.fact_orders_items`` (dbt mart).
* ``bi_top_products`` reads the same Gold fact.
* ``bi_cart_abandonment`` combines open cart units from
  ``postgres.analytics.cart_snapshots`` with paid units from Gold. cartservice
  has no event stream, so the CronJob samples Redis every five minutes; the
  resulting ratio is an explicitly documented proxy, not a session funnel.
"""

from __future__ import annotations

import json
import os
import textwrap

from superset.app import create_app

# Model modules are imported lazily inside the Flask app context (see main()):
# superset.models.helpers reads `app.config` at module level, so a top-level
# import raises "Working outside of application context" in a standalone script.
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

DATABASE_NAME = "Trino Iceberg"
DASHBOARD_SLUG = "business-overview"
DASHBOARD_TITLE = "Báo cáo kinh doanh — Online Boutique"
ADMIN_USERNAME = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
ADMIN_EMAIL = os.environ.get("SUPERSET_ADMIN_EMAIL", "admin@local.dev")
TRINO_URI = os.environ.get(
    "SUPERSET_TRINO_URI", "trino://superset@trino:8080/iceberg"
)

REVENUE_BY_HOUR_SQL = textwrap.dedent(
    """\
    SELECT
        date_trunc('hour', event_timestamp) AS event_hour,
        CAST(SUM(total_price) AS DECIMAL(18, 2)) AS revenue,
        CAST(SUM(quantity) AS BIGINT) AS units,
        COUNT(DISTINCT order_id) AS orders
    FROM iceberg.gold.fact_orders_items
    WHERE status = 'SUCCEED'
    GROUP BY 1
    ORDER BY 1 DESC
    """
)

TOP_PRODUCTS_SQL = textwrap.dedent(
    """\
    SELECT
        product_id,
        CAST(SUM(quantity) AS BIGINT) AS units_sold,
        CAST(SUM(total_price) AS DECIMAL(18, 2)) AS revenue,
        COUNT(DISTINCT order_id) AS orders
    FROM iceberg.gold.fact_orders_items
    WHERE status = 'SUCCEED'
    GROUP BY product_id
    ORDER BY units_sold DESC
    """
)

CART_ABANDONMENT_SQL = textwrap.dedent(
    """\
    WITH latest AS (
        SELECT max(snapshot_ts) AS snapshot_ts
        FROM postgres.analytics.cart_snapshots
    ),
    cart AS (
        SELECT
            c.product_id,
            CAST(SUM(c.units_in_cart) AS BIGINT) AS cart_units,
            CAST(SUM(c.carts_with_product) AS BIGINT) AS carts
        FROM postgres.analytics.cart_snapshots c
        CROSS JOIN latest l
        WHERE c.snapshot_ts = l.snapshot_ts
        GROUP BY c.product_id
    ),
    paid AS (
        SELECT
            product_id,
            CAST(SUM(quantity) AS BIGINT) AS paid_units,
            CAST(SUM(total_price) AS DECIMAL(18, 2)) AS paid_revenue
        FROM iceberg.gold.fact_orders_items
        WHERE status = 'SUCCEED'
        GROUP BY product_id
    )
    SELECT
        COALESCE(c.product_id, p.product_id) AS product_id,
        COALESCE(c.cart_units, 0) AS cart_units,
        COALESCE(c.carts, 0) AS carts,
        COALESCE(p.paid_units, 0) AS paid_units,
        CAST(COALESCE(c.cart_units, 0) AS DOUBLE)
            / NULLIF(COALESCE(c.cart_units, 0) + COALESCE(p.paid_units, 0), 0)
            AS cart_not_paid_rate
    FROM cart c
    FULL OUTER JOIN paid p ON c.product_id = p.product_id
    ORDER BY cart_not_paid_rate DESC NULLS LAST
    """
)

DATASETS = [
    {
        "name": "bi_revenue_by_hour",
        "sql": REVENUE_BY_HOUR_SQL,
        "columns": [
            ("event_hour", "TIMESTAMP", True),
            ("revenue", "DECIMAL(18, 2)", False),
            ("units", "BIGINT", False),
            ("orders", "BIGINT", False),
        ],
        "metrics": [
            {"name": "revenue", "expression": "SUM(revenue)", "label": "Doanh thu"},
            {"name": "units", "expression": "SUM(units)", "label": "Số lượng"},
        ],
    },
    {
        "name": "bi_top_products",
        "sql": TOP_PRODUCTS_SQL,
        "columns": [
            ("product_id", "VARCHAR", False),
            ("units_sold", "BIGINT", False),
            ("revenue", "DECIMAL(18, 2)", False),
            ("orders", "BIGINT", False),
        ],
        "metrics": [
            {"name": "units_sold", "expression": "SUM(units_sold)", "label": "Đã bán"},
            {"name": "revenue", "expression": "SUM(revenue)", "label": "Doanh thu"},
        ],
    },
    {
        "name": "bi_cart_abandonment",
        "sql": CART_ABANDONMENT_SQL,
        "columns": [
            ("product_id", "VARCHAR", False),
            ("cart_units", "BIGINT", False),
            ("carts", "BIGINT", False),
            ("paid_units", "BIGINT", False),
            ("cart_not_paid_rate", "DOUBLE", False),
        ],
        "metrics": [],
    },
]


def _admin_user(security_manager, db):
    admin = security_manager.find_user(username=ADMIN_USERNAME)
    if admin is not None:
        return admin

    role = security_manager.find_role("Admin")
    if role is None:
        raise RuntimeError("Admin role is missing; run `superset init` first")

    admin = security_manager.add_user(
        username=ADMIN_USERNAME,
        first_name="Superset",
        last_name="Admin",
        email=ADMIN_EMAIL,
        role=role,
        password=os.environ["SUPERSET_ADMIN_PASSWORD"],
    )
    db.session.commit()
    return admin


def _database(db, Database):
    database = (
        db.session.query(Database)
        .filter_by(database_name=DATABASE_NAME)
        .one_or_none()
    )
    if database is None:
        database = Database(
            database_name=DATABASE_NAME,
            sqlalchemy_uri=TRINO_URI,
            expose_in_sqllab=True,
        )
        db.session.add(database)
    else:
        database.sqlalchemy_uri = TRINO_URI
    database.expose_in_sqllab = True
    database.allow_ctas = False
    database.allow_cvas = False
    database.allow_dml = False
    db.session.commit()
    return database


def _dataset(db, models, database, spec):
    SqlaTable, TableColumn, SqlMetric = models
    dataset = (
        db.session.query(SqlaTable)
        .filter_by(table_name=spec["name"], database_id=database.id)
        .one_or_none()
    )
    if dataset is None:
        dataset = SqlaTable(
            table_name=spec["name"], database=database, sql=spec["sql"]
        )
        db.session.add(dataset)
        db.session.flush()

    dataset.database = database
    dataset.sql = spec["sql"]
    dataset.schema = None
    dataset.is_sqllab_view = True
    dataset.main_dttm_col = next(
        (name for name, _, is_temporal in spec["columns"] if is_temporal), None
    )

    for column in list(dataset.columns):
        db.session.delete(column)
    for metric in list(dataset.metrics):
        db.session.delete(metric)
    db.session.flush()

    for name, sql_type, is_temporal in spec["columns"]:
        dataset.columns.append(
            TableColumn(column_name=name, type=sql_type, is_dttm=is_temporal)
        )
    for metric in spec["metrics"]:
        dataset.metrics.append(
            SqlMetric(
                metric_name=metric["name"],
                expression=metric["expression"],
                metric_type="sum",
                verbose_name=metric["label"],
            )
        )
    db.session.commit()
    return dataset


def _raw_table_params(columns, order_by, row_limit):
    return {
        "viz_type": "table",
        "query_mode": "raw",
        "all_columns": columns,
        "order_by_cols": [json.dumps([order_by, False])],
        "row_limit": row_limit,
        "time_range": "No filter",
        "adhoc_filters": [],
        "groupby": [],
        "metrics": [],
        "include_search": False,
        "table_timestamp_format": "smart_date",
    }


def _chart(db, Slice, dataset, name, params, description, admin):
    chart = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
    if chart is None:
        chart = Slice(slice_name=name)
        db.session.add(chart)

    chart.viz_type = "table"
    chart.datasource_type = "table"
    chart.datasource_id = dataset.id
    chart.datasource_name = dataset.table_name
    chart.params = json.dumps(
        {**params, "datasource": f"{dataset.id}__table"}, ensure_ascii=False
    )
    chart.description = description
    chart.cache_timeout = -1
    chart.owners = [admin]
    db.session.commit()
    return chart


def _layout(charts):
    root = {
        "type": "ROOT",
        "id": "ROOT_ID",
        "children": ["GRID_ID"],
        "parents": [],
    }
    grid = {
        "type": "GRID",
        "id": "GRID_ID",
        "children": [],
        "parents": ["ROOT_ID"],
    }
    layout = {"DASHBOARD_VERSION_KEY": "v2", "ROOT_ID": root, "GRID_ID": grid}

    for index, chart in enumerate(charts):
        row_id = f"ROW-{index}"
        chart_id = f"CHART-{chart.id}"
        layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [chart_id],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        layout[chart_id] = {
            "type": "CHART",
            "id": chart_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {
                "chartId": chart.id,
                "width": 12,
                "height": 50,
                "sliceName": chart.slice_name,
                "uuid": str(chart.uuid),
            },
        }
        grid["children"].append(row_id)
    return layout


def _dashboard(db, Dashboard, charts, admin):
    dashboard = (
        db.session.query(Dashboard).filter_by(slug=DASHBOARD_SLUG).one_or_none()
    )
    if dashboard is None:
        dashboard = Dashboard(
            dashboard_title=DASHBOARD_TITLE, slug=DASHBOARD_SLUG
        )
        db.session.add(dashboard)

    dashboard.dashboard_title = DASHBOARD_TITLE
    dashboard.published = True
    dashboard.slices = charts
    dashboard.owners = [admin]
    db.session.commit()

    dashboard.position_json = json.dumps(_layout(charts), ensure_ascii=False)
    dashboard.json_metadata = json.dumps(
        {
            "refresh_frequency": 0,
            "color_scheme": "supersetColors",
            "expanded_slices": {},
            "default_filters": "[]",
            "filter_scopes": {},
            "timed_refresh_immune_slices": [],
        }
    )
    db.session.commit()
    return dashboard


def main() -> int:
    app = create_app()
    with app.app_context():
        from superset import db, security_manager
        from superset.connectors.sqla.models import SqlMetric, SqlaTable, TableColumn
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        admin = _admin_user(security_manager, db)
        database = _database(db, Database)
        dataset_models = (SqlaTable, TableColumn, SqlMetric)
        datasets = [_dataset(db, dataset_models, database, spec) for spec in DATASETS]

        charts = [
            _chart(
                db,
                Slice,
                datasets[0],
                "Doanh thu theo giờ (Gold)",
                _raw_table_params(
                    ["event_hour", "revenue", "units", "orders"],
                    "event_hour",
                    168,
                ),
                "Doanh thu, số lượng và số đơn theo từng giờ từ "
                "iceberg.gold.fact_orders_items.",
                admin,
            ),
            _chart(
                db,
                Slice,
                datasets[1],
                "Sản phẩm bán chạy nhất",
                _raw_table_params(
                    ["product_id", "units_sold", "revenue", "orders"],
                    "units_sold",
                    20,
                ),
                "Top 20 sản phẩm theo số lượng bán (order-item grain) từ Gold.",
                admin,
            ),
            _chart(
                db,
                Slice,
                datasets[2],
                "Sản phẩm bỏ giỏ chưa thanh toán (proxy)",
                _raw_table_params(
                    [
                        "product_id",
                        "cart_units",
                        "carts",
                        "paid_units",
                        "cart_not_paid_rate",
                    ],
                    "cart_not_paid_rate",
                    20,
                ),
                "Tỷ lệ đơn vị đang nằm trong giỏ chưa thanh toán so với số đã "
                "thanh toán, từ snapshot Redis (CronJob cart-snapshot) và Gold.",
                admin,
            ),
        ]

        dashboard = _dashboard(db, Dashboard, charts, admin)
        print(
            "Superset bootstrap complete: "
            f"dashboard={dashboard.slug} charts={len(charts)} "
            f"datasets={len(datasets)} database={database.database_name}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
