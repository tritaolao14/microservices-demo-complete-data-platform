"""Bootstrap the Iceberg ``bronze.orders`` table from the Bronze Parquet zone.

DPFMD-35. The DPFMD-34 Bronze writer lands raw order events under
``s3a://lakehouse/bronze/orders/{yyyy}/{mm}/{dd}/part-*.parquet`` (7 columns,
plus ``_manifest.json`` / ``_SUCCESS`` markers). This job wraps that landing
zone in an Iceberg table in the JDBC catalog configured by the
``spark-iceberg-config`` ConfigMap:

    catalog   my_catalog (type=jdbc, Postgres product_catalog)
    table     my_catalog.bronze.orders
    partition days(_event_date)

Idempotent on purpose: the table is created if missing and data is loaded only
while it is empty, so re-running the Job (ArgoCD re-syncs the dev overlay)
never duplicates rows.
"""

import logging
import sys

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("bronze.orders")

CATALOG = "my_catalog"
NAMESPACE = "bronze"
TABLE = f"{CATALOG}.{NAMESPACE}.orders"
SOURCE = "s3a://lakehouse/bronze/orders"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    raw              string,
    _ingested_at     timestamp,
    _event_date      date,
    _source_topic    string,
    _kafka_partition int,
    _kafka_offset    bigint,
    _schema_version  string
)
USING iceberg
PARTITIONED BY (days(_event_date))
"""


def load(spark: SparkSession) -> int:
    """Create the table if needed and load Bronze Parquet when it is empty."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    spark.sql(DDL)

    existing = spark.sql(f"SELECT count(*) AS n FROM {TABLE}").first()["n"]
    if existing:
        log.info("table %s already holds %d row(s); nothing to load", TABLE, existing)
        return existing

    log.info("reading Bronze Parquet from %s ...", SOURCE)
    try:
        # recursiveFileLookup is required, not an optimisation. The Bronze zone
        # is partitioned by date as {yyyy}/{mm}/{dd}; left to its default
        # partition inference Spark tries to turn those numeric directory
        # segments into partition columns and ends up with an empty file list,
        # failing with UNABLE_TO_INFER_SCHEMA on every parent directory (only
        # leaf date directories resolve). Turning inference off makes the tree
        # a flat set of files, which is what is wanted here: _event_date is
        # already a real column inside the Parquet files, so nothing is lost.
        source = spark.read.option("recursiveFileLookup", "true").parquet(SOURCE)
        count = source.count()
    except Exception as exc:  # noqa: BLE001 - empty/missing source is not fatal
        log.warning("no readable Bronze Parquet under %s (%s)", SOURCE, exc)
        return 0
    if count == 0:
        log.warning("Bronze Parquet under %s is empty; table left empty", SOURCE)
        return 0

    source.createOrReplaceTempView("bronze_orders_source")
    spark.sql(
        f"INSERT INTO {TABLE} "
        "SELECT raw, "
        "CAST(_ingested_at AS timestamp), "
        "CAST(_event_date AS date), "
        "_source_topic, _kafka_partition, _kafka_offset, _schema_version "
        "FROM bronze_orders_source"
    )
    loaded = spark.sql(f"SELECT count(*) AS n FROM {TABLE}").first()["n"]
    log.info("loaded %d row(s) into %s (source had %d)", loaded, TABLE, count)
    spark.sql(f"SELECT * FROM {TABLE}.snapshots").show(truncate=False)
    return loaded


def main() -> int:
    spark = SparkSession.builder.appName("bronze-orders-iceberg-load").getOrCreate()
    try:
        load(spark)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
