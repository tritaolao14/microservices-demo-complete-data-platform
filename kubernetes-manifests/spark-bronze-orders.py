"""Load the Iceberg ``bronze.orders`` table from the Bronze Parquet zone.

DPFMD-35. The DPFMD-34 Bronze writer lands raw order events under
``s3a://lakehouse/bronze/orders/{yyyy}/{mm}/{dd}/part-*.parquet`` (7 columns,
plus ``_manifest.json`` / ``_SUCCESS`` markers). This job wraps that landing
zone in an Iceberg table in the JDBC catalog configured by the
``spark-iceberg-config`` ConfigMap:

    catalog   my_catalog (type=jdbc, Postgres product_catalog)
    table     my_catalog.bronze.orders
    partition days(_event_date)

Load is **incremental**. Earlier revisions loaded only while the table was
empty, which meant the very first batch was the only batch: every later order
stayed in the Parquet zone and the Iceberg table never changed again. Reading
it kept returning the original rows.

Incremental tracking relies on the Bronze writer naming files by content hash
(``part-<sha256(content)[:16]>.parquet``). A file whose contents were already
ingested keeps the same name, so remembering the names that were loaded is
enough to make re-runs safe. ``my_catalog.bronze.ingest_state`` holds those
names and is written in the same job, right after the rows land.

Both steps read the same "files not yet seen" view, so the rows and the state
cannot disagree.
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
STATE_TABLE = f"{CATALOG}.{NAMESPACE}.ingest_state"
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

STATE_DDL = f"""
CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
    path        string,
    rows        bigint,
    ingested_at timestamp
)
USING iceberg
"""

# Derived from the source scan so every row knows which file it came from.
SOURCE_VIEW = """
CREATE OR REPLACE TEMP VIEW bronze_orders_source AS
SELECT *, input_file_name() AS __source_file
FROM bronze_orders_source_raw
"""

NEW_FILES_VIEW = f"""
CREATE OR REPLACE TEMP VIEW bronze_pending_files AS
SELECT DISTINCT s.__source_file
FROM bronze_orders_source s
LEFT ANTI JOIN {STATE_TABLE} st ON s.__source_file = st.path
"""

INSERT_ROWS = f"""
INSERT INTO {TABLE}
SELECT s.raw,
       CAST(s._ingested_at AS timestamp),
       CAST(s._event_date AS date),
       s._source_topic,
       s._kafka_partition,
       s._kafka_offset,
       s._schema_version
FROM bronze_orders_source s
JOIN bronze_pending_files nf ON s.__source_file = nf.__source_file
"""

INSERT_STATE = f"""
INSERT INTO {STATE_TABLE}
SELECT s.__source_file, count(*), current_timestamp()
FROM bronze_orders_source s
JOIN bronze_pending_files nf ON s.__source_file = nf.__source_file
GROUP BY s.__source_file
"""


def count_rows(spark: SparkSession, table: str) -> int:
    return spark.sql(f"SELECT count(*) AS n FROM {table}").first()["n"]


def seed_state_from_source(spark: SparkSession) -> None:
    """Mark every currently visible Bronze file as already ingested.

    Only reached when the state table is empty but ``orders`` is not: that is a
    table populated by the old load-once behaviour, whose history we cannot
    reconstruct because earlier revisions never recorded which files they
    read. Assuming "everything present has been loaded" keeps existing rows and
    avoids inserting them a second time. Starting from genuinely empty state,
    the next run resumes incremental loading.
    """
    log.warning(
        "%s has rows but %s is empty; this table predates incremental loading. "
        "Marking every Bronze file currently present as ingested so the "
        "existing rows are not duplicated. New files load from the next run on.",
        TABLE,
        STATE_TABLE,
    )
    spark.sql(
        f"INSERT INTO {STATE_TABLE} "
        "SELECT __source_file, count(*), current_timestamp() "
        "FROM bronze_orders_source GROUP BY __source_file"
    )


def load(spark: SparkSession) -> int:
    """Create the tables if needed, then append any Bronze file not yet loaded."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    spark.sql(DDL)
    spark.sql(STATE_DDL)

    # recursiveFileLookup is required, not an optimisation. The Bronze zone is
    # partitioned as {yyyy}/{mm}/{dd}; left to its default partition inference
    # Spark tries to turn those numeric directory segments into partition
    # columns, ends up with an empty file list, and fails with
    # UNABLE_TO_INFER_SCHEMA on every parent directory (only leaf date
    # directories resolve). Turning inference off makes the tree a flat set of
    # files, which is what is wanted here: _event_date is already a real column
    # inside the Parquet files, so nothing is lost.
    source = spark.read.option("recursiveFileLookup", "true").parquet(SOURCE)
    if source.isEmpty():
        log.info("no Bronze Parquet under %s; nothing to do", SOURCE)
        return count_rows(spark, TABLE)

    source.createOrReplaceTempView("bronze_orders_source_raw")
    spark.sql(SOURCE_VIEW)

    existing_rows = count_rows(spark, TABLE)
    if existing_rows and count_rows(spark, STATE_TABLE) == 0:
        seed_state_from_source(spark)
        return existing_rows

    spark.sql(NEW_FILES_VIEW)
    pending = spark.sql("SELECT count(*) AS n FROM bronze_pending_files").first()["n"]
    if pending == 0:
        log.info("every Bronze file is already loaded; %s unchanged at %d row(s)", TABLE, existing_rows)
        return existing_rows

    spark.sql(INSERT_ROWS)
    spark.sql(INSERT_STATE)

    loaded = count_rows(spark, TABLE)
    log.info(
        "appended %d file(s); %s now holds %d row(s) (was %d)",
        pending,
        TABLE,
        loaded,
        existing_rows,
    )
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