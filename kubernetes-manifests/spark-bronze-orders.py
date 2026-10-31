"""Load the Bronze ``orders`` Parquet zone into the Iceberg ``bronze.orders`` table.

DPFMD-35 created the table and loaded it once, only while it was empty; DPFMD-36
needed that load to keep running because the Bronze writer keeps landing new
Parquet files under ``s3a://lakehouse/bronze/orders/{yyyy}/{mm}/{dd}``.

Two modes, selected with ``BRONZE_SYNC_MODE``:

- ``incremental`` (default): sync only the Parquet files that have not been
  recorded in ``bronze.ingest_state`` yet, then record them. This is what
  ``CronJob/spark-bronze-orders`` runs every hour. It also bootstraps: the
  merge inserts whatever is not in the table yet, so a fresh cluster catches up
  on the whole landing zone.
- ``bootstrap``: the original one-shot behaviour, kept as a manual escape hatch
  (load everything while the table is empty). Not scheduled anywhere.

The catalog is the JDBC one configured by the ``spark-iceberg-config``
ConfigMap:

    catalog   my_catalog (type=jdbc, Postgres product_catalog)
    table     my_catalog.bronze.orders
    state     my_catalog.bronze.ingest_state
    source    s3a://lakehouse/bronze/orders (Parquet + per-day _manifest.json)

Idempotency, in layers:

1. A file is processed only once: its manifest path is recorded in
   ``ingest_state`` after the rows are committed.
2. Rows carry ``_row_id``, a hash of the raw payload plus its Bronze metadata.
   The incremental merge is keyed on it, so a run that dies between the commit
   and the state write does not duplicate rows on the next attempt.
3. ``_row_id`` is backfilled for rows written before this change, so the first
   incremental run can match them instead of inserting the whole table again.

Bucket(16, order_id) is the ticket's "Bucket 16 on user_id" translated to this
dataset: orders have no user id, so the natural high-cardinality key,
``order_id``, is extracted from ``raw`` for bucketing. As with the rest of this
job, the extraction is done in Spark rather than by changing the writer, which
keeps the landing zone as the writer produced it.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("bronze.orders")

CATALOG = "my_catalog"
NAMESPACE = "bronze"
TABLE = f"{CATALOG}.{NAMESPACE}.orders"
STATE_TABLE = f"{CATALOG}.{NAMESPACE}.ingest_state"
SOURCE = "s3a://lakehouse/bronze/orders"
BUCKET_URI = "s3a://lakehouse/"

MODE = os.getenv("BRONZE_SYNC_MODE", "incremental").lower()
# Bound the work per run so one cycle cannot hold a table-level commit for an
# unbounded time. The backlog is drained oldest-first over several runs.
MAX_FILES = int(os.getenv("BRONZE_SYNC_MAX_FILES", "2000"))
BUCKET_COUNT = 16

# ``_row_id`` is the hash of the source event itself. It deliberately excludes
# _ingested_at, topic, partition and offset: the landing zone stores
# _ingested_at as an ISO-8601 string while Iceberg stores it as a timestamp, so
# hashing that column would produce two different ids for the same row (one
# from Parquet, one from the table) and the first incremental run would insert
# every pre-existing row again. The Kafka coordinates are excluded on purpose
# too: an at-least-once replay arrives at a new offset but is still the same
# event and must be deduplicated, not duplicated. ``raw`` is unique per event
# because it carries the checkout-generated ``order_id``.
ROW_ID_EXPR = "sha2(raw, 256)"

COLUMNS = (
    "raw, order_id, _ingested_at, _event_date, _source_topic, "
    "_kafka_partition, _kafka_offset, _schema_version, _row_id"
)

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    raw              string,
    order_id         string,
    _ingested_at     timestamp,
    _event_date      date,
    _source_topic    string,
    _kafka_partition int,
    _kafka_offset    bigint,
    _schema_version  string,
    _row_id          string
)
USING iceberg
PARTITIONED BY (days(_event_date), bucket({BUCKET_COUNT}, order_id))
"""

STATE_DDL = f"""
CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
    source_path      string,
    rows             bigint,
    file_created_at  timestamp,
    synced_at        timestamp
)
USING iceberg
"""


def _ignore_exists(action, description: str) -> None:
    """Run a metadata ALTER, treating "already there" as success.

    Spark has no ``IF NOT EXISTS`` for ADD COLUMN / ADD PARTITION FIELD on
    Iceberg tables, so an existing table created by an older version of this
    job needs the error swallowed. Anything else is re-raised.
    """
    try:
        action()
    except Exception as exc:
        message = str(exc)
        if any(
            marker in message.lower()
            for marker in ("already", "exists", "duplicate", "redundant")
        ):
            log.info("%s already present; skipping", description)
            return
        raise


def ensure_table(spark: SparkSession) -> None:
    """Create the table if needed and bring an older one to the current schema."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    spark.sql(DDL)

    # DPFMD-36 additions. On a fresh cluster DDL already contains them, so the
    # ALTERs are no-ops; on the cluster that DPFMD-35 bootstrapped they migrate
    # the existing table in place (metadata-only; no data rewrite).
    _ignore_exists(
        lambda: spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN order_id string"),
        "column order_id",
    )
    _ignore_exists(
        lambda: spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN _row_id string"),
        "column _row_id",
    )
    _ignore_exists(
        lambda: spark.sql(
            f"ALTER TABLE {TABLE} ADD PARTITION FIELD bucket({BUCKET_COUNT}, order_id)"
        ),
        f"partition field bucket({BUCKET_COUNT}, order_id)",
    )


def backfill_generated_columns(spark: SparkSession) -> None:
    """Fill ``order_id`` and ``_row_id`` for rows written before this change.

    Without this the first incremental merge cannot match the rows loaded by the
    old bootstrap job (NULL never equals NULL), so every file would be inserted
    a second time.
    """
    missing = spark.sql(
        f"SELECT count(*) AS n FROM {TABLE} WHERE _row_id IS NULL"
    ).first()["n"]
    if missing == 0:
        return
    log.info("backfilling order_id and _row_id for %d row(s)", missing)
    spark.sql(
        f"""
        UPDATE {TABLE}
        SET order_id = get_json_object(raw, '$.order_id'),
            _row_id = {ROW_ID_EXPR}
        WHERE _row_id IS NULL
        """
    )


def _read_manifest(jvm, fs, path):
    """Return the file entries listed by one per-partition manifest."""
    stream = fs.open(path)
    try:
        text = jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
    finally:
        stream.close()
    document = json.loads(text)
    entries = []
    for item in document.get("files", []):
        created_at = item.get("created_at")
        entries.append(
            {
                "path": item["path"],
                "rows": int(item.get("rows", 0)),
                "created_at": datetime.fromisoformat(created_at)
                if created_at
                else None,
            }
        )
    return entries


def list_manifest_files(spark: SparkSession):
    """List every committed Bronze file through the manifests, not the files.

    The writer publishes ``_manifest.json`` per date partition as the reader
    contract (see ``BronzeLayout``), so listing manifests avoids walking every
    Parquet object in the bucket.
    """
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    root = jvm.org.apache.hadoop.fs.Path(SOURCE)
    fs = root.getFileSystem(hadoop_conf)

    entries = []
    iterator = fs.listFiles(root, True)
    while iterator.hasNext():
        status = iterator.next()
        path = status.getPath()
        if path.getName() == "_manifest.json":
            entries.extend(_read_manifest(jvm, fs, path))
    log.info("manifest scan found %d Bronze file(s)", len(entries))
    return entries


def _rows_frame(source):
    """Project the landing-zone schema into the Iceberg row shape."""
    return source.select(
        F.col("raw"),
        F.get_json_object("raw", "$.order_id").alias("order_id"),
        F.col("_ingested_at").cast("timestamp").alias("_ingested_at"),
        F.col("_event_date").cast("date").alias("_event_date"),
        F.col("_source_topic"),
        F.col("_kafka_partition").cast("int").alias("_kafka_partition"),
        F.col("_kafka_offset").cast("bigint").alias("_kafka_offset"),
        F.col("_schema_version"),
        F.expr(ROW_ID_EXPR).alias("_row_id"),
    )


def load_full(spark: SparkSession) -> int:
    """Manual bootstrap: load the landing zone while the table is empty."""
    ensure_table(spark)
    backfill_generated_columns(spark)

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

    _rows_frame(source).createOrReplaceTempView("bronze_orders_source")
    spark.sql(
        f"INSERT INTO {TABLE} ({COLUMNS}) SELECT {COLUMNS} FROM bronze_orders_source"
    )
    loaded = spark.sql(f"SELECT count(*) AS n FROM {TABLE}").first()["n"]
    log.info("loaded %d row(s) into %s (source had %d)", loaded, TABLE, count)
    return loaded


def sync_incremental(spark: SparkSession) -> int:
    """Append files not yet recorded in ``ingest_state``.

    Ordered oldest-first and bounded by ``BRONZE_SYNC_MAX_FILES`` so a large
    backlog is drained across several CronJob runs instead of one long commit.
    """
    ensure_table(spark)
    backfill_generated_columns(spark)
    spark.sql(STATE_DDL)

    known = {
        row["source_path"]
        for row in spark.sql(f"SELECT source_path FROM {STATE_TABLE}").collect()
    }
    pending = sorted(
        (entry for entry in list_manifest_files(spark) if entry["path"] not in known),
        key=lambda entry: entry["path"],
    )
    if not pending:
        log.info("no new Bronze Parquet files; nothing to sync")
        return 0
    if len(pending) > MAX_FILES:
        log.info("%d file(s) pending; syncing the oldest %d", len(pending), MAX_FILES)
        pending = pending[:MAX_FILES]

    uris = [BUCKET_URI + entry["path"] for entry in pending]
    log.info("syncing %d Bronze file(s) into %s", len(uris), TABLE)
    source = _rows_frame(spark.read.parquet(*uris))
    # A single file cannot contain the same _row_id twice, but the same event
    # can appear in two files after an at-least-once replay. Keep one row per
    # key so MERGE does not raise on duplicate source keys.
    source.createOrReplaceTempView("bronze_sync_source")
    spark.sql(
        f"""
        MERGE INTO {TABLE} AS target
        USING (
            SELECT {COLUMNS}
            FROM (
                SELECT *, row_number() OVER (PARTITION BY _row_id ORDER BY _row_id) AS _dedupe
                FROM bronze_sync_source
            )
            WHERE _dedupe = 1
        ) AS source
        ON target._row_id = source._row_id
        WHEN NOT MATCHED THEN
            INSERT ({COLUMNS}) VALUES (
                source.raw, source.order_id, source._ingested_at, source._event_date,
                source._source_topic, source._kafka_partition, source._kafka_offset,
                source._schema_version, source._row_id
            )
        """
    )

    # Only now is it safe to remember the files: the merge has committed the
    # rows. A crash before this insert re-reads them next run and the merge
    # matches on _row_id, so it is a no-op rather than a duplicate.
    synced_at = datetime.now(timezone.utc)
    state_rows = [
        (entry["path"], entry["rows"], entry["created_at"], synced_at)
        for entry in pending
    ]
    spark.createDataFrame(
        state_rows,
        "source_path string, rows bigint, file_created_at timestamp, synced_at timestamp",
    ).createOrReplaceTempView("bronze_sync_state")
    spark.sql(f"INSERT INTO {STATE_TABLE} SELECT * FROM bronze_sync_state")
    log.info("recorded %d file(s) in %s", len(state_rows), STATE_TABLE)
    return len(state_rows)


def main() -> int:
    spark = SparkSession.builder.appName("bronze-orders-iceberg-load").getOrCreate()
    try:
        if MODE == "incremental":
            sync_incremental(spark)
        elif MODE == "bootstrap":
            load_full(spark)
        else:
            log.error(
                "unknown BRONZE_SYNC_MODE=%s (expected bootstrap or incremental)", MODE
            )
            return 2
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
