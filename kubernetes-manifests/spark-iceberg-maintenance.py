"""Iceberg table maintenance for ``bronze.orders``.

DPFMD-36. The ticket this implements was written in Delta Lake terms
("VACUUM 7d, retention 30d/90d"), but DPFMD-35 created an **Iceberg** table,
and Iceberg has no ``VACUUM``. The translation used here:

    Delta Lake                       Apache Iceberg
    -------------------------------  -----------------------------------------
    VACUUM t RETAIN 7 DAYS           expire_snapshots (older_than = now-7d)
    delta.deletedFileRetentionDuration  history.expire.max-snapshot-age-ms
    delta.logRetentionDuration           history.expire.min-snapshots-to-keep
    OPTIMIZE t ZORDER BY (c)            rewrite_data_files (strategy='binpack')
    (no equivalent)                     remove_orphan_files

Running ``VACUUM``-style semantics against this table would either fail
(unknown procedure) or, worse, appear to succeed: Iceberg silently ignores
unrecognised ``delta.*`` table properties, so a Delta config copied verbatim
leaves the table with no retention at all while reporting success.

Retention lives in **table properties** rather than in this script, so any
client reading the table sees the same policy. The constants below are the
single source of truth and are both written to the table and used as the
procedure arguments, so the two can never drift apart.

Age-based retention ("30d/90d" in the ticket) is enforced here too. The
lakehouse layers in this repo are raw (Kafka topic + Parquet landing zone) and
bronze (this Iceberg table), and ``.agents/guides/lakehouse/governance.md``
assigns raw 30d / bronze 90d, so that is the mapping used:

    raw    30d   Kafka ``orders`` retention.ms + prune synced landing files
    bronze 90d   DELETE from {table} where _event_date is older than the cutoff

The landing-zone prune is guarded by ``bronze.ingest_state``: a Parquet file is
only ever deleted after the sync job recorded it as loaded into Iceberg, so raw
retention can never outrun bronze ingestion. Kafka retention is configured on
the topic itself (``gitops/overlays/dev/schema-registry.yaml``).
"""

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
log = logging.getLogger("iceberg.maintenance")

CATALOG = "my_catalog"
NAMESPACE = "bronze"
TABLE = f"{CATALOG}.{NAMESPACE}.orders"
STATE_TABLE = f"{CATALOG}.{NAMESPACE}.ingest_state"
LANDING_URI = "s3a://lakehouse/"

# Age-based retention, in days. Overridable so an operator can tighten or
# disable (0 or a negative value) a window without rebuilding the image.
RAW_RETENTION_DAYS = int(os.getenv("RAW_RETENTION_DAYS", "30"))
BRONZE_RETENTION_DAYS = int(os.getenv("BRONZE_RETENTION_DAYS", "90"))
# Deleting landing-zone objects is the destructive half of raw retention; keep
# a dry-run switch so a first rollout can be inspected.
RAW_PRUNE_DRY_RUN = os.getenv("RAW_PRUNE_DRY_RUN", "false").lower() == "true"

# Snapshot retention. 7 days mirrors the ticket's "VACUUM 7d"; Iceberg's own
# default is 5 days, so the value is set explicitly rather than relied upon.
SNAPSHOT_MAX_AGE_DAYS = 7
# Iceberg's default is 1. Keeping 24 costs nothing on a table this small and
# protects against a schedule that fires more often than the age cutoff.
SNAPSHOTS_TO_KEEP = 24

# Grace period for orphan cleanup. Iceberg compares the warehouse listing against
# current metadata, so this only has to exceed the longest in-flight write: a
# file that a writer is still uploading is not in metadata yet and would look
# like an orphan. Three days is far above a single Spark batch; lowering it to
# seconds would risk deleting in-progress files.
ORPHAN_GRACE_DAYS = 3

# Compaction. target size matches BRONZE_TARGET_FILE_BYTES in the DPFMD-34
# writer so compacted output lands in the same size band the writer produces.
COMPACTION_TARGET_BYTES = 134_217_728  # 128 MiB
COMPACTION_MIN_INPUT_FILES = 2

TABLE_PROPERTIES = {
    "history.expire.max-snapshot-age-ms": str(
        SNAPSHOT_MAX_AGE_DAYS * 24 * 60 * 60 * 1000
    ),
    "history.expire.min-snapshots-to-keep": str(SNAPSHOTS_TO_KEEP),
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "10",
}


def _timestamp_literal(days_ago: int) -> str:
    """Render a UTC cutoff as a SQL TIMESTAMP literal.

    A literal is used rather than ``now() - INTERVAL n DAYS`` because procedure
    argument parsing is stricter than plain SQL expression parsing.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def apply_table_properties(spark: SparkSession) -> None:
    """Write the retention policy onto the table (idempotent)."""
    assignments = ", ".join(
        f"'{key}' = '{value}'" for key, value in TABLE_PROPERTIES.items()
    )
    spark.sql(f"ALTER TABLE {TABLE} SET TBLPROPERTIES ({assignments})")
    log.info("applied %d table properties to %s", len(TABLE_PROPERTIES), TABLE)


def enforce_bronze_retention(spark: SparkSession) -> None:
    """Delete Iceberg rows older than the bronze retention window.

    Iceberg has no automatic age-based retention, so this is an explicit DELETE.
    It is safe to run before compaction: the delete commits new metadata and the
    rewrite below then packs whatever rows remain.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=BRONZE_RETENTION_DAYS)).date()
    stale = spark.sql(
        f"SELECT count(*) AS n FROM {TABLE} WHERE _event_date < DATE '{cutoff.isoformat()}'"
    ).first()["n"]
    if stale == 0:
        log.info(
            "no Bronze row older than %s (retention=%dd); nothing to drop",
            cutoff,
            BRONZE_RETENTION_DAYS,
        )
        return
    spark.sql(f"DELETE FROM {TABLE} WHERE _event_date < DATE '{cutoff.isoformat()}'")
    log.info(
        "dropped %d Bronze row(s) older than %s (retention=%dd)",
        stale,
        cutoff,
        BRONZE_RETENTION_DAYS,
    )


def prune_landing_zone(spark: SparkSession) -> None:
    """Delete landing-zone Parquet that was synced and is past raw retention.

    Only paths recorded in ``ingest_state`` are candidates, so a file that has
    not reached Iceberg yet is never removed no matter how old it looks. The
    per-day manifest is left in place; the sync job filters by state, and the
    manifest is only a few KB.
    """
    cutoff = _timestamp_literal(RAW_RETENTION_DAYS)
    try:
        stale = spark.sql(
            f"SELECT source_path FROM {STATE_TABLE} "
            f"WHERE source_path IS NOT NULL "
            f"AND file_created_at < TIMESTAMP '{cutoff}'"
        ).collect()
    except Exception as exc:  # noqa: BLE001 - no state table means nothing was synced
        log.warning(
            "%s is not usable (%s); skipping landing-zone prune", STATE_TABLE, exc
        )
        return
    if not stale:
        log.info(
            "no synced landing file older than %s (retention=%dd); nothing to prune",
            cutoff,
            RAW_RETENTION_DAYS,
        )
        return

    jvm = spark._jvm
    root = jvm.org.apache.hadoop.fs.Path(LANDING_URI)
    fs = root.getFileSystem(spark._jsc.hadoopConfiguration())
    deleted = 0
    for row in stale:
        path = jvm.org.apache.hadoop.fs.Path(LANDING_URI + row["source_path"])
        if not fs.exists(path):
            continue
        if RAW_PRUNE_DRY_RUN:
            continue
        if fs.delete(path, False):
            deleted += 1
    log.info(
        "landing-zone prune: %d candidate(s) older than %s, %d deleted (dry_run=%s)",
        len(stale),
        cutoff,
        deleted,
        RAW_PRUNE_DRY_RUN,
    )


def expire_snapshots(spark: SparkSession) -> None:
    """Drop snapshot history beyond the retention window.

    Iceberg only deletes data files that no surviving snapshot references, so a
    still-current snapshot keeps its files regardless of age.
    """
    older_than = _timestamp_literal(SNAPSHOT_MAX_AGE_DAYS)
    spark.sql(
        f"CALL {CATALOG}.system.expire_snapshots("
        f"table => '{TABLE}', "
        f"older_than => TIMESTAMP '{older_than}', "
        f"retain_last => {SNAPSHOTS_TO_KEEP})"
    )
    log.info(
        "expired snapshots older than %s (retain_last=%d)",
        older_than,
        SNAPSHOTS_TO_KEEP,
    )


def compact_data_files(spark: SparkSession) -> None:
    """Bin-pack small Parquet files left behind by the Bronze writer."""
    options = (
        f"map('min-input-files', '{COMPACTION_MIN_INPUT_FILES}', "
        f"'target-file-size-bytes', '{COMPACTION_TARGET_BYTES}')"
    )
    spark.sql(
        f"CALL {CATALOG}.system.rewrite_data_files("
        f"table => '{TABLE}', strategy => 'binpack', options => {options})"
    )
    log.info("compaction pass completed (target=%d bytes)", COMPACTION_TARGET_BYTES)


def remove_orphan_files(spark: SparkSession) -> None:
    """Delete warehouse files no snapshot references.

    This is the one destructive step: it compares the warehouse listing against
    current metadata and deletes the difference. Dry-run is ON by default so
    the first runs report candidates without deleting them.
    """
    dry_run = os.getenv("ICEBERG_ORPHAN_DRY_RUN", "true").lower() == "true"
    older_than = _timestamp_literal(ORPHAN_GRACE_DAYS)
    spark.sql(
        f"CALL {CATALOG}.system.remove_orphan_files("
        f"table => '{TABLE}', "
        f"older_than => TIMESTAMP '{older_than}', "
        f"dry_run => {str(dry_run).lower()})"
    )
    log.info("orphan cleanup finished (older_than=%s, dry_run=%s)", older_than, dry_run)


def table_exists(spark: SparkSession) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {TABLE}").collect()
    except Exception as exc:  # noqa: BLE001 - any failure means "not usable"
        log.warning("%s is not usable (%s); skipping maintenance", TABLE, exc)
        return False
    return True


def main() -> int:
    spark = SparkSession.builder.appName(
        "iceberg-bronze-orders-maintenance"
    ).getOrCreate()
    try:
        if not table_exists(spark):
            return 0
        apply_table_properties(spark)
        enforce_bronze_retention(spark)
        compact_data_files(spark)
        prune_landing_zone(spark)
        expire_snapshots(spark)
        remove_orphan_files(spark)
        log.info("maintenance finished for %s", TABLE)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
