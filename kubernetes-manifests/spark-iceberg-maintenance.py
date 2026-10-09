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

Not covered by this script: data retention by age (the "30d/90d" part).
Iceberg has no automatic age-based data retention -- dropping old partitions
is an explicit rewrite that must never run before Bronze has been synced into
Iceberg, which today happens only once at bootstrap (see spark-bronze-orders.py).
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

# Snapshot retention. 7 days mirrors the ticket's "VACUUM 7d"; Iceberg's own
# default is 5 days, so the value is set explicitly rather than relied upon.
SNAPSHOT_MAX_AGE_DAYS = 7
# Iceberg's default is 1. Keeping 24 costs nothing on a table this small and
# protects against a schedule that fires more often than the age cutoff.
SNAPSHOTS_TO_KEEP = 24

# Grace period for orphan cleanup. MUST be >= SNAPSHOT_MAX_AGE_DAYS: an orphan
# file younger than the snapshot window may still be referenced by a live
# snapshot, and delete_orphan_files only compares against current metadata.
# A value below the snapshot window risks deleting files that are in use --
# silent data loss with no error.
ORPHAN_GRACE_DAYS = 3

# Compaction. target size matches BRONZE_TARGET_FILE_BYTES in the DPFMD-34
# writer so compacted output lands in the same size band the writer produces.
COMPACTION_TARGET_BYTES = 134_217_728  # 128 MiB
COMPACTION_MIN_INPUT_FILES = 2

TABLE_PROPERTIES = {
    "history.expire.max-snapshot-age-ms": str(SNAPSHOT_MAX_AGE_DAYS * 24 * 60 * 60 * 1000),
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
    log.info("expired snapshots older than %s (retain_last=%d)", older_than, SNAPSHOTS_TO_KEEP)


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
    spark = SparkSession.builder.appName("iceberg-bronze-orders-maintenance").getOrCreate()
    try:
        if not table_exists(spark):
            return 0
        apply_table_properties(spark)
        expire_snapshots(spark)
        compact_data_files(spark)
        remove_orphan_files(spark)
        log.info("maintenance finished for %s", TABLE)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
