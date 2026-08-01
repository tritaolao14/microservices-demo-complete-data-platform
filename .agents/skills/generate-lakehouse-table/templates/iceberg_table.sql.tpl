-- Iceberg table creation for {dataset_name}

-- Create Iceberg table with proper configuration
CREATE TABLE IF NOT EXISTS lakehouse.{dataset_name} (
    id STRING,
    name STRING,
    created_at TIMESTAMP,
    date DATE
)
USING iceberg
PARTITIONED BY (date)
LOCATION 's3a://your-bucket/lakehouse/{dataset_name}'
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.partition.overwrite' = 'true',
    'write.compaction.strategy' = 'major',
    'write.compaction.enabled' = 'true'
);

-- Configure retention policy (example)
ALTER TABLE lakehouse.{dataset_name}
SET TBLPROPERTIES (
    'retention.days' = 30
);

-- Configure partitioning
ALTER TABLE lakehouse.{dataset_name}
SET TBLPROPERTIES (
    'partitioning' = 'date'
);

-- Example query to verify table creation
SELECT * FROM lakehouse.{dataset_name} LIMIT 10;