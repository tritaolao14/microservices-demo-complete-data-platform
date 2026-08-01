# Hudi table configuration for {dataset_name}

version: "1.0"
name: "{dataset_name} Hudi Table Configuration"
description: "Configuration for Hudi table in lakehouse"

table:
  name: "lakehouse.{dataset_name}"
  location: "s3a://your-bucket/lakehouse/{dataset_name}"
  format: "hudi"
  
partitioning:
  enabled: true
  column: "date"
  type: "daily"
  
storage:
  file_format: "parquet"
  compression: "snappy"
  
compaction:
  enabled: true
  strategy: "major"
  interval_days: 7
  
retention:
  enabled: true
  days: 30
  
write:
  operation: "upsert"
  payload_class: "org.apache.hudi.payload.AvroPayload"
  
read:
  consistency_check: true
  snapshot_query: true
  
properties:
  # Hudi specific properties
  hoodie.datasource.write.recordkey.field: "id"
  hoodie.datasource.write.partitionpath.field: "date"
  hoodie.datasource.write.table.name: "{dataset_name}"
  hoodie.datasource.write.keygenerator.class: "org.apache.hudi.keygen.SimpleKeyGenerator"