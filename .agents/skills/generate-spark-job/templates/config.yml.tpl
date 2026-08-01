# Spark job configuration for {dataset_name}

job:
  name: "{dataset_name}_processing"
  description: "Spark job for processing {dataset_name} data"
  
spark:
  app_name: "{dataset_name}_processing"
  master: "local[*]"
  config:
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.execution.arrow.pyspark.enabled: "true"
    spark.sql.shuffle.partitions: "200"
    
storage:
  input_path: "s3a://your-bucket/raw/{dataset_name}"
  output_path: "s3a://your-bucket/processed/{dataset_name}"
  checkpoint_path: "s3a://your-bucket/checkpoint/{dataset_name}"
  
partitioning:
  enabled: true
  column: "date"
  format: "yyyy-MM-dd"
  
retention:
  enabled: true
  days: 30
  
compaction:
  enabled: true
  interval_days: 7