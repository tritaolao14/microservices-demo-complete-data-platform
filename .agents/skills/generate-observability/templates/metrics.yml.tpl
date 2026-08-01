# Observability metrics for {dataset_name} pipeline

version: "1.0"
name: "{dataset_name} Pipeline Metrics"
description: "Metrics configuration for {dataset_name} pipeline observability"

metrics:
  # Record count metrics
  - name: "record_count_extract"
    description: "Number of records extracted from source"
    type: "counter"
    labels:
      - pipeline_id
      - stage
      - dataset_name
    
  - name: "record_count_transform"
    description: "Number of records transformed"
    type: "counter"
    labels:
      - pipeline_id
      - stage
      - dataset_name
    
  - name: "record_count_load"
    description: "Number of records loaded to destination"
    type: "counter"
    labels:
      - pipeline_id
      - stage
      - dataset_name

  # Duration metrics
  - name: "duration_extract"
    description: "Duration of extract stage in seconds"
    type: "gauge"
    labels:
      - pipeline_id
      - stage
      - dataset_name
    
  - name: "duration_transform"
    description: "Duration of transform stage in seconds"
    type: "gauge"
    labels:
      - pipeline_id
      - stage
      - dataset_name
    
  - name: "duration_load"
    description: "Duration of load stage in seconds"
    type: "gauge"
    labels:
      - pipeline_id
      - stage
      - dataset_name

  # Error rate metrics
  - name: "error_count"
    description: "Number of errors in pipeline"
    type: "counter"
    labels:
      - pipeline_id
      - stage
      - dataset_name
      - error_type
    
  # Freshness metrics
  - name: "data_freshness_hours"
    description: "Hours since latest data timestamp"
    type: "gauge"
    labels:
      - pipeline_id
      - dataset_name

  # Dead letter metrics
  - name: "dead_letter_count"
    description: "Number of records in dead letter queue"
    type: "counter"
    labels:
      - pipeline_id
      - dataset_name