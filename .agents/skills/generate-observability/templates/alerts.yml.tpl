# Alert configuration for {dataset_name} pipeline

version: "1.0"
name: "{dataset_name} Pipeline Alerts"
description: "Alert configuration for {dataset_name} pipeline observability"

alerts:
  # Pipeline failure alert
  - name: "pipeline_failure_alert"
    description: "Alert when pipeline fails for 3 consecutive runs"
    condition:
      metric: "error_count"
      operator: ">="
      threshold: 3
      window: "5m"
    severity: "critical"
    recipients:
      - "data-engineering-team@company.com"
    
  # Data freshness alert
  - name: "data_freshness_alert"
    description: "Alert when data is older than SLA"
    condition:
      metric: "data_freshness_hours"
      operator: ">"
      threshold: 24
      window: "1h"
    severity: "warning"
    recipients:
      - "data-engineering-team@company.com"
    
  # High error rate alert
  - name: "high_error_rate_alert"
    description: "Alert when error rate exceeds threshold"
    condition:
      metric: "error_count"
      operator: ">="
      threshold: 10
      window: "15m"
    severity: "warning"
    recipients:
      - "data-engineering-team@company.com"
    
  # Dead letter alert
  - name: "dead_letter_alert"
    description: "Alert when dead letter queue has records"
    condition:
      metric: "dead_letter_count"
      operator: ">"
      threshold: 0
      window: "5m"
    severity: "warning"
    recipients:
      - "data-engineering-team@company.com"

# Alert routing configuration
routing:
  # Route alerts by domain/team
  data_engineering:
    teams:
      - "data-engineering-team@company.com"
    severity: "warning"
    
  # Route alerts by dataset
  {dataset_name}:
    teams:
      - "data-engineering-team@company.com"
    severity: "critical"