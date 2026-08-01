# Data Quality Rules for {dataset_name}

version: "1.0"
name: "{dataset_name} Data Quality Rules"
description: "Data quality rules for {dataset_name} dataset"

rules:
  # Schema validation rules
  - name: "schema_validation"
    description: "Validate that all fields match expected schema"
    type: "schema"
    severity: "error"
    threshold: 0.95
    # Example:
    # - field: "id"
    #   type: "string"
    #   nullable: false
    # - field: "name"
    #   type: "string"
    #   nullable: true

  # Completeness rules
  - name: "completeness_check"
    description: "Ensure all required fields are present"
    type: "completeness"
    severity: "error"
    threshold: 0.99
    # Example:
    # required_fields:
    #   - "id"
    #   - "created_at"

  # Uniqueness rules
  - name: "uniqueness_check"
    description: "Ensure primary key is unique"
    type: "uniqueness"
    severity: "error"
    threshold: 1.0
    # Example:
    # key_field: "id"

  # Consistency rules
  - name: "consistency_check"
    description: "Ensure data consistency across related fields"
    type: "consistency"
    severity: "warning"
    threshold: 0.95
    # Example:
    # - field1: "status"
    #   field2: "updated_at"
    #   condition: "when status is 'active', updated_at should be recent"

  # Timeliness rules
  - name: "timeliness_check"
    description: "Ensure data freshness meets SLA requirements"
    type: "timeliness"
    severity: "warning"
    threshold: 0.95
    # Example:
    # max_age_hours: 24

  # Validity rules
  - name: "validity_check"
    description: "Ensure all values are within expected ranges"
    type: "validity"
    severity: "error"
    threshold: 0.98
    # Example:
    # - field: "age"
    #   min_value: 0
    #   max_value: 150