# Data contract for {dataset_name}

version: "1.0"
name: "{dataset_name}"
description: "Data contract for {dataset_name} dataset"

fields:
  # Define your fields here
  # Example:
  # id:
  #   type: string
  #   nullable: false
  #   description: "Unique identifier"
  # name:
  #   type: string
  #   nullable: true
  #   description: "User name"
  # created_at:
  #   type: timestamp
  #   nullable: false
  #   description: "Timestamp when record was created"

schema:
  type: object
  properties:
    # Define your schema properties here
    # Example:
    # id:
    #   type: string
    # name:
    #   type: string
    # created_at:
    #   type: string
    #   format: date-time

expectations:
  # Define data quality expectations here
  # Example:
  # - name: "id_not_null"
  #   description: "ID field should not be null"
  #   rule:
  #     field: id
  #     operator: "not_null"
  # - name: "email_format"
  #   description: "Email should be in valid format"
  #   rule:
  #     field: email
  #     operator: "regex"
  #     pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"