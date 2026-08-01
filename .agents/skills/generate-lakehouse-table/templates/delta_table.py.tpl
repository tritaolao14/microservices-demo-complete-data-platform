"""Delta table creation script for {dataset_name}."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging


def create_delta_table(spark, table_name, location, schema):
    """Create Delta table with proper configuration."""
    
    # Create the table with Delta format
    df = spark.createDataFrame([], schema)
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", location) \
        .saveAsTable(table_name)
    
    # Configure table properties
    spark.sql(f"""
        ALTER TABLE {table_name} 
        SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.enableChangeDataFeed' = 'true'
        )
    """)
    
    logging.info(f"Created Delta table {table_name} at {location}")


def setup_partitioning(spark, table_name, partition_column):
    """Setup partitioning for the table."""
    
    # Add partitioning configuration
    spark.sql(f"""
        ALTER TABLE {table_name} 
        SET TBLPROPERTIES (
            'delta.partitionColumns' = '{partition_column}'
        )
    """)
    
    logging.info(f"Configured partitioning on {table_name} by {partition_column}")


def setup_retention(spark, table_name, retention_days):
    """Setup data retention policy."""
    
    # Configure retention (this is a simplified example)
    spark.sql(f"""
        ALTER TABLE {table_name} 
        SET TBLPROPERTIES (
            'delta.retentionPeriod' = '{retention_days} days'
        )
    """)
    
    logging.info(f"Configured {retention_days}-day retention for {table_name}")


def main():
    """Main function to create Delta table."""
    spark = SparkSession.builder \
        .appName("Create {dataset_name} Delta Table") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Table configuration
        table_name = "lakehouse.{dataset_name}"
        location = f"s3a://your-bucket/lakehouse/{dataset_name}"
        partition_column = "date"
        retention_days = 30
        
        # Define schema (customize based on your data)
        schema = [
            ("id", "string"),
            ("name", "string"),
            ("created_at", "timestamp"),
            ("date", "date")
        ]
        
        # Create table
        create_delta_table(spark, table_name, location, schema)
        
        # Setup partitioning
        setup_partitioning(spark, table_name, partition_column)
        
        # Setup retention
        setup_retention(spark, table_name, retention_days)
        
        logging.info("Delta table creation completed successfully")
        
    except Exception as e:
        logging.error(f"Error creating Delta table: {str(e)}")
        raise
    
    spark.stop()


if __name__ == "__main__":
    main()