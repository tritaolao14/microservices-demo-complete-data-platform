"""Spark job for {dataset_name} processing."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging


def create_spark_session():
    """Create Spark session with proper configuration."""
    spark = SparkSession.builder \
        .appName("{dataset_name}_processing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    return spark


def process_{dataset_name}_data(spark, input_path, output_path):
    """Process {dataset_name} data."""
    
    # Read input data
    df = spark.read.format("delta").load(input_path)
    
    # Process data (example transformation)
    processed_df = df.withColumn("processed_at", current_timestamp())
    
    # Write to output (Delta table)
    processed_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(output_path)
    
    logging.info(f"Processed {dataset_name} data successfully")


def main():
    """Main function to run the Spark job."""
    spark = create_spark_session()
    
    # Configuration
    input_path = "s3a://your-bucket/raw/{dataset_name}"
    output_path = "s3a://your-bucket/processed/{dataset_name}"
    
    try:
        process_{dataset_name}_data(spark, input_path, output_path)
        logging.info("Spark job completed successfully")
    except Exception as e:
        logging.error(f"Error in Spark job: {str(e)}")
        raise
    
    spark.stop()


if __name__ == "__main__":
    main()