import os
import logging
import sys
from pyspark.sql import SparkSession

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def create_sample_datasets():
    """
    Generates synthetic healthcare data in various formats (Parquet, ORC) 
    for testing the ingestion pipeline.
    """
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("GenerateSampleData") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # Resolve paths relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    output_dir = os.path.join(project_root, "data")

    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Generating sample data in: {output_dir}")

    # --- Dataset 1: Provider C (Parquet) ---
    logger.info("Generating Provider C (Parquet)...")
    data_p = [
        ("PARQ-001", "Steve", "Jobs", "1955-02-24", "steve@apple.com", "555-666-7777"),
        ("PARQ-002", "Tim", "Cook", "1960-11-01", "tim@apple.com", "555-999-8888")
    ]
    schema_p = ["memberID", "givenName", "familyName", "birthDate", "emailAddress", "phoneNumber"]
    
    df_p = spark.createDataFrame(data_p, schema_p)
    parquet_path = os.path.join(output_dir, "provider_c.parquet")
    df_p.write.mode("overwrite").parquet(parquet_path)

    # --- Dataset 2: Provider D (ORC) ---
    logger.info("Generating Provider D (ORC)...")
    data_o = [
        ("ORC-001", "Satya", "Nadella", "1967-08-19", "satya@ms.com", "555-000-1111")
    ]
    schema_o = ["m_id", "first", "last", "dob", "email_addr", "contact"]

    df_o = spark.createDataFrame(data_o, schema_o)
    orc_path = os.path.join(output_dir, "provider_d.orc")
    df_o.write.mode("overwrite").orc(orc_path)

    logger.info("Sample data creation complete.")
    spark.stop()

if __name__ == "__main__":
    create_sample_datasets()