import json
import logging
import os
import sys
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, date_format, initcap, lit, lower, regexp_replace, to_date
)

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "HealthcareEligibilityPipeline") -> SparkSession:
    """
    Initializes and returns a Spark session.
    
    Args:
        app_name: Name of the Spark application.
        
    Returns:
        A configured SparkSession object.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    
    # Reduce noise in logs from Spark internal operations
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_config(config_path: str) -> Dict:
    """
    Loads the JSON configuration file for partner mappings.
    
    Args:
        config_path: Absolute path to the configuration file.
        
    Returns:
        Dictionary containing configuration data.
        
    Raises:
        SystemExit: If the configuration file is missing or invalid.
    """
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.critical(f"Configuration file not found at: {config_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.critical(f"Invalid JSON format in configuration file: {config_path}")
        sys.exit(1)


def standardize_phone(column: str) -> col:
    """
    Standardizes phone numbers to XXX-XXX-XXXX format using regex.
    
    Args:
        column: The DataFrame column name containing phone data.
        
    Returns:
        A Spark Column object with standardized formatting.
    """
    clean_phone = regexp_replace(column, "[^0-9]", "")
    return regexp_replace(clean_phone, r"(\d{3})(\d{3})(\d{4})", "$1-$2-$3")


def process_partner(spark: SparkSession, partner_name: str, config: Dict, file_path: str) -> Optional[DataFrame]:
    """
    Ingests and transforms data for a specific partner based on their configuration.
    
    Args:
        spark: Active SparkSession.
        partner_name: Unique identifier for the partner.
        config: Partner-specific transformation and ingestion rules.
        file_path: Path to the source data file.
        
    Returns:
        A transformed Spark DataFrame, or None if processing fails.
    """
    logger.info(f"Processing partner: {partner_name}")
    
    if not os.path.exists(file_path):
        logger.error(f"File not found for {partner_name}: {file_path}")
        return None

    # Ingestion strategy based on file type
    file_type = config.get("file_type", "delimited")
    
    try:
        if file_type == "delimited":
            raw_df = spark.read.format("csv") \
                .option("delimiter", config.get('delimiter', ',')) \
                .option("header", config.get('has_header', 'true')) \
                .option("inferSchema", "true") \
                .load(file_path)
        else:
            raw_df = spark.read.format(file_type).load(file_path)
    except Exception as e:
        logger.exception(f"Failed to read {file_type} file for {partner_name}")
        return None

    # Schema Standardization (Renaming & Filling Missing)
    select_expr = []
    for source_col, target_col in config['mappings'].items():
        if source_col in raw_df.columns:
            select_expr.append(col(source_col).alias(target_col))
        else:
            logger.warning(f"Column '{source_col}' missing for {partner_name}. Filling with NULL.")
            select_expr.append(lit(None).alias(target_col))
    
    stg_df = raw_df.select(*select_expr)

    # Core Transformations
    stg_df = stg_df \
        .withColumn("first_name", initcap(col("first_name"))) \
        .withColumn("last_name", initcap(col("last_name"))) \
        .withColumn("email", lower(col("email"))) \
        .withColumn("phone", standardize_phone("phone")) \
        .withColumn("dob", date_format(to_date(col("dob"), config['input_date_format']), "yyyy-MM-dd")) \
        .withColumn("partner_code", lit(partner_name))

    # Data Quality Filter: Remove records without a primary identifier
    valid_df = stg_df.filter(col("external_id").isNotNull())
    
    dropped_count = stg_df.count() - valid_df.count()
    if dropped_count > 0:
        logger.warning(f"Dropped {dropped_count} records for {partner_name} due to null external_id.")

    return valid_df


def main():
    """Main entry point for the healthcare data pipeline."""
    # Resolve project paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config", "partners.json")
    data_dir = os.path.join(base_dir, "data")

    # Initialize environment
    spark = get_spark_session()
    partners_config = load_config(config_path)

    unified_dfs: List[DataFrame] = []

    # Dynamically process each partner defined in the config
    for partner_name, config in partners_config.items():
        file_path = os.path.join(data_dir, config.get("file_name", ""))
        
        df = process_partner(spark, partner_name, config, file_path)
        if df:
            unified_dfs.append(df)
    
    # Consolidate and display results
    if unified_dfs:
        final_df = unified_dfs[0]
        for df in unified_dfs[1:]:
            final_df = final_df.unionByName(df)
        
        logger.info("Pipeline completed successfully. Unified Dataset:")
        final_df.show(truncate=False)
        
        # Note: In production, we would write to a curated layer (e.g., S3/Delta Lake)
        # final_df.write.mode("overwrite").partitionBy("partner_code").parquet(...)
    else:
        logger.error("No data was successfully processed.")

    spark.stop()


if __name__ == "__main__":
    main()