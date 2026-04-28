"""
Module for reading NYC Taxi data.
All functions for loading data are here
"""
from pathlib import Path
import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame

from src.schema import YELLOW_TAXI_SCHEMA


logger = logging.getLogger(__name__)


def load_yellow_taxi_data(spark: SparkSession, path: str, apply_schema: bool = True) -> DataFrame:
    """
    Loads NYC Yellow Taxi data from Parquet file or folder
    
    Args:
        spark: Active Spark session
        path: path to he parquet file or folder
        apply_schema: If True - gives explicit schema (recommended).
                      If False - Spark will infer the schema (its slow and risky)
    
    Returns:
        Spark DataFrame with the data
    
    Raises:
        FileNotFoundError: if path does not exist
    """
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Data path not found: {path}")
    
    logger.info(f"Loading data from: {path}")
    
    if apply_schema:
        df = spark.read.schema(YELLOW_TAXI_SCHEMA).parquet(path)
        logger.info("Loaded with explicit schema")
    else:
        df = spark.read.parquet(path)
        logger.info("Loaded with inferred schema")
    
    return df


def load_all_raw_data(spark: SparkSession, raw_dir: str = "data/raw", pattern: str = "yellow_tripdata_*.parquet") -> DataFrame:
    """
    Loads all parquet files from the raw folder
    Spark can rnead a lot files at once as they are combined in one DF
    
    Args:
        spark: Active Spark session
        raw_dir: path to folder with raw data
        pattern: Glob pattern for files
    
    Returns:
        joined DataFrame from all files
    """
    raw_path = Path(raw_dir)
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw data directory not found: {raw_dir}")
    
    # Finds all files which correspond of the pattern
    files = sorted(raw_path.glob(pattern))
    
    if not files:
        raise FileNotFoundError(f"No files matching '{pattern}' found in {raw_dir}")
    
    logger.info(f"Found {len(files)} file(s) to load:")
    for f in files:
        size_mb = f.stat().st_size / (1024 * 1024)
        logger.info(f"  - {f.name} ({size_mb:.1f} MB)")
    
    # Spark can read the glob pattern directly
    full_pattern = str(raw_path / pattern)
    df = spark.read.schema(YELLOW_TAXI_SCHEMA).parquet(full_pattern)
    
    return df


def get_data_summary(df: DataFrame) -> dict:
    """
    Returns summary for DF
    Good for check after load
    
    Args:
        df: DataFrame For analysis
    
    Returns:
        Dict with summary: row_count, column_count, partition_count
    """
    return {
        "row_count": df.count(),
        "column_count": len(df.columns),
        "partition_count": df.rdd.getNumPartitions(),
        "columns": df.columns,
    }


def print_data_summary(df: DataFrame, title: str = "Data Summary") -> None:
    """prints summary in readable way"""
    summary = get_data_summary(df)
    print(f"\n{'='*60}")
    print(f"{title}")
    print(f"{'='*60}")
    print(f"  Rows:       {summary['row_count']:,}")
    print(f"  Columns:    {summary['column_count']}")
    print(f"  Partitions: {summary['partition_count']}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    # Test: Load data and show summary
    from src.spark_session import get_spark_session, stop_spark_session
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    spark = get_spark_session("DataLoaderTest")
    
    try:
        df = load_all_raw_data(spark)
        print_data_summary(df, "Raw Yellow Taxi Data")
        
        print("Schema:")
        df.printSchema()
        
        print("\nFirst 3 rows:")
        df.show(3, truncate=False)
        
    finally:
        stop_spark_session(spark)


"""
1. Type hints (spark: SparkSession, -> DataFrame) – Tells to other devs what types the function gives. Python does not forces them, but helps a lot
Docstrings – Always use docstrings ! 
logging instead of print – Best way to show what the programs does. It can be turned on and off, write files (DEBUG, INFO, WARNING, ERROR)
try/finally – Guarantees that spark will stop where there is error
spark.read.schema(...).parquet(...) – chained methods, pyspark Style
""" 