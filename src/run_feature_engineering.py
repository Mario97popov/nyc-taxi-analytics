"""
Main Script for feature engineering.

Reads clean data from data/processed/yellow_taxi_clean
Saves enriched data in data/processed/yellow_taxi_features

How to use:
    python -m src.run_feature_engineering
"""
import logging
import sys

from src.spark_session import get_spark_session, stop_spark_session, load_config
from src.loader import load_yellow_taxi_data
from src.transformations import add_all_features


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/feature_engineering.log"),
        ]
    )


def main() -> int:
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting feature engineering pipeline")

    try:
        config = load_config()
    except FileNotFoundError as e:
        logger.error(f"Config not found: {e}")
        return 1

    spark = get_spark_session("FeatureEngineering")

    try:
        # Loads the clean data
        clean_path = config["paths"]["processed_data"] + "/yellow_taxi_clean"
        df = load_yellow_taxi_data(spark, clean_path, apply_schema=False)

        initial_count = df.count()
        initial_columns = len(df.columns)
        logger.info(f"Loaded {initial_count:,} rows, {initial_columns} columns")

        # Does feature engineering
        enriched_df = add_all_features(df)

        # Save by partitioning on pickup date.
        output_path = config["paths"]["processed_data"] + "/yellow_taxi_features"
        logger.info(f"Writing to: {output_path}")

        (enriched_df
            .write
            .mode("overwrite")
            .partitionBy("pickup_day")
            .parquet(output_path)
        )

        # Verification
        final_df = spark.read.parquet(output_path)
        final_count = final_df.count()
        final_columns = len(final_df.columns)

        logger.info("=" * 60)
        logger.info("Feature engineering complete:")
        logger.info(f"  Rows:    {initial_count:,} -> {final_count:,}")
        logger.info(f"  Columns: {initial_columns} -> {final_columns}")
        logger.info(f"  New features: {final_columns - initial_columns}")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.exception(f"Feature engineering failed: {e}")
        return 1

    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    sys.exit(main())