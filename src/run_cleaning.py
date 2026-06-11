"""
Main script for cleaning

How to start it:
    python -m src.run_cleaning
"""
import logging
import sys

from src.spark_session import get_spark_session, stop_spark_session, load_config
from src.loader import load_all_raw_data
from src.cleaner import clean_taxi_data, save_cleaned_data


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/cleaning.log"),
        ]
    )


def main() -> int:
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting cleaning pipeline")

    try:
        config = load_config()
    except FileNotFoundError as e:
        logger.error(f"Config not found: {e}")
        return 1

    spark = get_spark_session("DataCleaning")

    try:
        # loading the raw data
        df = load_all_raw_data(spark)

        # cleaning
        clean_df = clean_taxi_data(df, config)

        # Save
        output_path = config["paths"]["processed_data"] + "/yellow_taxi_clean"
        save_cleaned_data(clean_df, output_path)

        logger.info(f"Cleaned data saved to: {output_path}")
        return 0

    except Exception as e:
        logger.exception(f"Cleaning failed: {e}")
        return 1

    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    sys.exit(main())