"""
Main Script to use quality check on raw data.

How to use:
    python -m src.run_quality_check
"""
import logging
import sys

from src.spark_session import get_spark_session, stop_spark_session, load_config
from src.loader import load_all_raw_data
from src.quality import run_all_checks
from src.reports.quality_report import print_quality_report, save_quality_report_json


def setup_logging():
    """Sets up logging - in console + file."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # Console
            logging.FileHandler("logs/quality_check.log"),  # File
        ]
    )


def main() -> int:
    """Main function - return exit code"""
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting quality check pipeline")

    # Loads config
    try:
        config = load_config()
    except FileNotFoundError as e:
        logger.error(f"Config not found: {e}")
        return 1

    spark = get_spark_session("QualityCheck")

    try:
        # Loads data
        df = load_all_raw_data(spark)

        # Starts all checks
        results = run_all_checks(df, config)

        # Prints the report
        print_quality_report(results)

        # Save to JSON
        report_file = save_quality_report_json(results, config["paths"]["reports"])
        logger.info(f"Report saved to: {report_file}")

        return 0

    except Exception as e:
        logger.exception(f"Quality check failed: {e}")
        return 1

    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    sys.exit(main())