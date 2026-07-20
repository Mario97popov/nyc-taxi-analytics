"""
Main pipeline runner - runs the whole chain

Steps:
    1. Download data (skip if exists)
    2. Quality check on raw data
    3. Clean data
    4. Feature engineering
    5. Analytics
    6. Quality validation on clean data

How to use:
    python -m src.run_pipeline                    # Whole pipeline
    python -m src.run_pipeline --skip-download    # without download
    python -m src.run_pipeline --skip-analytics   # without analytics
"""
import argparse
import logging
import sys
import time
from pathlib import Path

from src.spark_session import get_spark_session, stop_spark_session, load_config
from src.loader import load_all_raw_data, load_yellow_taxi_data
from src.quality import run_all_checks
from src.reports.quality_report import print_quality_report, save_quality_report_json
from src.cleaner import clean_taxi_data, save_cleaned_data
from src.transformations import add_all_features
from src.analytics import (
    analyze_hourly_demand,
    analyze_day_of_week,
    find_top_routes,
    find_top_pickup_zones_per_hour,
    analyze_payment_types,
    analyze_airport_trips,
    analyze_revenue_patterns,
    analyze_traffic_by_hour,
    analyze_trip_length_distribution,
)


def setup_logging():
    """Setups logging to console + file"""
    Path("logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/pipeline.log"),
        ]
    )


class PipelineStep:
    """Context manager for pipeline steps with timing and logging."""

    def __init__(self, name: str, logger: logging.Logger):
        self.name = name
        self.logger = logger
        self.start_time: float = 0.0

    def __enter__(self):
        self.start_time = time.time()
        self.logger.info("")
        self.logger.info("#" * 70)
        self.logger.info(f"#  STEP: {self.name}")
        self.logger.info("#" * 70)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start_time
        if exc_type is None:
            self.logger.info(f"✓ {self.name} completed in {elapsed:.1f}s")
        else:
            self.logger.error(f"✗ {self.name} FAILED after {elapsed:.1f}s")
        return False  # dont get exceptions


def step_download(config: dict, logger: logging.Logger):
    """Step 1: Data download"""
    from src.download_data import main as download_main

    # download_data.py-a has it's main fun - we call it
    exit_code = download_main()
    if exit_code != 0:
        raise RuntimeError("Data download failed")


def step_quality_check_raw(spark, config: dict, logger: logging.Logger):
    """Step 2: Quality check on raw data."""
    df = load_all_raw_data(spark)
    results = run_all_checks(df, config)

    logger.info(f"Raw data rows: {results['summary']['total_rows']:,}")

    # Save JSON report
    report_file = save_quality_report_json(results, config["paths"]["reports"])
    logger.info(f"Quality report saved: {report_file}")


def step_clean(spark, config: dict, logger: logging.Logger):
    """Step 3: Cleaning data."""
    df = load_all_raw_data(spark)
    clean_df = clean_taxi_data(df, config)

    output_path = config["paths"]["processed_data"] + "/yellow_taxi_clean"
    save_cleaned_data(clean_df, output_path)
    logger.info(f"Clean data saved to: {output_path}")


def step_features(spark, config: dict, logger: logging.Logger):
    """Step 4: Feature engineering."""
    clean_path = config["paths"]["processed_data"] + "/yellow_taxi_clean"
    df = load_yellow_taxi_data(spark, clean_path, apply_schema=False)

    enriched_df = add_all_features(df)

    output_path = config["paths"]["processed_data"] + "/yellow_taxi_features"
    (enriched_df
        .write
        .mode("overwrite")
        .partitionBy("pickup_day")
        .parquet(output_path)
    )
    logger.info(f"Features saved to: {output_path}")


def step_analytics(spark, config: dict, logger: logging.Logger):
    """Step 5: Analytics."""
    features_path = config["paths"]["processed_data"] + "/yellow_taxi_features"
    df = load_yellow_taxi_data(spark, features_path, apply_schema=False)
    df.cache()

    output_dir = Path(config["paths"]["output_data"])
    output_dir.mkdir(parents=True, exist_ok=True)

    analyses = [
        ("hourly_demand", analyze_hourly_demand(df)),
        ("day_of_week", analyze_day_of_week(df)),
        ("top_routes", find_top_routes(df, top_n=20)),
        ("top_pickup_zones_per_hour", find_top_pickup_zones_per_hour(df, top_n=3)),
        ("payment_types", analyze_payment_types(df)),
        ("airport_trips", analyze_airport_trips(df)),
        ("revenue_patterns", analyze_revenue_patterns(df)),
        ("traffic_by_hour", analyze_traffic_by_hour(df)),
        ("trip_length_distribution", analyze_trip_length_distribution(df)),
    ]

    for name, analysis_df in analyses:
        logger.info(f"  Saving: {name}")
        (analysis_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .csv(str(output_dir / f"{name}_csv"))
        )

    df.unpersist()
    logger.info(f"All analytics saved to: {output_dir}")


def step_validate_clean(spark, config: dict, logger: logging.Logger):
    """Step 6: Verifications for the clean data if they are really cleaned."""
    features_path = config["paths"]["processed_data"] + "/yellow_taxi_features"
    df = load_yellow_taxi_data(spark, features_path, apply_schema=False)

    results = run_all_checks(df, config)

    # Checks if the main problems are 0
    total_issues = sum([
        sum(results["negative_values"].values()),
        sum(results["dates"].values()),
        sum(results["duration"].values()),
        sum(results["categories"].values()),
    ])

    if total_issues == 0:
        logger.info("✓ Clean data validation PASSED - no issues found")
    else:
        logger.warning(f"⚠ Clean data has {total_issues} remaining issues")


def parse_args():
    parser = argparse.ArgumentParser(description="NYC Taxi Analytics Pipeline")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip data download step"
    )
    parser.add_argument(
        "--skip-quality",
        action="store_true",
        help="Skip quality check on raw data"
    )
    parser.add_argument(
        "--skip-analytics",
        action="store_true",
        help="Skip analytics step"
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging()
    logger = logging.getLogger(__name__)

    pipeline_start = time.time()

    logger.info("=" * 70)
    logger.info("NYC TAXI ANALYTICS - FULL PIPELINE")
    logger.info("=" * 70)

    try:
        config = load_config()
    except FileNotFoundError as e:
        logger.error(f"Config not found: {e}")
        return 1

    # Step 1: Download (without Spark - uses requests)
    if not args.skip_download:
        with PipelineStep("Download Data", logger):
            step_download(config, logger)

    # steps with Spark
    spark = get_spark_session("FullPipeline")

    try:
        if not args.skip_quality:
            with PipelineStep("Quality Check (Raw)", logger):
                step_quality_check_raw(spark, config, logger)

        with PipelineStep("Data Cleaning", logger):
            step_clean(spark, config, logger)

        with PipelineStep("Feature Engineering", logger):
            step_features(spark, config, logger)

        if not args.skip_analytics:
            with PipelineStep("Analytics", logger):
                step_analytics(spark, config, logger)

        with PipelineStep("Validate Clean Data", logger):
            step_validate_clean(spark, config, logger)

        total_time = time.time() - pipeline_start
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY in {total_time:.1f}s")
        logger.info("=" * 70)

        return 0

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        return 1

    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    sys.exit(main())