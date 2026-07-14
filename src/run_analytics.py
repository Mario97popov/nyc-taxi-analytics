"""
Main script for analysis requests

Read enriched data from data/processed/yellow_taxi_features
saves results in data/output/

Use:
    python -m src.run_analytics
"""
import logging
import sys
from pathlib import Path

from src.spark_session import get_spark_session, stop_spark_session, load_config
from src.loader import load_yellow_taxi_data
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/analytics.log"),
        ]
    )


def save_analysis(df, output_dir: str, name: str) -> None:
    """ Saves the analysis as parquet and csv"""
    output_path = Path(output_dir) / name

    # Coalesce(1) - sums everything in one file
    # For small aggregations this is okay
    df.coalesce(1).write.mode("overwrite").parquet(str(output_path))

    # Save as csv for easier open in excel
    csv_path = Path(output_dir) / f"{name}_csv"
    (df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(str(csv_path))
    )


def main() -> int:
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting analytics pipeline")

    try:
        config = load_config()
    except FileNotFoundError as e:
        logger.error(f"Config not found: {e}")
        return 1

    spark = get_spark_session("Analytics")

    try:
        # Loads the enriched data
        features_path = config["paths"]["processed_data"] + "/yellow_taxi_features"
        df = load_yellow_taxi_data(spark, features_path, apply_schema=False)

        total_rows = df.count()
        logger.info(f"Loaded {total_rows:,} rows for analysis")

        # Cache - we will read more than once
        df.cache()

        output_dir = config["paths"]["output_data"]
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # All analysis
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
            logger.info(f"Saving: {name}")
            save_analysis(analysis_df, output_dir, name)

            # Short preview
            print(f"\n=== {name.upper()} ===")
            analysis_df.show(10, truncate=False)

        df.unpersist()

        logger.info("=" * 60)
        logger.info(f"Analytics complete. Results saved to: {output_dir}")
        logger.info("=" * 60)

        return 0

    except Exception as e:
        logger.exception(f"Analytics failed: {e}")
        return 1

    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    sys.exit(main())