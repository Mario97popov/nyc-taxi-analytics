"""
Data Quality Framework for NYC Taxi data.

Each functions returns a dicts with results, it's easy for future use in this file for report or alerting.

"""
import logging
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, when, sum as spark_sum,
    year as spark_year, month as spark_month,
    unix_timestamp, lit
)


logger = logging.getLogger(__name__)

# ============================================================
# Section 1: General statistics
# ============================================================

def get_row_count(df: DataFrame) -> int:
    """
    Number of rows in a DF, this is action 
    """
    return df.count()

def get_column_count(df: DataFrame) -> int:
    """
    Number of columns
    """
    return len(df.columns)

# ============================================================
# Section 2: Null analysis
# ============================================================

def check_nulls(df: DataFrame) -> dict:
    """
    Number of nulls in each column

    Returns:
        Dict {column_name: null_count}
    """
    logger.info("Checking nulls in all columns...")

    # Number of nulls in all columns at once
    # Better than loops with different requests
    null_counts_row = df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ]).collect()[0]

    # convert in row dict
    return {c: null_counts_row[c] for c in df.columns}


def get_null_percentage(null_counts: dict, total_rows: int) -> dict:
    """Transform number of nulls in percent % this shows the % of nulls in a column"""
    if total_rows == 0:
        return {c: 0.0 for c in null_counts}

    return {
        c: round((count / total_rows) * 100, 2)
        for c, count in null_counts.items()
    }

# ============================================================
# Section 3: Duplicates
# ============================================================

def count_duplicates(df: DataFrame) -> int:
    """
    Number of dupes
    """
    total = df.count()
    distinct = df.distinct().count()
    return total - distinct


# ============================================================
# Section 4: Business rules
# ============================================================

def check_negative_values(df: DataFrame) -> dict:
    """
    We check for negative values in columns where we shouldnt have such values.
    """
    logger.info("Checking negative values...")

    checks = {
        "negative_fare_amount": col("fare_amount") < 0,
        "negative_trip_distance": col("trip_distance") < 0,
        "negative_tip_amount": col("tip_amount") < 0,
        "negative_total_amount": col("total_amount") < 0,
        "negative_passenger_count": col("passenger_count") < 0,
    }

    # Number of conditions with one request, much faster than 5 different counters.
    result_row = df.select([
        count(when(condition, True)).alias(name)
        for name, condition in checks.items()
    ]).collect()[0]

    return {name: result_row[name] for name in checks.keys()}


def check_zero_values(df: DataFrame) -> dict:
    """
    Checks for 0 values where they are sus
    """
    logger.info("Checking zero values...")

    checks = {
        "zero_trip_distance": col("trip_distance") == 0,
        "zero_passenger_count": col("passenger_count") == 0,
        "zero_total_amount": col("total_amount") == 0,
    }

    result_row = df.select([
        count(when(condition, True)).alias(name)
        for name, condition in checks.items()
    ]).collect()[0]

    return {name: result_row[name] for name in checks.keys()}


def check_outliers(df: DataFrame, rules: dict) -> dict:
    """
    Check for values out of normal ranges

    Args:
        df: Spark DataFrame
        rules: Dict с rules от config (quality_rules section)
    """
    logger.info("Checking outliers...")

    checks = {
        "distance_too_high": col("trip_distance") > rules["max_trip_distance"],
        "fare_too_high": col("fare_amount") > rules["max_fare_amount"],
        "total_too_high": col("total_amount") > rules["max_total_amount"],
        "passengers_too_high": col("passenger_count") > rules["max_passenger_count"],
    }
    
    result_row = df.select([
        count(when(condition, True)).alias(name)
        for name, condition in checks.items()
    ]).collect()[0]

    return {name: result_row[name] for name in checks.keys()}

# ============================================================
# Section 5: Date validation
# ============================================================

def check_date_validity(
    df: DataFrame,
    expected_year: int,
    expected_months: list
) -> dict:
    """
    Checks if the data is in the expected time perios

    A classic trap in TLC data, there are records whose dates fall outside the period you want to analyze
    """
    logger.info("Checking date validity...")

    pickup_year = spark_year("tpep_pickup_datetime")
    pickup_month = spark_month("tpep_pickup_datetime")

    checks = {
        "pickup_null": col("tpep_pickup_datetime").isNull(),
        "dropoff_null": col("tpep_dropoff_datetime").isNull(),
        "wrong_year": (pickup_year != expected_year) & col("tpep_pickup_datetime").isNotNull(),
        "wrong_month": (
            (pickup_year == expected_year) &
            (~pickup_month.isin(expected_months)) &
            col("tpep_pickup_datetime").isNotNull()
        ),
        "dropoff_before_pickup": (
            col("tpep_dropoff_datetime") < col("tpep_pickup_datetime")
        ),
    }

    result_row = df.select([
        count(when(condition, True)).alias(name)
        for name, condition in checks.items()
    ]).collect()[0]

    return {name: result_row[name] for name in checks.keys()}


def check_trip_duration(df: DataFrame, rules: dict) -> dict:
    """
    Checks for unvalid checks for unvalid duration of the taxi course.
    """
    logger.info("Checking trip duration...")

    # calculates duration in minutes
    duration_minutes = (
        (unix_timestamp("tpep_dropoff_datetime") -
         unix_timestamp("tpep_pickup_datetime")) / 60
    )

    checks = {
        "duration_too_short": duration_minutes < rules["min_trip_duration_minutes"],
        "duration_too_long": duration_minutes > rules["max_trip_duration_minutes"],
        "negative_duration": duration_minutes < 0,
    }

    result_row = df.select([
        count(when(condition, True)).alias(name)
        for name, condition in checks.items()
    ]).collect()[0]

    return {name: result_row[name] for name in checks.keys()}

# ============================================================
# Section 6: Reference data validation
# ============================================================

def check_invalid_categories(df: DataFrame, rules: dict) -> dict:
    """
    Checks for unvalid values in category columns
    """
    logger.info("Checking invalid categorical values...")

    checks = {
        "invalid_payment_type": (
            col("payment_type").isNotNull() &
            (~col("payment_type").isin(rules["valid_payment_types"]))
        ),
        "invalid_rate_code": (
            col("RatecodeID").isNotNull() &
            (~col("RatecodeID").isin(rules["valid_rate_codes"]))
        ),
        "invalid_pickup_location": (
            (col("PULocationID") < rules["min_location_id"]) |
            (col("PULocationID") > rules["max_location_id"])
        ),
        "invalid_dropoff_location": (
            (col("DOLocationID") < rules["min_location_id"]) |
            (col("DOLocationID") > rules["max_location_id"])
        ),
    }

    result_row = df.select([
        count(when(condition, True)).alias(name)
        for name, condition in checks.items()
    ]).collect()[0]

    return {name: result_row[name] for name in checks.keys()}


# ============================================================
# Section 7: Main Functions - Starts all checks we made till now
# ============================================================

def run_all_checks(df: DataFrame, config: dict) -> dict:
    """
    Starts all data quality checks and returns an agregated report

    Args:
        df: DataFrame for check
        config: Loaded config (from config.yaml)

    Returns:
        Dict with all the results, grouped by category.
    """
    logger.info("="*60)
    logger.info("Starting full data quality check")
    logger.info("="*60)

    rules = config["quality_rules"]
    expected_year = config["expected_period"]["year"]
    expected_months = config["expected_period"]["months"]

    # IMPORTANT: We cache the DF as we will use it a lot
    # Without cache, each count will reread the Parquet file
    df.cache()
    total_rows = df.count()  # Forces the cache

    results = {
        "summary": {
            "total_rows": total_rows,
            "total_columns": get_column_count(df),
        },
        "nulls": {
            "counts": check_nulls(df),
        },
        "duplicates": {
            "full_duplicates": count_duplicates(df),
        },
        "negative_values": check_negative_values(df),
        "zero_values": check_zero_values(df),
        "outliers": check_outliers(df, rules),
        "dates": check_date_validity(df, expected_year, expected_months),
        "duration": check_trip_duration(df, rules),
        "categories": check_invalid_categories(df, rules),
    }

    # Adds null percentage in the summary
    results["nulls"]["percentages"] = get_null_percentage(
        results["nulls"]["counts"], total_rows
    )

    # Releases the cache
    df.unpersist()

    logger.info("Data quality check complete")
    return results


if __name__ == "__main__":
    from src.spark_session import get_spark_session, stop_spark_session, load_config
    from src.loader import load_all_raw_data

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    spark = get_spark_session("QualityCheck")
    config = load_config()

    try:
        df = load_all_raw_data(spark)
        results = run_all_checks(df, config)

        # Fast summary
        print(f"\nTotal rows: {results['summary']['total_rows']:,}")
        print(f"Duplicates: {results['duplicates']['full_duplicates']:,}")
        print(f"Negative values: {sum(results['negative_values'].values()):,}")
        print(f"Zero values: {sum(results['zero_values'].values()):,}")
        print(f"Outliers: {sum(results['outliers'].values()):,}")
        print(f"Date issues: {sum(results['dates'].values()):,}")
        print(f"Duration issues: {sum(results['duration'].values()):,}")
        print(f"Category issues: {sum(results['categories'].values()):,}")

    finally:
        stop_spark_session(spark)