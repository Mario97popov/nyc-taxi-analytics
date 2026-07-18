"""
Data Cleaning for NYC Taxi data.

We clean everything one by one and logging it.
Saves clean data in data/processed/
"""
import logging
from typing import Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, year as spark_year, month as spark_month,
    unix_timestamp
)


logger = logging.getLogger(__name__)


# ============================================================
# Helper: cleaning step with logs
# ============================================================

def apply_step(
    df: DataFrame,
    condition,
    step_name: str,
    before_count: int
) -> Tuple[DataFrame, int]:
    """
    Sets a filters and logs how many rows are removed.

    Args:
        df: DataFrame
        condition: PySpark condition (What to stay (filter))
        step_name: name of the step to log
        before_count: number of rows before the step

    Returns:
        (filtered_df, new_count)
    """
    filtered = df.filter(condition)
    after_count = filtered.count()
    removed = before_count - after_count
    pct_removed = (removed / before_count * 100) if before_count > 0 else 0

    logger.info(
        f"  [{step_name}] removed {removed:,} rows ({pct_removed:.2f}%) "
        f"-> {after_count:,} remaining"
    )

    return filtered, after_count


# ============================================================
# Cleaning steps
# ============================================================

def remove_nulls_in_critical_columns(
    df: DataFrame,
    before_count: int
) -> Tuple[DataFrame, int]:
    """Removes rows with null in important columns."""
    critical_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "PULocationID",
        "DOLocationID",
    ]

    condition = None
    for c in critical_columns:
        check = col(c).isNotNull()
        condition = check if condition is None else condition & check

    return apply_step(df, condition, "remove_nulls", before_count)


def remove_duplicates(
    df: DataFrame,
    before_count: int
) -> Tuple[DataFrame, int]:
    """Removes Dupes"""
    deduped = df.dropDuplicates()
    after_count = deduped.count()
    removed = before_count - after_count
    pct_removed = (removed / before_count * 100) if before_count > 0 else 0

    logger.info(
        f"  [remove_duplicates] removed {removed:,} rows ({pct_removed:.2f}%) "
        f"-> {after_count:,} remaining"
    )

    return deduped, after_count


def remove_invalid_dates(
    df: DataFrame,
    expected_year: int,
    expected_months: list,
    before_count: int
) -> Tuple[DataFrame, int]:
    """
    Removes rows with dates ouot of the desired dates.
    Removes rows where dropoff is before pickup.
    """
    condition = (
        (spark_year("tpep_pickup_datetime") == expected_year) &
        (spark_month("tpep_pickup_datetime").isin(expected_months)) &
        (col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
    )

    return apply_step(df, condition, "remove_invalid_dates", before_count)


def remove_invalid_trip_distance(
    df: DataFrame,
    rules: dict,
    before_count: int
) -> Tuple[DataFrame, int]:
    """Removes the rows with unvalid trip distance"""
    condition = (
        (col("trip_distance") >= rules["min_trip_distance"]) &
        (col("trip_distance") <= rules["max_trip_distance"])
    )

    return apply_step(df, condition, "remove_invalid_distance", before_count)


def remove_invalid_amounts(
    df: DataFrame,
    rules: dict,
    before_count: int
) -> Tuple[DataFrame, int]:
    """Removes rows with unvalid prices"""
    condition = (
        (col("fare_amount") >= rules["min_fare_amount"]) &
        (col("fare_amount") <= rules["max_fare_amount"]) &
        (col("total_amount") >= rules["min_total_amount"]) &
        (col("total_amount") <= rules["max_total_amount"]) &
        (col("tip_amount") >= 0) &
        (col("tolls_amount") >= 0)
    )

    return apply_step(df, condition, "remove_invalid_amounts", before_count)


def remove_invalid_passenger_count(
    df: DataFrame,
    rules: dict,
    before_count: int
) -> Tuple[DataFrame, int]:
    """Removoes rows with unvalid number of passengers"""
    condition = (
        (col("passenger_count") >= rules["min_passenger_count"]) &
        (col("passenger_count") <= rules["max_passenger_count"])
    )

    return apply_step(df, condition, "remove_invalid_passengers", before_count)


def remove_invalid_trip_duration(
    df: DataFrame,
    rules: dict,
    before_count: int
) -> Tuple[DataFrame, int]:
    """Remove rows with unvalid dropof to pickup."""
    duration_minutes = (
        (unix_timestamp("tpep_dropoff_datetime") -
         unix_timestamp("tpep_pickup_datetime")) / 60
    )

    condition = (
        (duration_minutes >= rules["min_trip_duration_minutes"]) &
        (duration_minutes <= rules["max_trip_duration_minutes"])
    )

    return apply_step(df, condition, "remove_invalid_duration", before_count)


def remove_invalid_locations(
    df: DataFrame,
    rules: dict,
    before_count: int
) -> Tuple[DataFrame, int]:
    """Removes unvalid location id's"""
    condition = (
        (col("PULocationID") >= rules["min_location_id"]) &
        (col("PULocationID") <= rules["max_location_id"]) &
        (col("DOLocationID") >= rules["min_location_id"]) &
        (col("DOLocationID") <= rules["max_location_id"])
    )

    return apply_step(df, condition, "remove_invalid_locations", before_count)


def remove_invalid_categories(
    df: DataFrame,
    rules: dict,
    before_count: int
) -> Tuple[DataFrame, int]:
    """Removes unvalid payment types and rate codes"""
    condition = (
        col("payment_type").isin(rules["valid_payment_types"]) &
        col("RatecodeID").isin(rules["valid_rate_codes"])
    )

    return apply_step(df, condition, "remove_invalid_categories", before_count)


# ============================================================
# Main Function - Starts all steps above
# ============================================================

def remove_unrealistic_speeds(
    df: DataFrame,
    before_count: int,
    max_speed_mph: float = 80.0
) -> Tuple[DataFrame, int]:
    """
    Removes courses with unrealistic speed.
    
    Wrong timestamps in taxy meters lead to absurd speed calculations (1000+ mph).

    Note: this check is based on aggregated speed, this is why we do it after the other filters.
    """
    from pyspark.sql.functions import unix_timestamp

    # aggregates speed inline (without adding column)
    duration_hours = (
        (unix_timestamp("tpep_dropoff_datetime") -
         unix_timestamp("tpep_pickup_datetime")) / 3600
    )

    speed = col("trip_distance") / duration_hours

    condition = (speed >= 0) & (speed <= max_speed_mph)

    return apply_step(df, condition, "remove_unrealistic_speed", before_count)


def clean_taxi_data(df: DataFrame, config: dict) -> DataFrame:
    """
    Starts all cleaning steps one by one

    Args:
        df: raw DataFrame
        config: loaded config

    Returns:
        Clean DataFrame
    """
    logger.info("=" * 60)
    logger.info("Starting data cleaning")
    logger.info("=" * 60)

    rules = config["quality_rules"]
    expected_year = config["expected_period"]["year"]
    expected_months = config["expected_period"]["months"]

    # Cache it so it's faster
    df.cache()
    initial_count = df.count()

    logger.info(f"Initial row count: {initial_count:,}")
    logger.info("")

    current = df
    count = initial_count

    # Steps one by one
    current, count = remove_nulls_in_critical_columns(current, count)
    current, count = remove_duplicates(current, count)
    current, count = remove_invalid_dates(current, expected_year, expected_months, count)
    current, count = remove_invalid_trip_distance(current, rules, count)
    current, count = remove_invalid_amounts(current, rules, count)
    current, count = remove_invalid_passenger_count(current, rules, count)
    current, count = remove_invalid_trip_duration(current, rules, count)
    current, count = remove_invalid_locations(current, rules, count)
    current, count = remove_invalid_categories(current, rules, count)
    current, count = remove_unrealistic_speeds(current, count, max_speed_mph=80.0)

    final_count = count
    total_removed = initial_count - final_count
    pct_removed = (total_removed / initial_count * 100) if initial_count > 0 else 0
    pct_kept = 100 - pct_removed

    logger.info("")
    logger.info("=" * 60)
    logger.info("Cleaning summary:")
    logger.info(f"  Initial:  {initial_count:,}")
    logger.info(f"  Removed:  {total_removed:,} ({pct_removed:.2f}%)")
    logger.info(f"  Final:    {final_count:,} ({pct_kept:.2f}% kept)")
    logger.info("=" * 60)

    # Release the cache
    df.unpersist()

    return current


def save_cleaned_data(
    df: DataFrame,
    output_path: str,
    partition_by: Optional[list] = None) -> None:
    """
    Saving the clean data in parquet format.

    Args:
        df: clean DataFrame
        output_path: path to the save
        partition_by: list with columns for partitioning
    """
    logger.info(f"Writing cleaned data to: {output_path}")

    writer = df.write.mode("overwrite")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.parquet(output_path)

    logger.info("Write complete")