"""
Feature engineering module - adds derived columns to clean data

Each func returns a new DF with the new column
"""
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit,
    unix_timestamp,
    hour, dayofweek, dayofmonth, month, year,
    date_format,
    round as spark_round,
)


logger = logging.getLogger(__name__)


# ============================================================
# Section 1: Time-based features
# ============================================================

def add_trip_duration(df: DataFrame) -> DataFrame:
    """
    Adds trip_duration_minutes - duration of the course in minutes
    """
    return df.withColumn(
        "trip_duration_minutes",
        spark_round(
            (unix_timestamp("tpep_dropoff_datetime") -
             unix_timestamp("tpep_pickup_datetime")) / 60,
            2
        )
    )


def add_datetime_features(df: DataFrame) -> DataFrame:
    """
    Gets components from pickup datetime:
    - pickup_year, pickup_month, pickup_day
    - pickup_hour (0-23)
    - pickup_day_of_week (1=Sunday, 7=Saturday)
    - pickup_day_name (Monday, Tuesday, ...)
    """
    return (df
        .withColumn("pickup_year", year("tpep_pickup_datetime"))
        .withColumn("pickup_month", month("tpep_pickup_datetime"))
        .withColumn("pickup_day", dayofmonth("tpep_pickup_datetime"))
        .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
        .withColumn("pickup_day_of_week", dayofweek("tpep_pickup_datetime"))
        .withColumn("pickup_day_name", date_format("tpep_pickup_datetime", "EEEE"))
    )


def add_is_weekend(df: DataFrame) -> DataFrame:
    """
    Adds is_weekend column ("True" if the course is in Saturday or Sunday).
    Spark dayofweek: 1=Sunday, 7=Saturday.
    """
    return df.withColumn(
        "is_weekend",
        col("pickup_day_of_week").isin([1, 7])
    )


def add_time_of_day(df: DataFrame) -> DataFrame:
    """
    Buckets hours in hours by categories:
    - Night:     0-5
    - Morning:   6-11
    - Afternoon: 12-16
    - Evening:   17-21
    - Late:      22-23
    """
    return df.withColumn(
        "time_of_day",
        when(col("pickup_hour").between(0, 5), "Night")
        .when(col("pickup_hour").between(6, 11), "Morning")
        .when(col("pickup_hour").between(12, 16), "Afternoon")
        .when(col("pickup_hour").between(17, 21), "Evening")
        .otherwise("Late")
    )


def add_rush_hour_flag(df: DataFrame) -> DataFrame:
    """
    NYC rush hours: 7-9 AM and 17-19 PM in work days.
    """
    return df.withColumn(
        "is_rush_hour",
        (~col("is_weekend")) & (
            col("pickup_hour").between(7, 9) |
            col("pickup_hour").between(17, 19)
        )
    )


# ============================================================
# Section 2: Trip-based features
# ============================================================

def add_speed(df: DataFrame) -> DataFrame:
    """
    Gets the average speed
    speed = distance / time
    """
    return df.withColumn(
        "speed_mph",
        when(
            col("trip_duration_minutes") > 0,
            spark_round(
                col("trip_distance") / (col("trip_duration_minutes") / 60),
                2
            )
        ).otherwise(lit(0.0))
    )


def add_tip_percentage(df: DataFrame) -> DataFrame:
    """
    Tip from the amount
    """
    return df.withColumn(
        "tip_percentage",
        when(
            col("fare_amount") > 0,
            spark_round((col("tip_amount") / col("fare_amount")) * 100, 2)
        ).otherwise(lit(0.0))
    )


def add_trip_length_category(df: DataFrame) -> DataFrame:
    """
    Categgory the cource by length of course
    - Short:  < 2 мили
    - Medium: 2-5 мили
    - Long:   5-15 мили
    - Very Long: > 15 мили
    """
    return df.withColumn(
        "trip_length_category",
        when(col("trip_distance") < 2, "Short")
        .when(col("trip_distance") < 5, "Medium")
        .when(col("trip_distance") < 15, "Long")
        .otherwise("Very Long")
    )


# ============================================================
# Section 3: Category labels (ID -> Text)
# ============================================================

def add_payment_type_name(df: DataFrame) -> DataFrame:
    """Converts payment_type ID more readable type"""
    return df.withColumn(
        "payment_type_name",
        when(col("payment_type") == 1, "Credit card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 5, "Unknown")
        .when(col("payment_type") == 6, "Voided trip")
        .otherwise("Other")
    )


def add_rate_code_name(df: DataFrame) -> DataFrame:
    """Converts rate code ID in readable text."""
    return df.withColumn(
        "rate_code_name",
        when(col("RatecodeID") == 1, "Standard")
        .when(col("RatecodeID") == 2, "JFK")
        .when(col("RatecodeID") == 3, "Newark")
        .when(col("RatecodeID") == 4, "Nassau/Westchester")
        .when(col("RatecodeID") == 5, "Negotiated")
        .when(col("RatecodeID") == 6, "Group ride")
        .otherwise("Unknown")
    )


def add_vendor_name(df: DataFrame) -> DataFrame:
    """Converts VendorID in name of company."""
    return df.withColumn(
        "vendor_name",
        when(col("VendorID") == 1, "Creative Mobile")
        .when(col("VendorID") == 2, "VeriFone")
        .when(col("VendorID") == 6, "Myle Technologies")
        .when(col("VendorID") == 7, "Helix")
        .otherwise("Unknown")
    )


# ============================================================
# Section 4: Airport flags
# ============================================================

# Airport location IDs From NYC TLC zone map
JFK_ZONE_IDS = [132]              # JFK Airport
LGA_ZONE_IDS = [138]              # LaGuardia Airport
EWR_ZONE_IDS = [1]                # Newark Airport


def add_airport_flags(df: DataFrame) -> DataFrame:
    """
    Marks airport Course.
    """
    all_airports = JFK_ZONE_IDS + LGA_ZONE_IDS + EWR_ZONE_IDS

    return (df
        .withColumn(
            "is_pickup_airport",
            col("PULocationID").isin(all_airports)
        )
        .withColumn(
            "is_dropoff_airport",
            col("DOLocationID").isin(all_airports)
        )
        .withColumn(
            "is_airport_trip",
            col("PULocationID").isin(all_airports) |
            col("DOLocationID").isin(all_airports)
        )
    )


# ============================================================
# Section 5: Main Function - does all the transformations
# ============================================================

def add_all_features(df: DataFrame) -> DataFrame:
    """
    Does all feature engineering steps.

    Order of execution is important, some function depend on columns created from other functions.
    """
    logger.info("Starting feature engineering")

    df = add_trip_duration(df)
    logger.info("  ✓ trip_duration_minutes")

    df = add_datetime_features(df)
    logger.info("  ✓ datetime features (year, month, day, hour, day_name)")

    df = add_is_weekend(df)
    logger.info("  ✓ is_weekend")

    df = add_time_of_day(df)
    logger.info("  ✓ time_of_day")

    df = add_rush_hour_flag(df)
    logger.info("  ✓ is_rush_hour")

    df = add_speed(df)
    logger.info("  ✓ speed_mph")

    df = add_tip_percentage(df)
    logger.info("  ✓ tip_percentage")

    df = add_trip_length_category(df)
    logger.info("  ✓ trip_length_category")

    df = add_payment_type_name(df)
    logger.info("  ✓ payment_type_name")

    df = add_rate_code_name(df)
    logger.info("  ✓ rate_code_name")

    df = add_vendor_name(df)
    logger.info("  ✓ vendor_name")

    df = add_airport_flags(df)
    logger.info("  ✓ airport flags")

    logger.info(f"Feature engineering complete. Total columns: {len(df.columns)}")

    return df