"""
Analysis on NYC Taxy data.

Each function returns DF with agg result that is ready for presentation.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    round as spark_round, desc, asc,
    row_number, rank, dense_rank,
    lag, lead, percent_rank,
    when, lit,
)

logger = logging.getLogger(__name__)

# ============================================================
# Section 1: Hourly demand analysis
# ============================================================

def analyze_hourly_demand(df: DataFrame) -> DataFrame:
    """
    Analysis by the hour

    For each hour:
    - number of courses
    - avg price
    - avg distance
    - avg speed (proxy for traffic)
    - avg tip in %
    """
    logger.info("Analyzing hourly demand...")

    return (df
        .groupBy("pickup_hour")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("speed_mph"), 1).alias("avg_speed_mph"),
            spark_round(avg("tip_percentage"), 2).alias("avg_tip_pct"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        )
        .orderBy("pickup_hour")
    )

# ============================================================
# Secttion 2: Day of week analysis
# ============================================================

def analyze_day_of_week(df: DataFrame) -> DataFrame:
    """
    Analys by the day of the week
    """
    logger.info("Analyzing day of week patterns...")

    return (df
        .groupBy("pickup_day_of_week", "pickup_day_name")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("tip_percentage"), 2).alias("avg_tip_pct"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        )
        .orderBy("pickup_day_of_week")
    )

# ============================================================
# Section 3: Top routes (with window functions)
# ============================================================

def find_top_routes(df: DataFrame, top_n: int = 20) -> DataFrame:
    """
    Gets top number of most used routes. (pickup -> dropoff)

    Returns:
    - PULocationID, DOLocationID
    - number courses
    - avg price
    - Total revenue
    - Rank (1 = most money)
    """
    logger.info(f"Finding top {top_n} routes...")

    # Window for ranking
    window = Window.orderBy(desc("trip_count"))

    return (df
        .groupBy("PULocationID", "DOLocationID")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        )
        .withColumn("rank", row_number().over(window))
        .filter(col("rank") <= top_n)
        .orderBy("rank")
    )

# ============================================================
# Section 4: Top pickup zones per hour (window function!)
# ============================================================

def find_top_pickup_zones_per_hour(df: DataFrame, top_n: int = 5) -> DataFrame:
    """
    For each hour finds the top number of pickup zones

    Groups by hour but ranking is in the group
    """
    logger.info(f"Finding top {top_n} pickup zones per hour...")

    # Agg by hour and zone
    aggregated = (df
        .groupBy("pickup_hour", "PULocationID")
        .agg(
            count("*").alias("trip_count"),
            spark_round(spark_sum("total_amount"), 2).alias("revenue"),
        )
    )

    # Window: partitioned by hour, ordered by trip_count
    window = Window.partitionBy("pickup_hour").orderBy(desc("trip_count"))

    return (aggregated
        .withColumn("hour_rank", row_number().over(window))
        .filter(col("hour_rank") <= top_n)
        .orderBy("pickup_hour", "hour_rank")
    )


# ============================================================
# Section 5: Payment type analysis
# ============================================================

def analyze_payment_types(df: DataFrame) -> DataFrame:
    """
    Analysys on different ways of paying
    """
    logger.info("Analyzing payment types...")

    # Total number of courses for aggregate on %
    total_trips = df.count()

    return (df
        .groupBy("payment_type_name")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("total_amount"), 2).alias("avg_total"),
            spark_round(avg("tip_amount"), 2).alias("avg_tip"),
            spark_round(avg("tip_percentage"), 2).alias("avg_tip_pct"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        )
        .withColumn(
            "percentage_of_trips",
            spark_round((col("trip_count") / total_trips) * 100, 2)
        )
        .orderBy(desc("trip_count"))
    )


# ============================================================
# Section 6: Airport analysis
# ============================================================

def analyze_airport_trips(df: DataFrame) -> DataFrame:
    """
    Analys on airport vs non airport courses
    """
    logger.info("Analyzing airport trips...")

    return (df
        .groupBy("is_airport_trip")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("total_amount"), 2).alias("avg_total"),
            spark_round(avg("tip_percentage"), 2).alias("avg_tip_pct"),
        )
        .orderBy("is_airport_trip")
    )


# ============================================================
# Section 7: Revenue by time of day и weekday/weekend
# ============================================================

def analyze_revenue_patterns(df: DataFrame) -> DataFrame:
    """
    Cross-analysis: revenue по time_of_day x weekday/weekend.

    This is pivot-like Analysis - two dimensions.
    """
    logger.info("Analyzing revenue patterns...")

    return (df
        .withColumn(
            "day_type",
            when(col("is_weekend"), "Weekend").otherwise("Weekday")
        )
        .groupBy("time_of_day", "day_type")
        .agg(
            count("*").alias("trip_count"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_revenue_per_trip"),
        )
        .orderBy("time_of_day", "day_type")
    )


# ============================================================
# Section 8: Speed analysis (traffic insights)
# ============================================================

def analyze_traffic_by_hour(df: DataFrame) -> DataFrame:
    """
    Speed as proxy for traffic
    Low speed = big traffic
    """
    from pyspark.sql.functions import expr

    logger.info("Analyzing traffic patterns by hour...")

    return (df
        .filter(col("trip_distance") > 0.5)
        .filter(col("speed_mph") > 0)
        .filter(col("speed_mph") <= 80)
        .groupBy("pickup_hour")
        .agg(
            spark_round(avg("speed_mph"), 2).alias("avg_speed_mph"),
            spark_round(expr("percentile_approx(speed_mph, 0.5)"), 2).alias("median_speed_mph"),
            spark_round(expr("percentile_approx(speed_mph, 0.25)"), 2).alias("p25_speed_mph"),
            spark_round(expr("percentile_approx(speed_mph, 0.75)"), 2).alias("p75_speed_mph"),
            count("*").alias("sample_size"),
        )
        .orderBy("pickup_hour")
    )

# ============================================================
# Section 9: Trip length distribution
# ============================================================

def analyze_trip_length_distribution(df: DataFrame) -> DataFrame:
    """
    Distribution по trip_length_category.
    """
    logger.info("Analyzing trip length distribution...")

    total = df.count()

    return (df
        .groupBy("trip_length_category")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("tip_percentage"), 2).alias("avg_tip_pct"),
        )
        .withColumn(
            "percentage",
            spark_round((col("trip_count") / total) * 100, 2)
        )
        .orderBy(desc("trip_count"))
    )