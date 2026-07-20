"""
Pytest fixtures - shared setup for texts.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Creates spark session which lives during the whole session.
    Does not create new session for each test - too slow
    """
    spark = (
        SparkSession.builder
        .appName("PySpark Tests")
        .master("local[2]")  # 2 cores for tests
        .config("spark.sql.shuffle.partitions", "2")  # little for speed
        .config("spark.sql.adaptive.enabled", "false")  # AQE
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")  # log in tests

    yield spark

    spark.stop()


@pytest.fixture
def sample_taxi_data(spark):
    """
    Small sample from taxi data for testing.
    All kind of cases.
    """
    from datetime import datetime

    data = [
        # Valid course (Monday, 9 AM, rush hour)
        (
            1,                                     # VendorID
            datetime(2024, 1, 8, 9, 0, 0),        # pickup
            datetime(2024, 1, 8, 9, 20, 0),       # dropoff (20 мин)
            2,                                     # passengers
            5.0,                                   # distance
            1,                                     # RatecodeID
            "N",                                   # store_and_fwd_flag
            100, 200,                              # PU, DO location
            1,                                     # payment (credit card)
            15.0,                                  # fare
            0.5, 0.5, 3.0, 0.0, 0.3, 19.3, 2.5, 0.0
        ),
        # Weekend course (Saturday)
        (
            2,
            datetime(2024, 1, 6, 14, 30, 0),
            datetime(2024, 1, 6, 15, 0, 0),
            1,
            3.0,
            1, "N", 50, 60, 2,
            10.0,
            0.0, 0.5, 0.0, 0.0, 0.3, 10.8, 0.0, 0.0
        ),
        # Airport course (JFK = 132)
        (
            1,
            datetime(2024, 1, 15, 20, 0, 0),
            datetime(2024, 1, 15, 20, 45, 0),
            3,
            18.0,
            2,  # JFK rate
            "N", 132, 50, 1,
            60.0,
            0.0, 0.5, 12.0, 6.55, 0.3, 79.35, 0.0, 1.75
        ),
    ]

    columns = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "store_and_fwd_flag", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge", "airport_fee"
    ]

    return spark.createDataFrame(data, columns)