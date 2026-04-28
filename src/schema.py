"""
Explicit schema for NYC Yellow Taxi data.
  1. Faster - Spark does not clean data twice
  2. More Sagee - if format changes we will see error
  3. Documented - we clearly see what we work with
"""

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, LongType, DoubleType, StringType, TimestampType
)

# Schema for NYC Yellow Taxi data (2024)
# Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
YELLOW_TAXI_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), nullable=True),
    StructField("tpep_pickup_datetime", TimestampType(), nullable=True),
    StructField("tpep_dropoff_datetime", TimestampType(), nullable=True),
    StructField("passenger_count", LongType(), nullable=True),
    StructField("trip_distance", DoubleType(), nullable=True),
    StructField("RatecodeID", LongType(), nullable=True),
    StructField("store_and_fwd_flag", StringType(), nullable=True),
    StructField("PULocationID", IntegerType(), nullable=True),
    StructField("DOLocationID", IntegerType(), nullable=True),
    StructField("payment_type", LongType(), nullable=True),
    StructField("fare_amount", DoubleType(), nullable=True),
    StructField("extra", DoubleType(), nullable=True),
    StructField("mta_tax", DoubleType(), nullable=True),
    StructField("tip_amount", DoubleType(), nullable=True),
    StructField("tolls_amount", DoubleType(), nullable=True),
    StructField("improvement_surcharge", DoubleType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=True),
    StructField("congestion_surcharge", DoubleType(), nullable=True),
    StructField("airport_fee", DoubleType(), nullable=True),
])


# Values for validation
# From TLC documentation
RATE_CODES = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
    99: "Unknown"
}

PAYMENT_TYPES = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}

VENDOR_IDS = {
    1: "Creative Mobile Technologies",
    2: "VeriFone Inc.",
    6: "Myle Technologies",
    7: "Helix"
}

def print_schema_summary() -> None:
    """function for debug - shows schema readable way"""
    print(f"\n{'='*60}")
    print("NYC Yellow Taxi Schema")
    print(f"{'='*60}")
    for field in YELLOW_TAXI_SCHEMA.fields:
        nullable = "nullable" if field.nullable else "NOT NULL"
        print(f"  {field.name:30} {str(field.dataType):20} {nullable}")
    print(f"{'='*60}")
    print(f"Total columns: {len(YELLOW_TAXI_SCHEMA.fields)}\n")

if __name__ == "__main__":
    print_schema_summary()


"""
StructType / StructField – Spark way to define of schema
LongType vs IntegerType - LongType is 64 bit | IntegerType  is 32 bit | if counting always LongType
Dictionary for validations
"""