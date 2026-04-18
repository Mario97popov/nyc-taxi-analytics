"""Quick test - checking if we can read the data."""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("QuickTest") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("data/raw/yellow_tripdata_2024-01.parquet")

separator = "=" * 60

print("\n" + separator)
print(f"Number of rows: {df.count():,}")
print(f"Number of columns: {len(df.columns)}")
print(separator + "\n")

print("Schema:")
df.printSchema()

print("\nFirst 5 rows:")
df.show(5, truncate=False)

spark.stop()
