"""
Creating a spark session
We will use get_spark_session() from all scripts
"""
from pathlib import Path
import yaml
from pyspark.sql import SparkSession

def load_config(config_path: str = "config/config.yaml") -> dict:
    """We will load the yaml configuration"""
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found in {config_path}")
    
    with open(config_file, "r") as f:
        return yaml.safe_load(f)

def get_spark_session(app_name: str | None = None) -> SparkSession:
    """
    Creates or gets an existing session from the config.yaml file
    We return a configured spark session
    """
    config = load_config()
    spark_config = config["spark"]

    name = app_name or spark_config["app_name"]

    spark = (
        SparkSession.builder
        .appName(name)
        .master(spark_config["master"])
        .config("spark.driver.memory", spark_config["driver_memory"])
        .config("spark.sql.shuffle.partitions", spark_config["shuffle_partitions"])
        # AQE 
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Better timehandling of timestamp of parquet 
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        # Lowers the overhead
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(spark_config["log_level"])
    
    return spark

def stop_spark_session(spark: SparkSession) -> None:
    """Stops spark session"""
    if spark is not None:
        spark.stop()

if __name__ == "__main__":
    # Ако пуснеш файла директно - прави бърз тест
    spark = get_spark_session("TestSession")
    print(f"✓ Spark version: {spark.version}")
    print(f"✓ App name: {spark.sparkContext.appName}")
    print(f"✓ Master: {spark.sparkContext.master}")
    stop_spark_session(spark)


"""
AQE - Spraks optimazies the queries alone
KryoSerializer  - 10 X times faster than the Java serializer
datetimeRebaseModeInRead - fixes problems with timestamps in Parqet files
if __name__ == "__main__": - code that executes only if we start the file directly and not when we import it
"""