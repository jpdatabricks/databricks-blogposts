try:
    from databricks.connect import DatabricksSession as SparkSession
except ImportError:
    from pyspark.sql import SparkSession as SparkSession

from pyspark.sql import DataFrame, functions as F

def get_spark() -> SparkSession:
  # This should be changed to use Spark in Local mode for unit tests
  spark = SparkSession.builder.getOrCreate()
  return spark

def get_nyctaxi_trips() -> DataFrame:
  spark = get_spark()
  df = spark.read.table("samples.nyctaxi.trips")
  return df


def calculate_avg_distance(df):
  return df.groupBy("pickup_zip").agg(F.avg("trip_distance").alias("avg"))