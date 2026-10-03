from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("SalesETL")
    .master("local[*]")
    .getOrCreate()
)

print("===================================")
print("Spark started successfully!")
print("Spark version:", spark.version)
print("===================================")

spark.stop()