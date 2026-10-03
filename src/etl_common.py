import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType


INPUT_PATH = "data/raw/sales.csv"
SALES_SCHEMA = StructType(
    [
        StructField("Transaction ID", IntegerType(), nullable=False),
        StructField("Date", DateType(), nullable=False),
        StructField("Customer ID", StringType(), nullable=False),
        StructField("Gender", StringType(), nullable=False),
        StructField("Age", IntegerType(), nullable=False),
        StructField("Product Category", StringType(), nullable=False),
        StructField("Quantity", IntegerType(), nullable=False),
        StructField("Price per Unit", IntegerType(), nullable=False),
        StructField("Total Amount", IntegerType(), nullable=False),
    ]
)


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )


def read_sales_csv(spark: SparkSession):
    return (
        spark.read
        .option("header", True)
        .option("dateFormat", "yyyy-MM-dd")
        .schema(SALES_SCHEMA)
        .csv(INPUT_PATH)
    )


def transform_sales_data(dataframe):
    from pyspark.sql import functions as F

    return (
        dataframe
        .withColumn("Year", F.year("Date"))
        .withColumn("Month", F.month("Date"))
        .withColumn("Month Start", F.date_trunc("month", "Date").cast("date"))
        .withColumn("Calculated Amount", F.col("Quantity") * F.col("Price per Unit"))
        .withColumn("Amount Variance", F.col("Total Amount") - F.col("Calculated Amount"))
    )
