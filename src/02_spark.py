from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("InspectSalesData")
    .master("local[*]")
    .getOrCreate()
)

file_path = "data/raw/sales.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(file_path)
)

print("\n========== SCHEMA ==========")
df.printSchema()

print("\n========== COLUMNS ==========")
print(df.columns)

print("\n========== FIRST 10 ROWS ==========")
df.show(10, truncate=False)

print("\n========== TOTAL ROWS ==========")
row_count = df.count()
print(row_count)

print("\n========== COLUMN COUNT ==========")
print(len(df.columns))

print("\n========== NULL COUNTS ==========")
null_counts = df.select(
    *[F.sum(F.col(column).isNull().cast("int")).alias(column) for column in df.columns]
)
null_counts.show(truncate=False)

print("\n========== DUPLICATE COUNTS ==========")
distinct_row_count = df.dropDuplicates().count()
print("Duplicate rows:", row_count - distinct_row_count)
print("Distinct rows:", distinct_row_count)

print("\n========== BASIC STATISTICS ==========")
df.describe().show()

print("\n========== DISTINCT CATEGORICAL VALUES ==========")
for column in ["Gender", "Product Category"]:
    print(f"{column}:")
    df.select(column).distinct().orderBy(column).show(truncate=False)

print("\n========== CANDIDATE KEYS AND DATE RANGE ==========")
for column in ["Transaction ID", "Customer ID"]:
    print(f"Distinct {column}:", df.select(column).distinct().count())
date_range = df.select(F.min("Date").alias("min_date"), F.max("Date").alias("max_date"))
date_range.show()

print("\n========== TOTAL AMOUNT CONSISTENCY ==========")
inconsistent_amounts = df.filter(
    F.col("Total Amount") != F.col("Quantity") * F.col("Price per Unit")
).count()
print("Rows where Total Amount != Quantity * Price per Unit:", inconsistent_amounts)

spark.stop()