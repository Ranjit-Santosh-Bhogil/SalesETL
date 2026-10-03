from etl_common import create_spark_session, read_sales_csv


if __name__ == "__main__":
    spark = create_spark_session("SalesETLIngestion")
    try:
        sales_df = read_sales_csv(spark)
        print("========== EXPLICIT INGESTION SCHEMA ==========")
        sales_df.printSchema()
        print("Input records:", sales_df.count())
        sales_df.show(10, truncate=False)
    finally:
        spark.stop()
