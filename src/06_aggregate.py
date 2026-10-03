import json
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl_common import create_spark_session, read_sales_csv, transform_sales_data


INPUT_PATH = "data/processed/sales_transformed"
PRODUCT_OUTPUT_PATH = "data/processed/sales_by_product"
MONTH_OUTPUT_PATH = "data/processed/sales_by_month"


def aggregate_by_product(transformed_df: DataFrame) -> DataFrame:
    return (
        transformed_df.groupBy("Product Category")
        .agg(
            F.countDistinct("Transaction ID").alias("transaction_count"),
            F.sum("Quantity").alias("units_sold"),
            F.sum("Total Amount").alias("total_sales"),
            F.avg("Total Amount").alias("average_transaction_value"),
        )
        .orderBy(F.desc("total_sales"))
    )


def aggregate_by_month(transformed_df: DataFrame) -> DataFrame:
    return (
        transformed_df.groupBy("Month Start")
        .agg(
            F.countDistinct("Transaction ID").alias("transaction_count"),
            F.sum("Quantity").alias("units_sold"),
            F.sum("Total Amount").alias("total_sales"),
        )
        .orderBy("Month Start")
    )


def write_json_lines(dataframe: DataFrame, output_path: str) -> None:
    output_directory = Path(output_path)
    output_directory.mkdir(parents=True, exist_ok=True)
    output_file = output_directory / "part-00000.json"
    with output_file.open("w", encoding="utf-8") as file:
        for row in dataframe.toLocalIterator():
            file.write(json.dumps(row.asDict(), default=str) + "\n")


if __name__ == "__main__":
    spark = create_spark_session("SalesETLAggregation")
    try:
        transformed_df = transform_sales_data(read_sales_csv(spark))
        product_df = aggregate_by_product(transformed_df)
        month_df = aggregate_by_month(transformed_df)
        write_json_lines(product_df, PRODUCT_OUTPUT_PATH)
        write_json_lines(month_df, MONTH_OUTPUT_PATH)
        print("========== SALES BY PRODUCT ==========")
        product_df.show(truncate=False)
        print("========== SALES BY MONTH ==========")
        month_df.show(truncate=False)
    finally:
        spark.stop()
