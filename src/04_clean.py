from functools import reduce
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl_common import create_spark_session, read_sales_csv


OUTPUT_PATH = "data/processed/sales_clean"


def clean_sales_data(sales_df: DataFrame) -> tuple[DataFrame, dict[str, int]]:
    input_count = sales_df.count()

    trimmed_df = sales_df.select(
        *[
            F.trim(F.col(column)).alias(column)
            if sales_df.schema[column].dataType.simpleString() == "string"
            else F.col(column)
            for column in sales_df.columns
        ]
    ).withColumn("Gender", F.initcap("Gender"))

    null_condition = reduce(
        lambda left, right: left | right,
        [F.col(column).isNull() for column in trimmed_df.columns],
    )
    null_count = trimmed_df.filter(null_condition).count()

    invalid_condition = (
        (F.col("Age") < 0)
        | (F.col("Age") > 120)
        | (F.col("Quantity") <= 0)
        | (F.col("Price per Unit") < 0)
        | (F.col("Total Amount") < 0)
        | (F.col("Total Amount") != F.col("Quantity") * F.col("Price per Unit"))
    )
    invalid_count = trimmed_df.filter(invalid_condition).count()

    non_null_df = trimmed_df.filter(~null_condition)
    valid_df = non_null_df.filter(~invalid_condition)
    deduplicated_df = valid_df.dropDuplicates(["Transaction ID"])
    duplicate_count = valid_df.count() - deduplicated_df.count()

    quality_summary = {
        "input_records": input_count,
        "null_records": null_count,
        "invalid_records": invalid_count,
        "duplicate_records": duplicate_count,
        "output_records": deduplicated_df.count(),
    }
    return deduplicated_df, quality_summary


def write_clean_parquet(cleaned_df: DataFrame) -> None:
    output_directory = Path(OUTPUT_PATH)
    output_directory.mkdir(parents=True, exist_ok=True)
    rows = [row.asDict() for row in cleaned_df.collect()]
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, output_directory / "part-00000.parquet")


if __name__ == "__main__":
    spark = create_spark_session("SalesETLCleaning")
    try:
        sales_df = read_sales_csv(spark)
        cleaned_df, summary = clean_sales_data(sales_df)
        print("========== DATA QUALITY SUMMARY ==========")
        for metric, value in summary.items():
            print(f"{metric}: {value}")
        write_clean_parquet(cleaned_df)
        print(f"Cleaned data written to: {OUTPUT_PATH}")
    finally:
        spark.stop()
