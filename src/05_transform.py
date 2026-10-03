import json
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl_common import create_spark_session, read_sales_csv, transform_sales_data


INPUT_PATH = "data/processed/sales_clean"
OUTPUT_PATH = "data/processed/sales_transformed"


def write_json_lines(dataframe: DataFrame, output_path: str) -> None:
    output_directory = Path(output_path)
    output_directory.mkdir(parents=True, exist_ok=True)
    output_file = output_directory / "part-00000.json"
    with output_file.open("w", encoding="utf-8") as file:
        for row in dataframe.toLocalIterator():
            file.write(json.dumps(row.asDict(), default=str) + "\n")


if __name__ == "__main__":
    spark = create_spark_session("SalesETLTransformation")
    try:
        cleaned_df = read_sales_csv(spark)
        transformed_df = transform_sales_data(cleaned_df)
        write_json_lines(transformed_df, OUTPUT_PATH)
        print("Transformed records:", transformed_df.count())
        transformed_df.select(
            "Transaction ID",
            "Date",
            "Year",
            "Month",
            "Calculated Amount",
            "Amount Variance",
        ).show(10, truncate=False)
        print(f"Transformed data written to: {OUTPUT_PATH}")
    finally:
        spark.stop()
