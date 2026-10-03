# Sales Data ETL Pipeline with PySpark

## Local environment

This project uses Python 3.13 and a project-specific virtual environment at
`.venv`. PySpark 4.0.1 requires a compatible Java runtime for the local Spark
process. Java 24 is installed on the machine, but Spark cannot start with it
because Java 24 removed an API used by Hadoop. Use the project-local JDK 21
installation when running Spark:

```powershell
$env:JAVA_HOME = "C:\Users\Admin\.jdk\jdk-21\jdk-21.0.10"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:HADOOP_HOME = (Resolve-Path ".\tools\hadoop").Path
.\.venv\Scripts\python.exe .\src\01_spark.py
```

Expected output includes:

```text
Spark started successfully!
Spark version: 4.0.1
```

The native Hadoop library warning is expected for a local Windows Spark setup.
The repository setup uses a local `tools\hadoop\bin\winutils.exe` utility for
filesystem operations; the `tools` directory is ignored because this binary is
machine-specific.

## Phase 3: explicit ingestion

Run the ingestion stage from the project root:

```powershell
.\.venv\Scripts\python.exe .\src\03_ingest.py
```

The stage reads `data/raw/sales.csv` with an explicit schema rather than
inferring types from the file. It should report 1,000 input records and parse
`Date` as a Spark `date` column.

## Phase 4: cleaning and quality checks

```powershell
.\.venv\Scripts\python.exe .\src\04_clean.py
```

The cleaning stage trims string values, standardizes gender capitalization,
removes duplicate transaction IDs, rejects null or invalid business records,
prints a quality summary, and writes cleaned Parquet files under
`data/processed/sales_clean`. Spark performs the transformations; PyArrow
serializes the small local result because Spark 4.0.1's bundled Hadoop 3.4.1
does not provide a compatible Windows native filesystem library.

## Phase 5: transformations and aggregations

```powershell
.\.venv\Scripts\python.exe .\src\05_transform.py
.\.venv\Scripts\python.exe .\src\06_aggregate.py
```

The dataset has no region column, so the pipeline does not fabricate a region
dimension. Instead, it derives year/month fields and amount consistency
columns, then aggregates sales by product category and month. These stages
write JSON because Spark's Windows filesystem limitations affect Hadoop
filesystem reads/writes. PyArrow handles the local Parquet boundary, while
Spark still performs the transformations and aggregations; the final
PostgreSQL load will use JDBC.