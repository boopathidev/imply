
# PySpark Data Processing Script

This project processes city data from various file formats using PySpark. The processed data is saved in the specified output directory.

## Prerequisites

- **Python 3.x**: Ensure Python is installed on your system.
- **PySpark**: Install PySpark before running the script.

## Data Files

Ensure the following files are available in the working directory:

- `CityListA.json` (JSON format)
- `CityListB.avro` (Avro format)
- `CityList.csv` (CSV format)

## Output Directory

The final processed data will be saved to the `output/` directory in the working directory.

## Execution

To run the script, ensure all environment variables and paths are correctly set, then execute the following command in your Python environment:

```bash
python path/to/main.py
```

## Data Insights

- **Total Row Count:** 2583
- **City with the Largest Population:** Mumbai (Bombay) with a population of 10,500,000
- **Total Population of Cities in Brazil (CountryCode == BRA):** 55,955,012

## What changes could be made to improve your program's performance ?

To improve the performance of your PySpark script, consider the following strategies:

- **Partitioning:** Partition data based on a key (e.g., `CountryCode`) to ensure even distribution across the cluster and prevent bottlenecks.
- **Persisting Data:** Cache or persist DataFrames after expensive operations to avoid recomputing them.
- **Broadcast Joins:** Use broadcast joins for smaller DataFrames to optimize join performance.
- **Shuffle and Spark Configurations:**
  - **Shuffling Data:** Configure `spark.shuffle.memoryFraction` or `spark.sql.shuffle.partitions` to manage shuffle memory.
  - **Executor Memory:** Adjust `spark.executor.memory` and `spark.driver.memory` settings to handle memory issues.
- **Optimizing File I/O:**
  - **File Format:** Use optimized formats like Parquet for large datasets.
  - **Reducing Small Files:** Use `coalesce` or `repartition` to reduce the number of output files.
- **Concurrency and Resource Allocation:**
  - **Parallelism:** Adjust the number of cores per executor to optimize resource allocation.

## How would you scale your solution to a much larger dataset (too large for a single machine to store)?


- **Distributed Storage System:**
  - **HDFS:** Use Hadoop Distributed File System (HDFS) to spread data across multiple nodes.
  - **Cloud Storage:** Utilize cloud storage services such as Amazon S3, Google Cloud Storage, or Azure Blob Storage.
  - **Iceberg/Hudi** Consider using Apache Iceberg/Hudi for high-performance handling of large analytic tables.

## Testing against OLAP databases:
    - The data was likewise put into Snowflake and Duckdb, and the same tests were performed. 
