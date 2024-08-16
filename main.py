import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
import findspark
findspark.init()
import logging
from pyspark import SparkConf, SparkContext, sql
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import col, sum as spark_sum

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Initialize Spark and setup environment variables
try:
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    findspark.init()

    logging.info("Starting Spark session")

    sc = SparkSession \
        .builder \
        .appName("My App") \
        .master("local[8]") \
        .config("pyspark --jars",
                "/opt/homebrew/Cellar/apache-spark/3.1.1/libexec/jars/mysql-connector-java-8.0.19.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.1.1") \
        .getOrCreate()

    logging.info("Spark session started successfully")

    # Step 2: Load Data from JSON, Avro, and CSV Files
    logging.info("Loading data from JSON, Avro, and CSV files")
  
    # Set the base directory for your datasets
    current_dir = os.path.join(os.getcwd(), "DataSets")

    # Construct file paths using os.path.join for better readability and compatibility
    json_path = os.path.join(current_dir, "CityListA.json")
    avro_path = os.path.join(current_dir, "CityListB.avro")
    csv_path = os.path.join(current_dir, "CityList.csv")

    # Load the datasets using the constructed paths
    city_list_a = sc.read.json(json_path)
    city_list_b = sc.read.format("avro").load(avro_path)
    city_list_c = sc.read.option("header", True).csv(csv_path)

    # Step 3: Combine and Deduplicate the Data
    logging.info("Combining and deduplicating data")
    combined_df = city_list_a.union(city_list_b).union(city_list_c)
    total_rows = combined_df.count()
    logging.info(f"Total row count for raw files: {total_rows}")

    deduplicated_df = combined_df.dropDuplicates(["Name", "CountryCode"])

    # Step 4: Sort the Data Alphabetically by City Name
    logging.info("Sorting data alphabetically by city name")
    sorted_df = deduplicated_df.orderBy(col("Name").asc())

    # Step 5: Perform Analysis
    # 1. Count of All Rows
    total_rows = sorted_df.count()
    logging.info(f"Total row count after removing duplicates: {total_rows}")

    # 2. City with the Largest Population
    largest_city = sorted_df.orderBy(col("Population").desc()).first()
    logging.info(
        f"City with largest population: {largest_city['Name']} with population of {largest_city['Population']}")

    # 3. Total Population of All Cities in Brazil (CountryCode == BRA)
    brazil_population = sorted_df.filter(col("CountryCode") == "BRA").agg(spark_sum("Population")).collect()[0][0]
    logging.info(f"Total population of all cities in Brazil: {brazil_population}")

    # Step 6: Save the Sorted and Deduplicated Data to a CSV File
    output_path = "output/"
    logging.info(f"Saving sorted and deduplicated data to {output_path}")
    sorted_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
    logging.info("Data saved successfully")

except Exception as e:
    logging.error(f"An error occurred: {str(e)}", exc_info=True)

finally:
    # Step 7: Stop the Spark session
    try:
        sc.stop()
        logging.info("Spark session stopped")
    except Exception as e:
        logging.error(f"Failed to stop Spark session: {str(e)}", exc_info=True)

