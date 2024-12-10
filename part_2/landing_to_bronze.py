import os
from pyspark.sql import SparkSession
from utils import download_data
from configs import BRONZE

ATHLETES_FILE_NAME = "athlete_bio.csv"
ATHLETES_EVENTS_FILE_NAME = "athlete_event_results.csv"


def write_csv_to_parquet(csv_file_name, output_path):
    spark = SparkSession.builder \
        .appName("CSV to Parquet Conversion") \
        .getOrCreate()
    df = spark.read.csv(csv_file_name, header=True, inferSchema=True)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write.parquet(output_path, mode="overwrite")
    print(f"CSV file '{csv_file_name}' converted to Parquet format: {output_path}")
    df.show()


def main():
    download_data(ATHLETES_FILE_NAME)
    download_data(ATHLETES_EVENTS_FILE_NAME)
    write_csv_to_parquet(ATHLETES_FILE_NAME, f"{BRONZE}/athletes_bio")
    write_csv_to_parquet(ATHLETES_EVENTS_FILE_NAME, f"{BRONZE}/athlete_event_results")


if __name__ == "__main__":
    main()
