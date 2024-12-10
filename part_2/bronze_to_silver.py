import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
from configs import BRONZE, SILVER


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


clean_text_udf = udf(clean_text, StringType())


def transform_bronze_to_silver(spark, table_name):
    bronze_table_path = f"{BRONZE}/{table_name}"
    silver_table_path = f"{SILVER}/{table_name}"
    df = spark.read.parquet(bronze_table_path)
    text_columns = [field.name for field in df.schema.fields if str(field.dataType) == "StringType"]
    for col_name in text_columns:
        df = df.withColumn(col_name, clean_text_udf(col(col_name)))
    df = df.dropDuplicates()
    os.makedirs(os.path.dirname(silver_table_path), exist_ok=True)
    df.write.parquet(silver_table_path, mode="overwrite")
    print(f"Bronze table '{table_name}' transformed to Silver: {silver_table_path}")
    df.show()


def main():
    spark = SparkSession.builder \
        .appName("Bronze to Silver") \
        .getOrCreate()
    transform_bronze_to_silver(spark, "athletes_bio")
    transform_bronze_to_silver(spark, "athlete_event_results")


if __name__ == "__main__":
    main()
