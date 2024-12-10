
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp
from configs import SILVER, GOLD


def transform_silver_to_gold(spark):
    athlete_bio_path = f"{SILVER}/athletes_bio"
    athlete_event_results_path = f"{SILVER}/athlete_event_results"
    gold_table_path = f"{GOLD}/avg_stats"

    athlete_bio_df = spark.read.parquet(athlete_bio_path)
    athlete_event_results_df = spark.read.parquet(athlete_event_results_path)

    athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")
    athlete_event_results_df = athlete_event_results_df.withColumnRenamed("country_noc", "event_country_noc")

    joined_df = athlete_bio_df.join(
        athlete_event_results_df,
        on="athlete_id",
        how="inner"
    )
    joined_df = joined_df.withColumn("country_noc", col("bio_country_noc")).drop("bio_country_noc", "event_country_noc")
    aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
        avg("weight").alias("avg_weight"),
        avg("height").alias("avg_height")
    ).withColumn("timestamp", current_timestamp())
    os.makedirs(os.path.dirname(gold_table_path), exist_ok=True)
    aggregated_df.write.parquet(gold_table_path, mode="overwrite")
    print(f"Data written to: {gold_table_path}")
    aggregated_df.show()


def main():
    spark = SparkSession.builder \
        .appName("Silver to Gold") \
        .getOrCreate()
    transform_silver_to_gold(spark)


if __name__ == "__main__":
    main()
