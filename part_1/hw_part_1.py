import os
import json
import uuid
from pyspark.sql import SparkSession
from kafka import KafkaProducer
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from configs import kafka_config, jdbc_config, ATHLETE_TOPIC_NAME, OUTPUT_TOPIC_NAME

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'
JDBC_TABLE = "athlete_bio"

# 1. Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio (база даних і Credentials до неї вам будуть надані).
spark = SparkSession.builder.config("spark.jars", "mysql-connector-j-8.0.32.jar").appName("JDBCToKafka").getOrCreate()
df1 = spark.read.format('jdbc').options(url=jdbc_config["url"],
                                       driver='com.mysql.cj.jdbc.Driver',
                                       dbtable=JDBC_TABLE,
                                       user=jdbc_config["username"],
                                       password=jdbc_config["password"]).load()

# 2. Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами. Можна це зробити на будь-якому етапі вашої програми.
mysql_df = df1.filter(
    (col("height").isNotNull()) & (col("height") != "") &
    (col("weight").isNotNull()) & (col("weight") != "")
)

mysql_df.show()

# 3. Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results. Дані з json-формату необхідно перевести в dataframe-формат, де кожне поле json є окремою колонкою.
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .config("spark.sql.debug.maxToStringFields", "200")
         .config("spark.sql.columnNameLengthThreshold", "200")
         .getOrCreate())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", ATHLETE_TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "300") \
    .load()

json_schema = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("athlete", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("event", StringType(), True)
])

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.*")

# 4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.
joined_df = json_df.join(mysql_df, json_df.athlete_id == mysql_df.athlete_id, "inner")

# 5. Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту, типу медалі або її відсутності, статі, країни (country_noc). Додайте також timestamp, коли розрахунки були зроблені.
aggregated_df = joined_df.groupBy(
    "sport", "medal", "sex", "country_noc"
).agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp")
).withColumn(
    "timestamp", date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

# 6. Зробіть стрим даних (за допомогою функції forEachBatch) у:
# а) вихідний кафка-топік,
# b) базу даних.
def write_to_kafka(df: DataFrame, epoch_id: int):
    producer = KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for row in df.collect():
        producer.send(OUTPUT_TOPIC_NAME, value=row.asDict(), key=str(uuid.uuid4()))
    producer.flush()


def write_to_database(df: DataFrame, epoch_id: int):
    db_properties = {
        "user": jdbc_config["username"],
        "password": jdbc_config["password"],
        "driver": jdbc_config["driver"]
    }
    df.write.jdbc(
        url=jdbc_config["my_url"],
        table=jdbc_config["athlete_enriched_results"],
        mode="append",
        properties=db_properties
    )


query_to_kafka = aggregated_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_kafka) \
    .start()


query_to_db = aggregated_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_database) \
    .start()

query_to_kafka.awaitTermination()
query_to_db.awaitTermination()
