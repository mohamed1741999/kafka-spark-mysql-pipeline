from datetime import datetime, timedelta
import json
import requests
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'my_topic'

# MySQL configuration
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'gamal'
MYSQL_PASSWORD = 'Password1234'
MYSQL_DATABASE = 'airflow'
MYSQL_TABLE = 'processed_data'

# API configuration
API_URL = 'https://jsonplaceholder.typicode.com/posts'


producer_config = {
        'bootstrap.servers': KAFKA_BROKER  
    }

producer = Producer(producer_config)
    
response = requests.get(API_URL)
response.raise_for_status()
data = response.json()

for record in data:
    producer.produce(KAFKA_TOPIC, key=str(record.get('id')), value=json.dumps(record))
    producer.flush()  


''''.config ("spark.master", 'spark://172.29.196.3:7077') \
    .config ("spark.executor.memory","2g") \
    .config ("spark.executor.cores","2") \
    .config ("spark.cores.max","2") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryo.registrationRequired", "false") \''''
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.master", "local[*]") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.jars", "/opt/spark/jars/mysql-connector-java-8.0.29.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")
spark.catalog.clearCache()


schema = StructType([
    StructField("userId", StringType(), True),
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("body", StringType(), True)
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()



json_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

processed_df = json_df.withColumn("processed_value", col("body"))


def write_to_mysql(batch_df, batch_id):
    print(f"Writing batch {batch_id} to MySQL")
    print(f"Batch data: {batch_df.count()} rows")
    batch_df.show()  
    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", MYSQL_TABLE) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .mode("append") \
        .save()
    
query = processed_df.writeStream \
        .foreachBatch(write_to_mysql) \
        .outputMode("update") \
        .trigger(once=True) \
        .start()

query.awaitTermination()




