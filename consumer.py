from confluent_kafka import Consumer, KafkaException
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, expr, lit
import socket
import json
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("eu-west-2")
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    return auth_token, expiry_ms/1000


spark = SparkSession.builder \
    .appName("EventAnonymiser") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,software.amazon.msk:aws-msk-iam-auth:2.2.0') \
    .getOrCreate()



kafka_options = {
    "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVER"),
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": os.getenv("KAFKA_SASL_JAAS_CONFIG"),
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "startingOffsets": "latest",
    "subscribe": "events"
}


kafka_options_processed_events = {
    "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVER"),
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": os.getenv("KAFKA_SASL_JAAS_CONFIG"),
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "startingOffsets": "latest"
}

df = spark.readStream.format("kafka").options(**kafka_options).load()
df = df.withColumn('decoded_value', col('value').cast('string'))


schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_name" , StringType(), True),
    StructField("page" , StringType(), True),
    StructField("item_url" , StringType(), True),
    StructField("order_email" , StringType(), True),
    StructField("channel", StringType(), True)
])


data_frame = df.withColumn(
    'parsed_value',
    from_json(col('decoded_value'), schema)
).select(
    col("parsed_value.user_id").alias("user_id"),
    col("parsed_value.event_name").alias("event_name"),
    col("parsed_value.page").alias("page"),
    col("parsed_value.item_url").alias("item_url"),
    col("parsed_value.order_email").alias("order_email"),
    col("parsed_value.channel").alias("channel")
)
data_frame = data_frame.withColumn("order_email", lit("[Redacted]"))


conn = psycopg2.connect(
        dbname=os.getenv("ANALYTICAL_DB_NAME"),
        user=os.getenv("ANALYTICAL_DB_USER"),
        password=os.getenv("ANALYTICAL_DB_PASSWORD"),
        host=os.getenv("ANALYTICAL_DB_HOST"),
        port=os.getenv("ANALYTICAL_DB_PORT"),
    )

cursor = conn.cursor()

select_user_data = """
SELECT id, birthdate, country_code, web_user_agent FROM users;
"""
cursor.execute(select_user_data)
rows = cursor.fetchall()

columns = ["id", "birthdate", "country_code", "web_user_agent"]
user_df = spark.createDataFrame(rows, columns)


master_df = user_df.join(data_frame, user_df.id == data_frame.user_id).select(data_frame["user_id"], user_df["birthdate"], user_df["country_code"], user_df["web_user_agent"], data_frame["event_name"], data_frame["page"], data_frame["order_email"])

df = master_df.selectExpr("to_json(struct(*)) AS value")


query = df.writeStream \
    .format("kafka") \
    .options(**kafka_options_processed_events) \
    .option("topic", "processed-events") \
    .option("checkpointLocation", "/tmp/kafka-checkpoints") \
    .start()


query.awaitTermination()

