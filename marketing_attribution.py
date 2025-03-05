from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import col, from_json, expr, lit
from pyspark.sql.functions import udf
import psycopg2
import json
import time
import threading
import socket
from confluent_kafka import Producer

def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("eu-west-2")
    return auth_token, expiry_ms/1000

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,software.amazon.msk:aws-msk-iam-auth:2.2.0') \
    .getOrCreate()

kafka_options = {
    "kafka.bootstrap.servers": "b-2-public.greencluster.jdc7ic.c3.kafka.eu-west-2.amazonaws.com:9198",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": """software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName="";""",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "startingOffsets": "latest",
    "subscribe": "events"
}

df = spark.readStream.format("kafka").options(**kafka_options).load()
df = df.withColumn('decoded_value', col('value').cast('string'))


schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("channel", StringType(), True)
])

# Parse JSON
data_frame = df.withColumn(
    'parsed_value',
    from_json(col('decoded_value'), schema)
).select(
    col("parsed_value.user_id").alias("user_id"),
    col("parsed_value.order_id").alias("order_id")
    col("parsed_value.event_name").alias("event_name"),
    col("parsed_value.channel").alias("channel")
)

user_tracker = {}

def track_marketing_channel(batch_df, batch_id):
    for row in batch_df.collect():
        user_id = row["user_id"]
        order_id = row["order_id"]
        event_name = row["event_name"]
        channel = row["channel"]
        if event_name == 'visit':
            if user_id not in user_tracker:
                if channel:
                    user_tracker[user_id] = channel 
                else:
                    user_tracker[user_id] ='organic'
                print(user_tracker)
        elif event_name == 'order_confirmed':
            channel = user_tracker.get(user_id)
        store_attribution(user_id, order_id, channel)


query = data_frame.writeStream \
    .foreachBatch(track_marketing_channel) \
    .start()

query.awaitTermination()


def store_attribution(user_id, order_id, channel):
    """
    Stores the marketing attribution data into the PostgreSQL database.
    """
    conn = psycopg2.connect(
        dbname="green-analytics-db",
        user="postgres",
        password="i_am_a_password",
        host="green-analytics-db.cfmnnswnfhpn.eu-west-2.rds.amazonaws.com",
        port="5432"
    )

    cursor = conn.cursor()

    insert_query = """
    INSERT INTO purchase_marketing_attributions (user_id, order_id, marketing_channel)
    VALUES (%s, %s, %s)
    """
    cursor.execute(insert_query, (user_id, order_id, channel))
    conn.commit()
    cursor.close()
    conn.close()


# conn = psycopg2.connect( dbname="analytics_db", user="admin", password="password123", host="your-database-host", port="5432" ) 

# cursor = conn.cursor()

