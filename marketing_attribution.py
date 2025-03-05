from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import col, from_json, expr, lit
from pyspark.sql.functions import udf
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
    StructField("event_name", StringType(), True),
    StructField("channel", StringType(), True)
])

# Parse JSON
data_frame = df.withColumn(
    'parsed_value',
    from_json(col('decoded_value'), schema)
).select(
    col("parsed_value.user_id").alias("user_id"),
    col("parsed_value.event_name").alias("event_name"),
    col("parsed_value.channel").alias("channel")
)

# query = df.writeStream \
#     .format("console") \
#     .options(**kafka_options) \
#     .option("checkpointLocation", "/tmp/kafka-checkpoints") \
#     .start()

# query.awaitTermination()


# if not, continue
# if so, search event stream fo first instance of user_id
# check instance for marketing channel
# if no marketing channel
# save record with markeitng channel as user ID, order ID, 'organic'
# if marketing channel
# save record with markeitng channel as user ID, order ID, marketing_channel_name

user_tracker = {}

def track_marketing_channel(batch_df):
    for row in batch_df.collect():
        user_id = row["user_id"]
        event_name = row["event_name"]
        channel = row["channel"]
        if event_name == 'visit':
            if user_id not in user_tracker:
                user_tracker[user_id] = channel if channel else 'organic'
        elif event_name == 'order_confirmed':
            channel = user_tracker.get(user_id)
    print("CHANNEL:", channel)
    return channel

            
track_marketing_channel(data_frame)
        

# for every event
# if event_name = 'visit'
# if user_id not in user_tracker
# add user_id, channel if exists to tracker else organic

# elif event_name = 'order_confirmed'
# take user_id
# search user_tracker for user id
# return user_id['channel']









        # if event_name == "order_confirmed":
        #     target_user = row["user_id"]
        #     # if so, search event stream fo first instance of user_id
        #     first_instance_user_event = batch_df[]

        #     if user_id not in add_to_cart_tracker:
        #         add_to_cart_tracker[user_id] = {}

        #     # Store item with timestamp
        #     add_to_cart_tracker[user_id][item_url] = current_time
        #     print(add_to_cart_tracker)
        #     # Remove old items (older than 5 seconds)
        #     add_to_cart_tracker[user_id] = {
        #         item: timestamp for item, timestamp in add_to_cart_tracker[user_id].items()
        #         if timestamp >= current_time - 5
        #     }

        #     # Check if user added 5 different items in the last 5 seconds
        #     if len(add_to_cart_tracker[user_id]) >= 2:
        #         send_fraud_alert(user_id)

