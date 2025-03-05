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

query = df.writeStream \
    .format("console") \
    .options(**kafka_options) \
    .option("checkpointLocation", "/tmp/kafka-checkpoints") \
    .start()



# check if event has event_name of 'purchase'
# if not, continue
# if so, search event stream fo first instance of user_id
# check instance for marketing channel
# if no marketing channel
# save record with markeitng channel as user ID, order ID, 'organic'
# if marketing channel
# save record with markeitng channel as user ID, order ID, marketing_channel_name
