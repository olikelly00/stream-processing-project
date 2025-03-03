from confluent_kafka import Consumer, KafkaException
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, expr
import socket
import json

def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("eu-west-2")
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    return auth_token, expiry_ms/1000

consumer = Consumer({
    # "debug": "all",
    'bootstrap.servers': 'b-2.greencluster.jdc7ic.c3.kafka.eu-west-2.amazonaws.com:9098',
    'client.id': socket.gethostname(),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': oauth_cb,
    'group.id': 'event-group-id',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['events'])

# try:
#     while True:
        # msg = consumer.poll(1.0)  # Poll for messages (1 second timeout)

        # if msg is None:
        #     continue  # No new messages, keep polling
        
        # if msg.error():
        #     raise KafkaException(msg.error())

        # # Print raw message content for debugging
        # raw_message = msg.value().decode('utf-8')
        # print(f"Raw message: {raw_message}")




# except json.JSONDecodeError as e:
#     print(f"Error decoding JSON: {e}")

kafka_options = {
    "kafka.bootstrap.servers": "b-2-public.greencluster.jdc7ic.c3.kafka.eu-west-2.amazonaws.com:9198",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": """software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName="";""",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "startingOffsets": "latest",
    "subscribe": "events"
}

spark = SparkSession.builder \
    .appName("KafkaEventAnonymizer") \
    .getOrCreate()


df = spark.readStream.format("kafka").options(**kafka_options).load()
df = df.withColumn('decoded_value', col('value').cast('string'))


schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_name" , StringType(), True),
    StructField("page" , StringType(), True),
    StructField("item_url" , StringType(), True),
    StructField("order_email" , StringType(), True),
])


data_frame = df.withColumn(
    'parsed_value',
    from_json(col('decoded_value'), schema)
).select(
    col("parsed_value.user_id").alias("user_id"),
    col("parsed_value.event_name").alias("event_name"),
    col("parsed_value.page").alias("page"),
    col("parsed_value.item_url").alias("item_url"),
    col("parsed_value.order_email").alias("order_email")
)

print("redacting df")
data_frame = data_frame.withcolumn("order_email", expr("******"))
print("df redacted")

print("writing query")
query = data_frame.writeStream \
    .format("console") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint-raw") \
    .start()
print("query written")


