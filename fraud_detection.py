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


# schema = StructType([
#     StructField("user_id", StringType(), True),
#     StructField("event_name" , StringType(), True),
#     StructField("page" , StringType(), True),
#     StructField("item_url" , StringType(), True),
#     StructField("order_email" , StringType(), True),
# ])


# data_frame = df.withColumn(
#     'parsed_value',
#     from_json(col('decoded_value'), schema)
# ).select(
#     col("parsed_value.user_id").alias("user_id"),
#     col("parsed_value.event_name").alias("event_name"),
#     col("parsed_value.page").alias("page"),
#     col("parsed_value.item_url").alias("item_url"),
#     col("parsed_value.order_email").alias("order_email")
# )

# df = data_frame.selectExpr("to_json(struct(*)) AS value")


# # query = df.writeStream \
# #     .format("console") \
# #     .options(**kafka_options) \
# #     .option("checkpointLocation", "/tmp/kafka-checkpoints") \
# #     .start()


# # query.awaitTermination()


# add_to_cart_tracker = {}

# async def reset_dict(dict):
#     await asyncio.sleep(5)
#     dict.clear()

# async def detect_fraud(value):
#     print(value)

#     #add_to_cart_tracker["user_id"] = 1
#     #print(add_to_cart_tracker)
#     #asyncio.create_task(reset_dict(add_to_cart_tracker))
#     #await reset_dict(add_to_cart_tracker)
#     #print(add_to_cart_tracker)
#     #await asyncio.sleep(6)
#     #print(add_to_cart_tracker)

#     value_dict = json.loads(str(value))
#     print(value_dict)
#     user_id = value_dict.get('user_id')
#     event_name = value_dict.get('event_name')
#     print("Hello fraudster!!!")

#     asyncio.create_task(reset_dict(add_to_cart_tracker))
#     if event_name == "add_to_cart":
#         print("Add to cart event detected")
#         current_time = time.time()
#         if user_id not in add_to_cart_tracker:
#             add_to_cart_tracker[user_id] = [current_time]
#             print(add_to_cart_tracker)
#         else:
#             add_to_cart_tracker[user_id].append(current_time)
#             print("I got here ----- in the else")
#                 #[timestamp for timestamp in add_to_cart_tracker[event["user_id"]] if timestamp >= (current_time - 5)]

#         if len(add_to_cart_tracker[user_id]) >= 5:
#             print("THIS IS A FRAUD")
#             return True
#             # DO A FRAUD MESSAGE
            
#         return False
    


# detect_fraud_spark_udf = udf(detect_fraud, BooleanType())

# df_with_fraud = df.withColumn("is_fraud", detect_fraud_spark_udf(col("value")))

# query = df_with_fraud.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()

# try:
#     print(df['value'])
#     asyncio.run(detect_fraud(df['value']))

# except Exception as e: 
#     print(e)
#detect_fraud(df['value'])
# cleanup_thread = threading.Thread(target=reset_dict, args=(add_to_cart_tracker,), daemon=True)
# cleanup_thread.start()



schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("item_url", StringType(), True),
])

# Parse JSON
data_frame = df.withColumn(
    'parsed_value',
    from_json(col('decoded_value'), schema)
).select(
    col("parsed_value.user_id").alias("user_id"),
    col("parsed_value.event_name").alias("event_name"),
    col("parsed_value.item_url").alias("item_url")
)

add_to_cart_tracker = {}

# Kafka producer setup for publishing fraud alerts
producer_config = {
    'bootstrap.servers': "b-2-public.greencluster.jdc7ic.c3.kafka.eu-west-2.amazonaws.com:9198",
    'client.id': socket.gethostname(),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'OAUTHBEARER',
    'oauth_cb': oauth_cb,
}
producer = Producer(producer_config)

def send_fraud_alert(user_id):
    """Publishes a fraud alert message to Kafka topic 'fraud-detection'."""
    message = json.dumps({"user_id": user_id, "alert": "potential fraud detected"})
    producer.produce("fraud-detection", value=message.encode("utf-8"))
    producer.flush()
    print(f"🚨 Fraud Alert Sent for user {user_id} 🚨")



def reset_tracker():
    """Clears old items every 5 seconds."""
    global add_to_cart_tracker
    current_time = time.time()

    for user_id in list(add_to_cart_tracker.keys()):
        add_to_cart_tracker[user_id] = {
            item: timestamp for item, timestamp in add_to_cart_tracker[user_id].items()
            if timestamp >= current_time - 5  # Keep only the last 5 seconds of data
        }

        # If a user's list is empty, remove them entirely
        if not add_to_cart_tracker[user_id]:
            del add_to_cart_tracker[user_id]

    print("🔄 Tracker Reset:", add_to_cart_tracker)  # Debugging print
    threading.Timer(5, reset_tracker).start()

# Start the reset loop
reset_tracker()

def simulate_test_events():
    """Simulates multiple 'add_to_cart' events for a single user to test fraud detection."""
    test_user_id = "test-user-123"
    test_items = ["item1", "item2", "item3", "item4", "item5"]

    for i, item in enumerate(test_items):
        time.sleep(1)  # Add one item per second (modify for faster/slower tests)
        current_time = time.time()

        # Manually update tracker
        if test_user_id not in add_to_cart_tracker:
            add_to_cart_tracker[test_user_id] = {}

        add_to_cart_tracker[test_user_id][item] = current_time

        # Check if fraud should be triggered
        if len(add_to_cart_tracker[test_user_id]) >= 5:
            send_fraud_alert(test_user_id)

        print(f"🛒 Simulated: {test_user_id} added {item} at {current_time}")
        print(f"📌 Current Tracker: {add_to_cart_tracker}")

# Run the simulation in a separate thread to not block the Spark streaming job
threading.Thread(target=simulate_test_events, daemon=True).start()


def detect_fraud(batch_df, batch_id):
    """Detects fraudulent 'add_to_cart' activity in each batch."""
    global add_to_cart_tracker
    current_time = time.time()

    for row in batch_df.collect():
        user_id = row["user_id"]
        event_name = row["event_name"]
        item_url = row["item_url"]

        if event_name == "add_to_cart" and user_id and item_url:
            # Ensure the user's entry exists
            if user_id not in add_to_cart_tracker:
                add_to_cart_tracker[user_id] = {}

            # Store item with timestamp
            add_to_cart_tracker[user_id][item_url] = current_time
            print(add_to_cart_tracker)
            # Remove old items (older than 5 seconds)
            add_to_cart_tracker[user_id] = {
                item: timestamp for item, timestamp in add_to_cart_tracker[user_id].items()
                if timestamp >= current_time - 5
            }

            # Check if user added 5 different items in the last 5 seconds
            if len(add_to_cart_tracker[user_id]) >= 2:
                send_fraud_alert(user_id)

# Apply fraud detection using foreachBatch()
query = data_frame.writeStream \
    .foreachBatch(detect_fraud) \
    .start()

query.awaitTermination()
