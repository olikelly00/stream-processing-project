from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json
import json
import time
import threading
import socket
import os
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("eu-west-2")
    return auth_token, expiry_ms/1000


spark = SparkSession.builder \
    .appName("FraudDetection") \
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

df = spark.readStream.format("kafka").options(**kafka_options).load()
df = df.withColumn('decoded_value', col('value').cast('string'))

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
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVER"),
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
