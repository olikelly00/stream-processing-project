from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import col, from_json, expr, lit
from pyspark.sql.functions import udf
import json

import time
import threading

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

df = data_frame.selectExpr("to_json(struct(*)) AS value")


# query = df.writeStream \
#     .format("console") \
#     .options(**kafka_options) \
#     .option("checkpointLocation", "/tmp/kafka-checkpoints") \
#     .start()


# query.awaitTermination()


add_to_cart_tracker = {}

def reset_dict(dict):
    while True:
        time.sleep(5)
        dict.clear()

print("HEY")

print("HEY2")

def detect_fraud(value):
    print(value)
    value_dict = json.loads(str(value))
    print(value_dict)
    user_id = value_dict.get('user_id')
    event_name = value_dict.get('event_name')
    print("Hello fraudster!!!")
    if event_name == "add_to_cart":
        print("Add to cart event detected")
        current_time = time.time()
        if user_id not in add_to_cart_tracker:
            add_to_cart_tracker[user_id] = [current_time]
            print(add_to_cart_tracker)
        else:
            add_to_cart_tracker[user_id].append(current_time)
            print("I got here ----- in the else")
                #[timestamp for timestamp in add_to_cart_tracker[event["user_id"]] if timestamp >= (current_time - 5)]

        if len(add_to_cart_tracker[user_id]) >= 5:
            print("THIS IS A FRAUD")
            return True
            # DO A FRAUD MESSAGE
            
        return False
    


detect_fraud_spark_udf = udf(detect_fraud, BooleanType())

df = df.withColumn("is_fraud", detect_fraud_spark_udf(col("value")))




detect_fraud(df['value'])
cleanup_thread = threading.Thread(target=reset_dict, args=(add_to_cart_tracker,), daemon=True)
cleanup_thread.start()



