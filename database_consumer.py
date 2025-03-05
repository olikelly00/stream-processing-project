from confluent_kafka import Consumer, KafkaException
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, expr, lit
import socket
import json

def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token("eu-west-2")
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch, while the token generator returns expiry in ms
    return auth_token, expiry_ms/1000


spark = SparkSession.builder \
    .appName("DatabaseConsumer") \
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

query = data_frame.writeStream \
    .format("parquet") \
    .option("path", "/tmp/stream_output/") \
    .option("checkpointLocation", "/tmp/stream_checkpoint/") \
    .outputMode("append") \
    .start()

query.awaitTermination()

batch_df = spark.read.parquet("/tmp/stream_output/")


batch_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://green-analytics-db.cfmnnswnfhpn.eu-west-2.rds.amazonaws.com:5432/green_analytics") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "events") \
    .option("user", "postgres") \
    .option("password", "i_am_a_password") \
    .mode("append") \
    .save()



