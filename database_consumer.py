from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,software.amazon.msk:aws-msk-iam-auth:2.2.0,org.postgresql:postgresql:42.5.0') \
    .getOrCreate()

# Kafka connection settings
kafka_options = {
    "kafka.bootstrap.servers": "b-2-public.greencluster.jdc7ic.c3.kafka.eu-west-2.amazonaws.com:9198",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": """software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName="";""",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "startingOffsets": "latest",
    "subscribe": "events"
}

# Read from Kafka
df = spark.readStream.format("kafka").options(**kafka_options).load()
df = df.withColumn('decoded_value', col('value').cast('string'))

# Define JSON schema
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("page", StringType(), True),
    StructField("item_url", StringType(), True),
    StructField("order_email", StringType(), True),
    StructField("channel", StringType(), True)
])

# Parse JSON data
data_frame = df.withColumn(
    'parsed_value', from_json(col('decoded_value'), schema)
).select(
    col("parsed_value.user_id").alias("user_id"),
    col("parsed_value.event_name").alias("event_name"),
    col("parsed_value.page").alias("page"),
    col("parsed_value.item_url").alias("item_url"),
    col("parsed_value.order_email").alias("order_email"),
    col("parsed_value.channel").alias("channel")
)

# Function to write to PostgreSQL in batches
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://green-analytics-db.cfmnnswnfhpn.eu-west-2.rds.amazonaws.com:5432/green_analytics") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "events") \
        .option("user", "postgres") \
        .option("password", "i_am_a_password") \
        .mode("append") \
        .save()

# Stream data and write to PostgreSQL
query = data_frame.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
