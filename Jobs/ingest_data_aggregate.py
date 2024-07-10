from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, window, from_json, when
from pyspark.sql.types import *
import pyspark.sql.functions as fn


KAFKA_TOPIC_NAME_CONS = "users_created"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092' #'broker:29092' 

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("_id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", TimestampType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False),
        StructField("timestamp", TimestampType(), False)

    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data'))\
        .select("data.*")
    print(sel)

    return sel

def write_mongo_row(df, epoch_id):
    try:
        # Group by window and count within a 1-minute window
        gender_counts = df.groupBy(
                        window("timestamp", "1 minutes")) \
                        .agg(count(when(col("gender") == "male", 1)).alias("male_count"),
                            count(when(col("gender") == "female", 1)).alias("female_count"),
                            count("*").alias("total_count")) \
                        .orderBy("window")
        
        # Write the aggregated data to MongoDB
        # .option("spark.mongodb.write.option.replaceDocument", "true") \
        gender_counts.write.format("mongo") \
            .mode("append") \
            .option("uri", 'mongodb://mongadmin:mongadmin@localhost:27017') \
            .option("database", "UserData") \
            .option("collection", "GenderCounts") \
            .save()
        
        # Show the gender counts on the console
        gender_counts.show()
    except Exception as e:
        print(f"Error writing to MongoDB: {e}")


if __name__ == "__main__":
    mongoURL = 'mongodb://mongadmin:mongadmin@localhost:27017' #local.startup_log

    spark = SparkSession.builder\
        .appName("Real-Time Streaming Data Pipeline")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2,"
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2")\
        .config("spark.mongodb.read.connection.uri", mongoURL) \
        .config("spark.mongodb.write.connection.uri", mongoURL) \
        .getOrCreate()   
    
    liveData = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

    liveData = create_selection_df_from_kafka(liveData)

    # Write the streaming data to the console
    # Trigger every 5 Minutes
    query = liveData.writeStream \
            .trigger(processingTime="1 minutes") \
            .foreachBatch(write_mongo_row) \
            .start()
    query.awaitTermination()