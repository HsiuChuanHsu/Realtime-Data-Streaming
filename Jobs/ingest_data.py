from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as fn


KAFKA_TOPIC_NAME_CONS = "users_created"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092' #'broker:29092' 

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

if __name__ == "__main__":
    mongoURL = 'mongodb://mongadmin:mongadmin@localhost:27017/UserData.User'

    spark = SparkSession.builder\
        .appName("Real-Time Streaming Data Pipeline")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2,"
                "org.mongodb.spark:mongo-spark-connector_2.13:10.3.0")\
        .config("spark.mongodb.read.connection.uri", mongoURL) \
        .config("spark.mongodb.write.connection.uri", mongoURL) \
        .getOrCreate()
        
        

        #"org.mongodb.spark:mongo-spark-connector_2.11:2.41")\
        # .config("spark.mongodb.read.connection.uri", mongoURL) \
        # .config("spark.mongodb.write.connection.uri", mongoURL) \
        # "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
        # .master("local[*]")\
    
    liveData = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "earliest") \
        .load()
    # liveData = liveData.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    liveData = create_selection_df_from_kafka(liveData)


    
    # Write the streaming data to the console
    # query = liveData.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()

    def write_mongo_row(df, epoch_id):
        df.write.format("com.mongodb.spark.sql.DefaultSource")\
            .mode("append")\
            .option("uri", mongoURL) \
            .save()
        pass

    query = liveData.writeStream.foreachBatch(write_mongo_row).start()
    
    query.awaitTermination()