from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
from pyspark.sql.types import *


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4," \
                                    "com.microsoft.azure:spark-mssql-connector_2.11:1.1.0 pyspark-shell"
server_name = "jdbc:sqlserver://technionddscourse.database.windows.net"
database_name = "rompaz"
url = server_name + ";" + "database=" + database_name
username = "rompaz"
password = "Qwerty12!"

spark, sc = init_spark('projectB')
# transforming stations text file to spark dataFrame
stations = spark.read.text("ghcnd-stations.txt")
stations = stations.select(stations.value.substr(0, 11).alias("StationId"),
                           stations.value.substr(13, 8).alias("latitude"),
                           stations.value.substr(22, 8).alias("longitude"),
                           stations.value.substr(32, 6).alias("elevation"))

kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
noaa_schema = StructType([StructField('StationId', StringType(), False),
                            StructField('Date', IntegerType(), False),
                            StructField('Variable', StringType(), False),
                            StructField('Value', IntegerType(), False),
                            StructField('M_Flag', StringType(), True),
                            StructField('Q_Flag', StringType(), True),
                            StructField('S_Flag', StringType(), True),
                            StructField('ObsTime', StringType(), True)])

raw_stream_df = spark.readStream\
                  .format("kafka")\
                  .option("kafka.bootstrap.servers", kafka_server)\
                  .option("subscribe", "CA, TZ")\
                  .option("startingOffsets", "earliest")\
                  .option("maxOffsetsPerTrigger", "1000000")\
                  .load()

string_value_df = raw_stream_df.selectExpr("CAST(value AS STRING)")
json_df = string_value_df.select(F.from_json(F.col("value"), schema=noaa_schema).alias('json'))
streaming_df = json_df.select("json.*")
# transforming stream and partition it
streaming_df = streaming_df\
    .where(streaming_df['Variable'] == 'PRCP')\
    .where(streaming_df['Q_Flag'].isNull()) \
    .withColumn("year", F.year(F.to_date(F.concat(F.col("date")), "yyyyMMdd")))\
    .withColumn("country", F.substring(F.col("stationId"), 0, 2))\
    .repartition("country", "StationId", "year")
# setting global counter
counter = 0


# this function makes the final aggregation after being called when global counter reaches its wanted value
def final_transformation():
    # loading all the tables containing batches aggregations
    station_year = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", "station_year") \
        .option("user", username) \
        .option("password", password) \
        .load()
    country_year = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("dbtable", "country_year") \
        .option("user", username) \
        .option("password", password) \
        .load()
    # making final aggregations
    station_year = station_year.groupby("country", "StationId", "year", 'latitude', 'longitude', 'elevation') \
        .agg(F.sum("PRCP_sum").alias("total_sum"), F.sum("PRCP_count").alias("total_count"))
    station_year = station_year.withColumn("avg_PRCP", F.col("total_sum") / F.col("total_count"))
    country_year = country_year.groupby("country", "year") \
        .agg(F.sum("PRCP_sum").alias("total_sum"), F.sum("PRCP_count").alias("total_count"))
    country_year = country_year.withColumn("avg_PRCP", F.col("total_sum") / F.col("total_count"))
    # writing the final tables back to the server
    station_year.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", "station_year_final") \
        .option("user", username) \
        .option("password", password) \
        .save()
    country_year.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", "country_year_final") \
        .option("user", username) \
        .option("password", password) \
        .save()


def handle_batch(df, id):
    global counter
    counter += 1

    cur_mode = "overwrite" if id == 0 else "append"
    station_year = df.select("country", "StationId", "year", 'Value')\
        .groupby("country", "StationId", "year") \
        .agg(F.sum('Value').alias("PRCP_sum"), F.count('Value').alias("PRCP_count"))\
        .join(stations, on=['StationId'])

    country_year = df.select('country', 'year', 'value')\
        .groupby('country', 'year') \
        .agg(F.sum('Value').alias("PRCP_sum"), F.count('Value').alias("PRCP_count"))

    station_year.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode(cur_mode) \
        .option("url", url) \
        .option("dbtable", "station_year") \
        .option("user", username) \
        .option("password", password) \
        .save()

    country_year.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode(cur_mode) \
        .option("url", url) \
        .option("dbtable", "country_year") \
        .option("user", username) \
        .option("password", password) \
        .save()

    if counter >= 100:
        final_transformation()
        query.stop()


query = streaming_df \
    .writeStream \
    .foreachBatch(handle_batch)\
    .trigger(processingTime='1 second')\
    .start()

query.awaitTermination()




