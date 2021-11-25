import findspark
findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import pandas
import matplotlib.pyplot as plt
import numpy as np

def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2," \
                                    "com.microsoft.azure:spark-mssql-connector_2.12:1.1.0 pyspark-shell"
server_name = "jdbc:sqlserver://technionddscourse.database.windows.net"
database_name = "rompaz"
url = server_name + ";" + "database=" + database_name
username = "rompaz"
password = "Qwerty12!"

spark, sc = init_spark('projectB')

country_year = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "country_year_final") \
        .option("user", username) \
        .option("password", password) \
        .load()

station_year = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "station_year_final") \
        .option("user", username) \
        .option("password", password) \
        .load()

fig, ax = plt.subplots()
#### Spatial Analysis ####
interval = 200
elevation_prcp = station_year \
    .withColumn("elevation", F.expr("cast(elevation as integer)")) \
    .withColumn("range", F.col('elevation') - (F.col('elevation') % interval)) \
    .withColumn("from", F.col('range')) \
    .withColumn("to", F.col('range') + interval) \
    .groupBy("from", "to") \
    .agg(F.avg("avg_PRCP").alias("Average Precipitation"))\
    .orderBy("from", ascending=True)\
    .withColumn("elevation", F.expr("from || '-' || to"))\
    .select("elevation", "Average Precipitation")
elevation = elevation_prcp.toPandas()
ax = elevation.plot.bar(x='elevation', y='Average Precipitation', rot=-45)
plt.show()

#### Temporal Analysis ####
year_prcp = country_year \
    .groupby('year') \
    .agg(F.avg("avg_PRCP").alias("Average Precipitation")) \
    .orderBy('year', ascending=True)
year = year_prcp.toPandas()
ax = year.plot.bar(x='year', y='Average Precipitation', rot=45)
ax.xaxis.set_ticklabels([])
plt.show()

#### Spatio-temporal Analysis ####
country1 = "CA"
country2 = "TZ"
country_year_prcp = country_year \
    .where(country_year['year'] >= 1990) \
    .orderBy('country', 'year', ascending=True)
country1_df = country_year_prcp.where(country_year_prcp["country"] == country1).drop("country").toPandas()
country2_df = country_year_prcp.where(country_year_prcp["country"] == country2).drop("country").toPandas()
ax = country1_df.plot.line(x='year', y='avg_PRCP', rot=45, title=f"{country1} PRCP", yticks=np.arange(0, 100, 20))
plt.show()
ax = country2_df.plot.line(x='year', y='avg_PRCP', rot=45, title=f"{country2} PRCP", color="red")
plt.show()