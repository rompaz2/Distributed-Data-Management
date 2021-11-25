from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.mllib.evaluation import RegressionMetrics


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
kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
spark, sc = init_spark('projectB')

station_year = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
    .option("url", url) \
    .option("dbtable", "station_year_final") \
    .option("user", username) \
    .option("password", password) \
    .load()

station_year = station_year.withColumn("lat", F.col('latitude').cast(DoubleType())).drop('latitude'). \
    withColumn("lon", F.col('longitude').cast(DoubleType())).drop('longitude'). \
    withColumn("ele", F.col('elevation').cast(DoubleType())).drop('elevation'). \
    withColumn("label", F.col('avg_PRCP').cast(DoubleType())).drop('avg_PRCP'). \
    withColumn("yea", F.col('year').cast(DoubleType())).drop('year'). \
    where(station_year['year'] >= 1950). \
    where(station_year['total_count'] >= 150)


def run_GLM(station_year, features):
    assembler = VectorAssembler(
        inputCols=features,
        outputCol="features")
    features_df = assembler.transform(station_year) \
        .drop("lat", "lon", "ele", "yea", "total_sum", "total_count", "country", "stationId")

    train, test = features_df.randomSplit([0.7, 0.3])

    glr = GeneralizedLinearRegression().setFamily("gaussian").setLink("identity")

    pipeline = Pipeline().setStages([glr])
    params = ParamGridBuilder().addGrid(glr.regParam, [0, 0.5, 1]).build()

    evaluator = RegressionEvaluator() \
        .setMetricName("rmse") \
        .setPredictionCol("prediction") \
        .setLabelCol("label")

    cv = CrossValidator() \
        .setEstimator(pipeline) \
        .setEvaluator(evaluator) \
        .setEstimatorParamMaps(params) \
        .setNumFolds(10)
    cvModel = cv.fit(train)

    out = cvModel.transform(test) \
        .select("prediction", "label").rdd.map(lambda x: (float(x[0]), float(x[1])))
    metrics = RegressionMetrics(out)
    print(str(metrics.rootMeanSquaredError))


rmse_dict = {}

features1 = ["lat", "lon", "ele", "yea"]

features2 = ["lat", "lon", "ele"]

features3 = ["lat", "lon"]

features4 = ["lat"]

features5 = ["lon"]

features6 = ["lat", "lon", "yea"]


print("RMSE by feature list:")
for feat in [features1, features2, features3, features4, features5, features6]:
    print("{}: ".format(feat))
    run_GLM(station_year, feat)

