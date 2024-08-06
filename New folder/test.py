from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, IntegerType, DoubleType, StructField

spark = SparkSession.builder \
    .appName("HealthcareDataAnalysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.broadcastTimeout", "36000") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


df = spark.read.json(r"G:\HEALTHCARE PROJECT\final\New folder\CIGNA(INSURANCE_CLAIM_DATA).json",multiLine=True)
df.printSchema()
df.show()

#
# df = spark.read.json(r"C:\Users\SAI\Documents\CIGNA(INSURANCE_CLAIM_DATA) (keyed array).json",multiLine=True)
# df.printSchema()
# df.show()


df = spark.read.json(r"C:\Users\SAI\Documents\CIGNA(INSURANCE_CLAIM_DATA) (jsonarray).json",multiLine=True)
df.printSchema()
df.show()


# df = spark.read.json(r"C:\Users\SAI\Documents\CIGNA(INSURANCE_CLAIM_DATA) (jsoncolarray).json")
# df.printSchema()
# df.show()

