from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType,StructType,IntegerType,DoubleType,StructField

spark = SparkSession.builder.master("local[*]").getOrCreate()

#
# df = spark.read.json(r"D:\FINALPROJECTS\random_pract\claims.json")
# df.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark session
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Define schema
schema1 = StructType([
    StructField("patientid", StringType(), True),
    StructField("patient_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("p_age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("patient_address", StringType(), True),
    StructField("health_insurance_id", StringType(), True),
    StructField("hospital_id", StringType(), True),
    StructField("Hospital_address", StringType(), True),
    StructField("physician_id", StringType(), True),
    StructField("Speciality", StringType(), True),
    StructField("INSURANCE_provider_name", StringType(), True),
    StructField("insurance_provider_id", StringType(), True),
    StructField("DISEASE", StringType(), True),
    StructField("SUB_DISEASE", StringType(), True),
    StructField("DISEASE_CODE", StringType(), True),
    StructField("DIAGNOSIS/TREATMENT", StringType(), True),
    StructField("medicine_names", StringType(), True),
    StructField("medical_bill_AMOUNT", IntegerType(), True),
    StructField("hospital_bill_AMOUNT", IntegerType(), True),
    StructField("INSURANCE_PLAN", StringType(), True),
    StructField("INSURANCE_PREMIUM", IntegerType(), True),
    StructField("insurance_start_date", StringType(), True),
    StructField("insurance_end_date", StringType(), True),
    StructField("claim_id", StringType(), True),
    StructField("claim_amount", IntegerType(), True),
    StructField("deductible", IntegerType(), True),
    StructField("copay", IntegerType(), True),
    StructField("COINSURANCE", DoubleType(), True),
    StructField("outofpocketmax", DoubleType(), True),
    StructField("claim_amount_settled", DoubleType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_id", StringType(), True)
])

# Load the data
df20 = spark.read.csv(r"D:\FINALPROJECTS\New folder\merged_data\merged_data_final - Copy.csv", header=True, schema=schema1)

# Print schema
df20.printSchema()

# Calculate mean and standard deviation for claim amounts
claim_stats = df20.agg(
    mean("claim_amount_settled").alias("mean_claim"),
    stddev("claim_amount_settled").alias("stddev_claim")
).collect()

mean_claim = claim_stats[0]["mean_claim"]
stddev_claim = claim_stats[0]["stddev_claim"]

print("Mean claim is :-",mean_claim)
print("stddev claim is :-",stddev_claim)

# Apply 2-sigma and 3-sigma rules to identify outliers
threshold_high_2sigma = mean_claim + 2 * stddev_claim
threshold_low_2sigma = mean_claim - 2 * stddev_claim
threshold_high_3sigma = mean_claim + 3 * stddev_claim
threshold_low_3sigma = mean_claim - 3 * stddev_claim

print("threshold_high_2sigmas :-",threshold_high_2sigma)
print("threshold_high_3sigma :-",threshold_high_3sigma)

# Filter outliers
outliers_2sigma = df20.filter((col("claim_amount_settled") > threshold_high_2sigma))
outliers_3sigma = df20.filter((col("claim_amount_settled") > threshold_high_3sigma))

print("Outlier 2sigma",outliers_2sigma.count())
print("Outlier 3sigma",outliers_3sigma.count())

outliers_2sigma.show()
outliers_3sigma.show()

print("############################################################################################")



# Calculate mean and standard deviation for claim amounts
claim_stats = df20.filter(col("disease")=="Cancer").agg(
    mean("claim_amount_settled").alias("mean_claim"),
    stddev("claim_amount_settled").alias("stddev_claim")
).collect()

mean_claim_cancer = claim_stats[0]["mean_claim"]
stddev_claim_cancer = claim_stats[0]["stddev_claim"]

print("Mean claim is cancer :-",mean_claim_cancer)
print("stddev claim is cancer :-",stddev_claim_cancer)

# Apply 2-sigma and 3-sigma rules to identify outliers
threshold_high_2sigma_cancer = mean_claim_cancer + 2 * stddev_claim_cancer

threshold_high_3sigma_cancer = mean_claim_cancer + 3 * stddev_claim_cancer

print("threshold_high_2sigmas cancer :-",threshold_high_2sigma_cancer)
print("threshold_high_3sigma cancer :-",threshold_high_3sigma_cancer)

# Filter outliers
outliers_2sigma_cancer = df20.filter((col("claim_amount_settled") > threshold_high_2sigma_cancer))
outliers_3sigma_cancer = df20.filter((col("claim_amount_settled") > threshold_high_3sigma_cancer))

print("Outlier 2sigma_cancer",outliers_2sigma_cancer.count())
print("Outlier 3sigma_cancer",outliers_3sigma_cancer.count())


print("##############################################   CANCER ##############################################")
frequent_claim_threshold = 2

fraudulent_claims = df20.withColumn("high_value_claim", when(df20.claim_amount_settled > threshold_high_2sigma_cancer, "Yes").otherwise("No"))

# Identifying frequent claims

frequent_claims = df20.groupBy("hospital_id").count().withColumnRenamed("count", "claim_count")

frequent_claims = frequent_claims.withColumn("frequent_claims", when(col("claim_count") > frequent_claim_threshold, "Yes").otherwise("No"))

# Join datasets to get potentially fraudulent claims
fraudulent_claims = fraudulent_claims.join(frequent_claims, on="hospital_id", how="left").filter((col("high_value_claim") == "Yes") | (col("frequent_claims") == "Yes"))

# Order by high_value_claim in descending order
fraudulent_claims = fraudulent_claims.filter(col("disease")=="Cancer").orderBy(col("high_value_claim").desc())

fraudulent_claims = fraudulent_claims.select("hospital_id","claim_id","insurance_provider_id","claim_amount_settled","high_value_claim","claim_count","frequent_claims")

fraudulent_claims.show()


print("Count of claims which are greater than threshold cancer claim amount :-",fraudulent_claims.count())


frauds = fraudulent_claims.withColumn("fraud_detected", when(((col("high_value_claim") == "Yes") & (col("frequent_claims") == "Yes")), "Yes").otherwise("No"))

frauds = frauds.select("hospital_id","claim_id","insurance_provider_id","claim_amount_settled","high_value_claim","claim_count","frequent_claims",col("fraud_detected"))

frauds = frauds.filter((col("high_value_claim") == "Yes") & (col("frequent_claims") == "Yes"))

frauds.show()

print("Count of fraud claims are :-",frauds.count())

print("################################################ DIABETES  ############################################")




# Calculate mean and standard deviation for claim amounts
claim_stats = df20.filter(col("disease")=="Diabetes").agg(
    mean("claim_amount_settled").alias("mean_claim"),
    stddev("claim_amount_settled").alias("stddev_claim")
).collect()

mean_claim_Diabetes = claim_stats[0]["mean_claim"]
stddev_claim_Diabetes = claim_stats[0]["stddev_claim"]

print("Mean claim Diabetes :-",mean_claim_Diabetes)
print("stddev claim Diabetes :-",stddev_claim_Diabetes)

# Apply 2-sigma and 3-sigma rules to identify outliers
threshold_high_2sigma_Diabetes = mean_claim_Diabetes + 2 * stddev_claim_Diabetes

threshold_high_3sigma_Diabetes = mean_claim_Diabetes + 3 * stddev_claim_Diabetes

print("threshold_high_2sigmas Diabetes :-",threshold_high_2sigma_Diabetes)
print("threshold_high_3sigma Diabetes :-",threshold_high_3sigma_Diabetes)

# Filter outliers
outliers_2sigma_Diabetes = df20.filter((col("claim_amount_settled") > threshold_high_2sigma_Diabetes))
outliers_3sigma_Diabetes = df20.filter((col("claim_amount_settled") > threshold_high_3sigma_Diabetes))

print("Outlier 2sigma_Diabetesr",outliers_2sigma_Diabetes.count())
print("Outlier 3sigma_Diabetes",outliers_3sigma_Diabetes.count())



frequent_claim_threshold = 2

fraudulent_claims = df20.withColumn("high_value_claim", when(df20.claim_amount_settled > threshold_high_2sigma_Diabetes, "Yes").otherwise("No"))

# Identifying frequent claims

frequent_claims = df20.groupBy("hospital_id").count().withColumnRenamed("count", "claim_count")

frequent_claims = frequent_claims.withColumn("frequent_claims", when(col("claim_count") > frequent_claim_threshold, "Yes").otherwise("No"))

# Join datasets to get potentially fraudulent claims
fraudulent_claims = fraudulent_claims.join(frequent_claims, on="hospital_id", how="left").filter((col("high_value_claim") == "Yes") | (col("frequent_claims") == "Yes"))

# Order by high_value_claim in descending order
fraudulent_claims = fraudulent_claims.filter(col("disease")=="Diabetes").orderBy(col("high_value_claim").desc())

fraudulent_claims = fraudulent_claims.select("hospital_id","claim_id","insurance_provider_id","claim_amount_settled","high_value_claim","claim_count","frequent_claims")

fraudulent_claims.show()


print("Count of claims which are greater than threshold Diabetes claim amount :-",fraudulent_claims.count())


frauds = fraudulent_claims.withColumn("fraud_detected", when(((col("high_value_claim") == "Yes") & (col("frequent_claims") == "Yes")), "Yes").otherwise("No"))

frauds = frauds.select("hospital_id","claim_id","insurance_provider_id","claim_amount_settled","high_value_claim","claim_count","frequent_claims",col("fraud_detected"))

frauds = frauds.filter((col("high_value_claim") == "Yes") & (col("frequent_claims") == "Yes"))

frauds.show()

print("Count of fraud claims are :-",frauds.count())

