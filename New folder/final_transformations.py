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

df20 = spark.read.csv(r"D:\FINALPROJECTS\New folder\merged_data\merged_data_final - Copy.csv", header=True, schema=schema1)
df20.cache()
df20.printSchema()


### Problem 1: Fraud Detection

# Identified fraudulent claims based on patterns such as unusually high claim amounts or frequent claims from the same provider.



# Define thresholds
high_claim_threshold = 4200
frequent_claim_threshold = 2

# Identify high-value claims
fraudulent_claims = df20.withColumn("high_value_claim", when(df20.claim_amount_settled > high_claim_threshold, "Yes").otherwise("No"))

# Identify frequent claims
frequent_claims = df20.groupBy("hospital_id").count().withColumnRenamed("count", "claim_count")
frequent_claims = frequent_claims.withColumn("frequent_claims", when(col("claim_count") > frequent_claim_threshold, "Yes").otherwise("No"))

# Broadcast frequent_claims for better performance
broadcasted_frequent_claims = broadcast(frequent_claims)

# Join datasets to get potentially fraudulent claims
fraudulent_claims = fraudulent_claims.join(broadcasted_frequent_claims, on="hospital_id", how="left") \
                                     .filter((col("high_value_claim") == "Yes") | (col("frequent_claims") == "Yes")) \
                                     .orderBy(col("high_value_claim").desc())

# Cache fraudulent_claims
fraudulent_claims.cache()

fraudulent_claims.printSchema()
fraudulent_claims.show(truncate=False)

print("Count of claims which are greater than threshold claim amount :-", fraudulent_claims.count())

print("##################################################################################")

print("#################### Hospital wise fraud detection #################### ")

frauds = fraudulent_claims.withColumn("fraud_detected", when(((col("high_value_claim") == "Yes") & (col("frequent_claims") == "Yes")), "Yes").otherwise("No"))
frauds = frauds.select("hospital_id", "claim_id", "insurance_provider_id", "claim_amount_settled", "high_value_claim", "claim_count", "frequent_claims", col("fraud_detected"))
frauds = frauds.filter((col("high_value_claim") == "Yes") & (col("frequent_claims") == "Yes"))

frauds.show()

print("Count of fraud claims are :-", frauds.count())





### Problem 2: Cost Analysis

# Analyzed the cost distribution among different types of claims and identify the top contributors to the overall cost.

# Calculate total cost for each claim type
cost_analysis = df20.groupBy("INSURANCE_PLAN").agg(
    sum("medical_bill_AMOUNT").alias("total_medical_bill"),
    sum("hospital_bill_AMOUNT").alias("total_hospital_bill"),
    sum("copay").alias("total_copay"),
    sum("coinsurance").alias("total_coinsurance"),
    sum("deductible").alias("total_deductible"),
    sum("claim_amount").alias("total_claim_amount")
)

# Calculate percentage contribution of each type
total_claims = cost_analysis.agg(sum("total_claim_amount").alias("total")).collect()[0]["total"]
cost_analysis = cost_analysis.withColumn("percentage_contribution", col("total_claim_amount") / total_claims * 100)

cost_analysis.orderBy(col("total_claim_amount").desc()).show()



### Problem 3: Patient Cost Burden Analysis

# Analyzed the out-of-pocket cost burden on patients, considering copay, coinsurance, and deductibles.

# Calculate patient out-of-pocket costs
average_cost_burden = df20.groupBy("patientid").agg(
    round(sum("outofpocketmax") / count("claim_id"), 2).alias("avg_out_of_pocket"))

# Show patients with highest average out-of-pocket costs
average_cost_burden.orderBy(col("avg_out_of_pocket").desc()).show()



### Problem 4: Provider Performance Analysis

# Calculate average claim amount and frequency of claims for each provider
provider_performance = df20.groupBy("insurance_provider_id").agg(
    avg("claim_amount_settled").alias("avg_claim_amount"),
    count("claim_id").alias("claim_count")
)

# Providers with highest average claim amount and frequency
provider_performance.orderBy(col("avg_claim_amount").desc(), col("claim_count").desc()).show()




### Problem 5: Calculate total coverage provided by each insurance plan

insurance_coverage = df20.groupBy("INSURANCE_PLAN").agg(
    avg("claim_amount_settled").alias("avg_claim_amount_settled"),
    max("claim_amount_settled").alias("max_claim_amount_settled"),
    min("claim_amount_settled").alias("min_claim_amount_settled")
)

print("Plan with total coverages :-", insurance_coverage.show())
