from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import matplotlib.pyplot as plt

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
outliers_2sigma = df20.filter((col("claim_amount_settled") > threshold_high_2sigma) | (col("claim_amount_settled") < threshold_low_2sigma))
outliers_3sigma = df20.filter((col("claim_amount_settled") > threshold_high_3sigma) | (col("claim_amount_settled") < threshold_low_3sigma))

outliers_2sigma.show()
outliers_3sigma.show()

# Convert to Pandas DataFrame for visualization
df_pandas = df20.select("claim_amount_settled").toPandas()

plt.figure(figsize=(12, 6))
plt.hist(df_pandas["claim_amount_settled"], bins=50, color='blue', alpha=0.7, label='Claim Amount Settled')

plt.axvline(mean_claim, color='k', linestyle='dashed', linewidth=1)
plt.axvline(threshold_high_2sigma, color='r', linestyle='dashed', linewidth=1)
plt.axvline(threshold_low_2sigma, color='r', linestyle='dashed', linewidth=1)
plt.axvline(threshold_high_3sigma, color='g', linestyle='dashed', linewidth=1)
plt.axvline(threshold_low_3sigma, color='g', linestyle='dashed', linewidth=1)

min_ylim, max_ylim = plt.ylim()
plt.text(mean_claim, max_ylim*0.9, 'Mean: {:.2f}'.format(mean_claim))
plt.text(threshold_high_2sigma, max_ylim*0.9, '2-Sigma: {:.2f}'.format(threshold_high_2sigma))
plt.text(threshold_low_2sigma, max_ylim*0.9, '2-Sigma: {:.2f}'.format(threshold_low_2sigma))
plt.text(threshold_high_3sigma, max_ylim*0.9, '3-Sigma: {:.2f}'.format(threshold_high_3sigma))
plt.text(threshold_low_3sigma, max_ylim*0.9, '3-Sigma: {:.2f}'.format(threshold_low_3sigma))

plt.xlabel('Claim Amount Settled')
plt.ylabel('Frequency')
plt.title('Claim Amount Distribution with 2-Sigma and 3-Sigma Thresholds')
plt.legend()
plt.show()
