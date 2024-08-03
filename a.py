from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.master("local[*]").appName("Healthcare Data Integration").getOrCreate()

# Load Insurance Providers Data
df1 = spark.read.csv(r"D:\FINALPROJECTS\New folder\BLUECROSS_OSCARHEALT_AETNA_CIGNA(INSURANCE_PROVIDERS).csv", header=True)
df1.printSchema()
df1.show()
print("Records in df1:", df1.count())

# Load Patient Info Data
df2 = spark.read.csv(r"D:\FINALPROJECTS\New folder\CERNER_CUREMD_PRACTO(PATIENT_INFO).csv", header=True)
df21 = df2.select("patientid", "patient_name", "date_of_birth", "p_age", "gender", "patient_address", "city", "contact_number")
df21.printSchema()
df21.show()

# Load Insurance Claim Data
df3 = spark.read.csv(r"D:\FINALPROJECTS\New folder\CIGNA(INSURANCE_CLAIM_DATA).csv", header=True)
df3 = df3.select("hospital_id", "claim_id", "health_insurance_id", "provider_name", "provider_id", "hospital_bill_AMOUNT", "medical_bill_AMOUNT", "claim_amount", "deductible", "copay", "COINSURANCE", "COINSURANCE_AMT", "outofpocketmax", "claim_amount_settled", "INSURANCE_PLAN", "INSURANCE_PREMIUM")
df3.printSchema()
df3.show()

# Load Payment Info Data
df4 = spark.read.csv(r"D:\FINALPROJECTS\New folder\CREDIT_UPI_PAYZAPP(PAYMENT_INFO).csv", header=True)
df4 = df4.select("hospital_id", "claim_id", "health_insurance_id", "payment_amount", "payment_status", "payment_method", "payment_id")
df4 = df4.withColumnRenamed("claim_id", "payment_claim_id")  # Rename to avoid duplicate column names
df4.printSchema()
df4.show()

# Load Patient Information Data
df5 = spark.read.csv(r"D:\FINALPROJECTS\New folder\HEALTHTAP_HEALTHVAULT(patient_information).csv", header=True)
df5 = df5.select("patientid", "hospital_id", "DISEASE", "SUB_DISEASE", "DISEASE_CODE", "DIAGNOSIS/TREATMENT", "MEDICINE")
df5 = df5.withColumnRenamed("DISEASE_CODE", "patient_disease_code")  # Rename to avoid duplicate column names
df5.printSchema()
df5.show()

# Load Medicine Bills Data
df6 = spark.read.csv(r"D:\FINALPROJECTS\New folder\MEDISAFE_CAREZONE_GOODRX_NETMEDS(MEDICINE_BILLS).csv", header=True)
df6 = df6.select("patientid", "hospital_id", "physician_id", "DISEASE_CODE", "medicine_names", "medical_bill_AMOUNT")
df6 = df6.withColumnRenamed("DISEASE_CODE", "medicine_disease_code")  # Rename to avoid duplicate column names
df6.printSchema()
df6.show()

# Load Hospitals Data
df7 = spark.read.csv(r"D:\FINALPROJECTS\New folder\HOSPITALS.csv", header=True)
df7 = df7.select("hospital_id", "Hospital_address", "physician_id", "Speciality", "INSURANCE_provider_name", "insurance_provider_id")
df7.printSchema()
df7.show()

# Perform the joins
df8 = df21.join(df5, "patientid", "inner") \
          .join(df3, "hospital_id", "inner") \
          .join(df4, "hospital_id", "inner") \
          .join(df6, "hospital_id", "inner") \
          .join(df7, "hospital_id", "inner") \
          .join(df1, df1.health_insurance_id == df3.health_insurance_id, "inner")

# Select specific columns to avoid duplication and ensure uniqueness
df8 = df8.select(
    col("patientid"),
    col("patient_name"),
    col("date_of_birth"),
    col("p_age"),
    col("gender"),
    col("patient_address"),
    col("city"),
    col("contact_number"),
    col("hospital_id"),
    col("DISEASE"),
    col("SUB_DISEASE"),
    col("patient_disease_code"),
    col("DIAGNOSIS/TREATMENT"),
    col("MEDICINE"),
    col("claim_id"),
    col("health_insurance_id"),
    col("provider_name"),
    col("provider_id"),
    col("hospital_bill_AMOUNT"),
    col("medical_bill_AMOUNT"),
    col("claim_amount"),
    col("deductible"),
    col("copay"),
    col("COINSURANCE"),
    col("COINSURANCE_AMT"),
    col("outofpocketmax"),
    col("claim_amount_settled"),
    col("INSURANCE_PLAN"),
    col("INSURANCE_PREMIUM"),
    col("payment_claim_id"),
    col("payment_amount"),
    col("payment_status"),
    col("payment_method"),
    col("payment_id"),
    col("physician_id"),
    col("medicine_disease_code"),
    col("medicine_names"),
    col("medical_bill_AMOUNT"),
    col("Hospital_address"),
    col("Speciality"),
    col("INSURANCE_provider_name"),
    col("insurance_provider_id"),
    col("insurance_start_date"),
    col("insurance_end_date")
)

# Print the schema of the resulting DataFrame
df8.printSchema()

# Show the joined data
df8.show()

# Write the resulting DataFrame to a CSV file
df8.write.csv(r"D:\FINALPROJECTS\New folder\New folder(2)\output.csv", header=True)

print("Integration complete and data written to CSV.")
