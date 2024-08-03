from pyspark.sql import SparkSession
from pyspark.sql import *

spark = SparkSession.builder.master("local[*]").getOrCreate()

# df = spark.read.csv(r"D:\FINALPROJECTS\New folder\finaldata2.csv",header=True)
# df.printSchema()
# df.show()
#
# df1 = df.select("patientid","patient_name","date_of_birth","age","gender","address","city","contact_number")
# df1.show()


df1 = spark.read.csv(r"D:\FINALPROJECTS\New folder\BLUECROSS_OSCARHEALT_AETNA_CIGNA(INSURANCE_PROVIDERS).csv",header=True)
df1.printSchema()
df1.show()
print("################## INSURANCE PROVIDER AND PLAN ##################")

print("records are",df1.count())

df2 = spark.read.csv(r"D:\FINALPROJECTS\New folder\CERNER_CUREMD_PRACTO(PATIENT_INFO).csv",header=True)
df21 = df2.select("patientid","patient_name","date_of_birth","p_age","gender","patient_address","city","contact_number")
df21.printSchema()
df21.show()

print("################## PATIENT INFO ##################")



df3 = spark.read.csv(r"D:\FINALPROJECTS\New folder\CIGNA(INSURANCE_CLAIM_DATA).csv",header=True)
df3.printSchema()
df3.show()


print("################## CLAIM DATA ##################")



df4 = spark.read.csv(r"D:\FINALPROJECTS\New folder\CREDIT_UPI_PAYZAPP(PAYMENT_INFO).csv",header=True)
df4.printSchema()
df4.show()

print("################## PAYMENT INFO ##################")



df5 = spark.read.csv(r"D:\FINALPROJECTS\New folder\HEALTHTAP_HEALTHVAULT(patient_information).csv",header=True)
# df5= df5.select("patientid","hospital_id","DISEASE","SUB_DISEASE","DISEASE_CODE","DIAGNOSIS/TREATMENT","MEDICINE")
df5.printSchema()
df5.show()

print("################## DISEASE INFO ##################")



df6 = spark.read.csv(r"D:\FINALPROJECTS\New folder\MEDISAFE_CAREZONE_GOODRX_NETMEDS(MEDICINE_BILLS).csv",header=True)
df6= df6.select("patientid","hospital_id","physician_id","DISEASE_CODE","medicine_names","medical_bill_AMOUNT")

df6.printSchema()
df6.show()

print("################## MEDICINE INFO ##################")



df7 = spark.read.csv(r"D:\FINALPROJECTS\New folder\HOSPITALS.csv",header=True)
df7 = df7.select("hospital_id","Hospital_address","physician_id","Speciality","INSURANCE_provider_name","insurance_provider_id")
df7.printSchema()
df7.show()


print("################## PROVIDER INFO ##################")


df8 = df2.join(df5,"patientid","inner")\
        .join(df6,"patientid","inner")
df8.printSchema()
df8.show()

print("Count of above dataframe is : - ",df8.count())

print("################## JOIN 1 ##################")
print("PATIENT_INFO,DISEASE_INFO,MEDICINE_INFO")


df9 = df3.join(df4,"hospital_id","inner")\
        .join(df7,"hospital_id","inner")
df9.printSchema()
df9.show()

print("Count of above dataframe is : - ",df9.count())


print("################## JOIN 2 ##################")
print("HOSPITALS_INFO,CLAIM_INFO,PAYMENT_INFO")


df10 = df8.join(df9,"hospital_id","inner")\
            .join(df1,"health_insurance_id","inner")

df10.printSchema()
df10.show()

print("Count of above dataframe is : - ",df10.count())


df11 = df10.select("patientid",df21.patient_name,df21.date_of_birth,df21.p_age,df21.gender,df21.patient_address,df3.health_insurance_id,\
                   df3.hospital_id,"Hospital_address",df7.physician_id,df7.Speciality,df7.INSURANCE_provider_name,df7.insurance_provider_id,\
                   df5.DISEASE,df5.SUB_DISEASE,df5.DISEASE_CODE,df5["DIAGNOSIS/TREATMENT"],df6.medicine_names,df6.medical_bill_AMOUNT,"hospital_bill_AMOUNT",\
                   "INSURANCE_PLAN","INSURANCE_PREMIUM","insurance_start_date","insurance_end_date",df3.claim_id,df3.claim_amount,df3.deductible,\
                   df3.copay,df3.COINSURANCE,df3.outofpocketmax,df3.claim_amount_settled,"payment_status","payment_method","payment_id")

df12 = df11.coalesce(1)

df12.write.csv(r"D://FINALPROJECTS/New folder/final_data/",mode="overwrite",header=True)

