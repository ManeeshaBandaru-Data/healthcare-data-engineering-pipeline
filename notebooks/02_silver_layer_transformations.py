from pyspark.sql.functions import (
    col,
    when,
    current_date,
    floor,
    datediff,
    year,
    count
)

# ============================================================
# SILVER LAYER - HEALTHCARE DATA ENGINEERING PIPELINE
# ============================================================

# ------------------------------------------------------------
# 1. PATIENTS
# ------------------------------------------------------------

patients_df = spark.table("patients")

# Data Quality Validation
print("Patient NULL counts:")
patients_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in patients_df.columns
]).show()

print("Invalid Gender records:")
patients_df.filter(~col("Gender").isin("Male", "Female")).show()

print("Invalid Insurance Type records:")
patients_df.filter(
    ~col("Insurance_Type").isin("Private", "Government", "Employer")
).show()

patients_clean_df = patients_df.dropDuplicates(["Patient_ID"])

silver_patients_df = patients_clean_df.withColumn(
    "Age",
    floor(datediff(current_date(), col("DOB")) / 365.25)
)

silver_patients_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_patients")


# ------------------------------------------------------------
# 2. ENCOUNTERS
# ------------------------------------------------------------

encounters_df = spark.table("encounters")

# Data Quality Validation
print("Encounter NULL counts:")
encounters_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in encounters_df.columns
]).show()

print("Invalid Encounter Type records:")
encounters_df.filter(
    ~col("Encounter_Type").isin("Inpatient", "Emergency", "OPD")
).show()

print("Invalid Department records:")
encounters_df.filter(
    ~col("Department").isin(
        "Cardiology",
        "Neurology",
        "Orthopedics",
        "General Medicine"
    )
).show()

encounters_clean_df = encounters_df.dropDuplicates(["Encounter_ID"])

silver_encounters_df = encounters_clean_df.withColumn(
    "Encounter_Year",
    year(col("Encounter_Date"))
)

silver_encounters_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_encounters")


# ------------------------------------------------------------
# 3. CLAIMS
# ------------------------------------------------------------

claims_df = spark.table("claims")

# Data Quality Validation
print("Claim NULL counts:")
claims_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in claims_df.columns
]).show()

print("Invalid Claim Status records:")
claims_df.filter(
    ~col("Claim_Status").isin("Approved", "Rejected", "Pending")
).show()

print("Invalid Claim Amount records:")
claims_df.filter(
    col("Claim_Amount") <= 0
).show()

claims_clean_df = claims_df.dropDuplicates(["Claim_ID"])

silver_claims_df = claims_clean_df.withColumn(
    "Claim_Year",
    year(col("Claim_Date"))
)

silver_claims_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_claims")


# ------------------------------------------------------------
# 4. PROVIDERS
# ------------------------------------------------------------

providers_df = spark.table("providers")

# Data Quality Validation
print("Provider NULL counts:")
providers_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in providers_df.columns
]).show()

print("Invalid Experience records:")
providers_df.filter(
    col("Experience_Years") <= 0
).show()

print("Invalid Specialty records:")
providers_df.filter(
    ~col("Specialty").isin(
        "General Medicine",
        "Orthopedics",
        "Cardiology",
        "Neurology"
    )
).show()

providers_clean_df = providers_df.dropDuplicates(["Provider_ID"])

silver_providers_df = providers_clean_df.withColumn(
    "Experience_Category",
    when(col("Experience_Years") < 10, "Junior")
    .when(col("Experience_Years") < 15, "Mid-Level")
    .otherwise("Senior")
)

silver_providers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_providers")


# ------------------------------------------------------------
# 5. LABS
# ------------------------------------------------------------

labs_df = spark.table("labs")

# Data Quality Validation
print("Lab NULL counts:")
labs_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in labs_df.columns
]).show()

print("Invalid Result Status records:")
labs_df.filter(
    ~col("Result_Status").isin("Normal", "High", "Low")
).show()

print("Invalid Test Result records:")
labs_df.filter(
    col("Test_Result") <= 0
).show()

labs_clean_df = labs_df.dropDuplicates(["Lab_ID"])

silver_labs_df = labs_clean_df.withColumn(
    "Abnormal_Flag",
    when(col("Result_Status").isin("High", "Low"), 1)
    .otherwise(0)
)

silver_labs_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_labs")


# ------------------------------------------------------------
# 6. MEDICATIONS
# ------------------------------------------------------------

medications_df = spark.table("medications")

# Data Quality Validation
print("Medication NULL counts:")
medications_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in medications_df.columns
]).show()

print("Invalid Duration records:")
medications_df.filter(
    col("Duration_Days") <= 0
).show()

print("Medication Name distribution:")
medications_df.groupBy("Medication_Name").count().orderBy("Medication_Name").show()

medications_clean_df = medications_df.dropDuplicates(["Medication_ID"])

silver_medications_df = medications_clean_df.withColumn(
    "Duration_Category",
    when(col("Duration_Days") < 7, "Short-Term")
    .when(col("Duration_Days") <= 14, "Medium-Term")
    .otherwise("Long-Term")
)

silver_medications_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_medications")
