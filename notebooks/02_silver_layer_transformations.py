from pyspark.sql.functions import (
    col,
    when,
    current_date,
    floor,
    datediff,
    year
)

# ============================================================
# SILVER LAYER - HEALTHCARE DATA ENGINEERING PIPELINE
# ============================================================

# ------------------------------------------------------------
# 1. PATIENTS
# ------------------------------------------------------------

patients_df = spark.table("patients")

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
