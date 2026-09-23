from pyspark.sql.functions import col, count, sum, when

# ============================================================
# GOLD LAYER - HEALTHCARE DATA ENGINEERING PIPELINE
# ============================================================

# ------------------------------------------------------------
# 1. LOAD SILVER TABLES
# ------------------------------------------------------------

silver_patients_df = spark.table("silver_patients")
silver_encounters_df = spark.table("silver_encounters")
silver_claims_df = spark.table("silver_claims")
silver_providers_df = spark.table("silver_providers")
silver_labs_df = spark.table("silver_labs")
silver_medications_df = spark.table("silver_medications")


# ------------------------------------------------------------
# 2. PATIENT SUMMARY
# ------------------------------------------------------------

patient_summary_df = silver_patients_df.join(
    silver_encounters_df.groupBy("Patient_ID")
    .count()
    .withColumnRenamed("count", "Total_Encounters"),
    "Patient_ID",
    "left"
)

claims_summary_df = silver_claims_df.groupBy("Patient_ID").agg(
    count("*").alias("Total_Claims"),
    sum("Claim_Amount").alias("Total_Claim_Amount")
)

patient_summary_df = patient_summary_df.join(
    claims_summary_df,
    "Patient_ID",
    "left"
)

labs_summary_df = silver_labs_df.groupBy("Patient_ID").agg(
    count("*").alias("Total_Lab_Tests"),
    sum("Abnormal_Flag").alias("Abnormal_Lab_Results")
)

patient_summary_df = patient_summary_df.join(
    labs_summary_df,
    "Patient_ID",
    "left"
)

medication_summary_df = silver_medications_df.groupBy("Patient_ID").agg(
    count("*").alias("Total_Medications")
)

patient_summary_df = patient_summary_df.join(
    medication_summary_df,
    "Patient_ID",
    "left"
)

patient_summary_df = patient_summary_df.fillna({
    "Total_Encounters": 0,
    "Total_Claims": 0,
    "Total_Claim_Amount": 0,
    "Total_Lab_Tests": 0,
    "Abnormal_Lab_Results": 0,
    "Total_Medications": 0
})

patient_summary_df = patient_summary_df.withColumn(
    "Activity_Category",
    when(col("Total_Encounters") >= 5, "High Activity")
    .when(col("Total_Encounters") >= 3, "Medium Activity")
    .otherwise("Low Activity")
)

patient_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_patient_summary")


# ------------------------------------------------------------
# 3. ENCOUNTER SUMMARY
# ------------------------------------------------------------

encounter_summary_df = silver_encounters_df.groupBy(
    "Encounter_Year",
    "Encounter_Type",
    "Department"
).agg(
    count("*").alias("Total_Encounters")
)

encounter_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_encounter_summary")


# ------------------------------------------------------------
# 4. CLAIMS SUMMARY
# ------------------------------------------------------------

claims_summary_gold_df = silver_claims_df.groupBy(
    "Claim_Year",
    "Claim_Status"
).agg(
    count("*").alias("Total_Claims"),
    sum("Claim_Amount").alias("Total_Claim_Amount")
)

claims_summary_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_claims_summary")


# ------------------------------------------------------------
# 5. LABS SUMMARY
# ------------------------------------------------------------

labs_summary_gold_df = silver_labs_df.groupBy(
    "Result_Status"
).agg(
    count("*").alias("Total_Lab_Tests"),
    sum("Abnormal_Flag").alias("Abnormal_Lab_Results")
)

labs_summary_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_labs_summary")


# ------------------------------------------------------------
# 6. MEDICATION SUMMARY
# ------------------------------------------------------------

medication_summary_gold_df = silver_medications_df.groupBy(
    "Duration_Category"
).agg(
    count("*").alias("Total_Medications")
)

medication_summary_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_medication_summary")


# ------------------------------------------------------------
# 7. PROVIDER SUMMARY
# ------------------------------------------------------------

provider_summary_gold_df = silver_providers_df.groupBy(
    "Specialty",
    "Experience_Category"
).agg(
    count("*").alias("Total_Providers"),
    sum("Experience_Years").alias("Total_Experience_Years")
)

provider_summary_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_provider_summary")
