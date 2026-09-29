from pyspark.sql.functions import col, sum, count

# ============================================================
# BUSINESS INSIGHTS
# ============================================================

# ------------------------------------------------------------
# 1. Claims by Status
# ------------------------------------------------------------

display(
    spark.table("gold_claims_summary")
    .orderBy("Claim_Status")
)


# ------------------------------------------------------------
# 2. Lab Results
# ------------------------------------------------------------

display(
    spark.table("gold_labs_summary")
    .orderBy("Result_Status")
)


# ------------------------------------------------------------
# 3. Provider Specialty
# ------------------------------------------------------------

display(
    spark.table("gold_provider_summary")
    .groupBy("Specialty")
    .sum("Total_Providers")
    .withColumnRenamed("sum(Total_Providers)", "Total_Providers")
    .orderBy("Specialty")
)


# ------------------------------------------------------------
# 4. Medication Usage
# ------------------------------------------------------------

display(
    spark.table("gold_medication_summary")
    .orderBy("Duration_Category")
)


# ------------------------------------------------------------
# 5. Patient Activity
# ------------------------------------------------------------

display(
    spark.table("gold_patient_summary")
    .groupBy("Activity_Category")
    .count()
    .orderBy("Activity_Category")
)


# ------------------------------------------------------------
# 6. Insurance Distribution
# ------------------------------------------------------------

display(
    spark.table("gold_patient_summary")
    .groupBy("Insurance_Type")
    .count()
    .orderBy("Insurance_Type")
)


# ------------------------------------------------------------
# 7. Patients by City
# ------------------------------------------------------------

display(
    spark.table("gold_patient_summary")
    .groupBy("City")
    .count()
    .orderBy("City")
)


# ------------------------------------------------------------
# 8. Encounters by Department
# ------------------------------------------------------------

display(
    spark.table("gold_encounter_summary")
    .groupBy("Department")
    .sum("Total_Encounters")
    .withColumnRenamed("sum(Total_Encounters)", "Total_Encounters")
    .orderBy("Department")
)


# ------------------------------------------------------------
# 9. Encounters by Type
# ------------------------------------------------------------

display(
    spark.table("gold_encounter_summary")
    .groupBy("Encounter_Type")
    .sum("Total_Encounters")
    .withColumnRenamed("sum(Total_Encounters)", "Total_Encounters")
    .orderBy("Encounter_Type")
)


# ------------------------------------------------------------
# 10. Overall Healthcare Metrics
# ------------------------------------------------------------

gold_patient_summary_df = spark.table("gold_patient_summary")

gold_patient_summary_df.select(
    "Total_Encounters",
    "Total_Claims",
    "Total_Claim_Amount",
    "Total_Labs",
    "Abnormal_Lab_Results",
    "Total_Medications"
).agg(
    sum("Total_Encounters").alias("Total_Encounters"),
    sum("Total_Claims").alias("Total_Claims"),
    sum("Total_Claim_Amount").alias("Total_Claim_Amount"),
    sum("Total_Labs").alias("Total_Labs"),
    sum("Abnormal_Lab_Results").alias("Abnormal_Lab_Results"),
    sum("Total_Medications").alias("Total_Medications")
).show()
