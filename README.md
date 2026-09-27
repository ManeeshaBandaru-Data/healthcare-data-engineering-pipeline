# Healthcare Data Engineering Pipeline

End-to-end Data Engineering project using Python, PySpark, SQL, and Databricks.

## 📌 Project Overview

This project demonstrates a healthcare data engineering pipeline that processes healthcare data through different layers:

**Bronze → Silver → Gold**

The pipeline performs data ingestion, transformation, cleaning, validation, and business-level aggregations using PySpark and Databricks.

## 🛠️ Technologies Used

- Python
- PySpark
- SQL
- Databricks
- Delta Lake
- GitHub

## 🏗️ Project Architecture

```text
Raw CSV Data
     ↓
Bronze Layer
     ↓
Silver Layer
     ↓
Gold Layer
     ↓
Business Insights

📂 Data Sources

The project contains the following healthcare datasets:

Patients
Encounters
Claims
Labs
Providers
Medications

🥉 Bronze Layer

The Bronze layer contains the raw healthcare datasets before transformation.

Raw files are stored in:

data/raw/

🥈 Silver Layer

The Silver layer performs data cleaning and transformation.

Key transformations include:

Removing duplicate patient records
Calculating patient age
Converting columns to appropriate data types
Extracting encounter year
Cleaning and preparing healthcare datasets
Creating Silver Delta tables

Silver transformation code:

notebooks/02_silver_layer_transformations.py

🥇 Gold Layer

The Gold layer contains business-level aggregations and analytics-ready tables.

Gold Tables
gold_patient_summary
gold_encounter_summary
gold_claims_summary
gold_labs_summary
gold_medication_summary
gold_provider_summary

An earlier gold_patient_analytics table is also present in the Databricks environment.

Gold transformation code:

notebooks/03_gold_layer_transformations.py

📊 Key Analytics

The Gold layer provides analytics such as:

Total patient encounters
Total claims
Total claim amounts
Total laboratory tests
Abnormal laboratory results
Total medications
Provider specialty summaries
Provider experience summaries
Patient activity categories

🎯 Project Objective

The objective of this project is to demonstrate an end-to-end healthcare data engineering workflow using PySpark and Databricks, following a layered data architecture and producing analytics-ready datasets.
