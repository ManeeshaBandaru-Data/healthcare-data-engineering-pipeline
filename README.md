# Healthcare Data Engineering Pipeline

An end-to-end healthcare data engineering project built using PySpark and Databricks.

The project implements a layered data architecture using Bronze, Silver, and Gold layers to transform raw healthcare data into analytics-ready datasets and business insights.

## 📌 Project Overview

This project demonstrates a healthcare data engineering pipeline that processes healthcare data through different layers:

**Bronze → Silver → Gold**

The pipeline performs data ingestion, transformation, cleaning, validation, and business-level aggregations using PySpark and Databricks.

## 🛠️ Technologies Used

- Python
- PySpark
- Databricks
- Delta Lake
- SQL
- GitHub

## 📂 Data Sources

The project uses the following healthcare datasets:

- Patients
- Encounters
- Claims
- Labs
- Providers
- Medications

## 🏗️ Data Architecture

The project follows a layered data architecture:

Raw CSV Data
↓
Bronze Layer
↓
Silver Layer
↓
Gold Layer
↓
Business Insights

## 🥉 Bronze Layer

The Bronze layer contains the raw healthcare datasets without major transformations.

Raw CSV files are stored in:

`data/raw/`

The Bronze layer preserves the original source data and serves as the starting point for the data pipeline.

## 🥈 Silver Layer

The Silver layer contains cleaned and transformed healthcare data.

The following transformations were performed using PySpark:

- Data type conversions
- Null value handling
- Duplicate removal
- Data validation
- Column transformations
- Derived columns such as `Age`
- Data standardization

The cleaned datasets are stored as Silver tables:

- `silver_patients`
- `silver_encounters`
- `silver_claims`
- `silver_providers`
- `silver_labs`
- `silver_medications`

## 🥇 Gold Layer

The Gold layer contains business-level aggregations and analytics-ready datasets created from the Silver layer.

The following Gold tables were created:

- `gold_patient_summary`
- `gold_encounter_summary`
- `gold_claims_summary`
- `gold_labs_summary`
- `gold_medication_summary`
- `gold_provider_summary`

## 📊 Business Insights

The Gold layer is used to generate business-level healthcare insights, including:

1. Claims by status
2. Laboratory results
3. Provider specialty
4. Medication usage by duration
5. Patient activity
6. Patient insurance distribution
7. Patients by city
8. Encounters by department
9. Encounters by type
10. Total healthcare metrics

## 🎯 Project Objective

The objective of this project is to demonstrate an end-to-end healthcare data engineering pipeline using PySpark and Databricks.

The project focuses on:

- Building a layered Bronze, Silver, and Gold architecture
- Cleaning and transforming healthcare datasets
- Creating analytics-ready Gold tables
- Generating business-level healthcare insights
- Applying data engineering concepts using PySpark

## 📁 Project Structure

```text
healthcare-data-engineering-pipeline/
│
├── data/
│   └── raw/
│       ├── patients.csv
│       ├── encounters.csv
│       ├── claims.csv
│       ├── labs.csv
│       ├── providers.csv
│       └── medications.csv
│
├── notebooks/
│   ├── 01_create_healthcare_data.ipynb
│   ├── 02_silver_layer_transformations.py
│   ├── 03_gold_layer_transformations.py
│   └── 04_business_insights.py
│
├── .gitignore
└── README.md
```
## 📊 Project Results

The completed pipeline successfully processed the healthcare datasets through the Bronze, Silver, and Gold layers.

### Final Data Metrics

| Metric | Total |
|---|---:|
| Patients | 100 |
| Encounters | 200 |
| Claims | 300 |
| Total Claim Amount | 862,500 |
| Lab Records | 400 |
| Abnormal Lab Results | 267 |
| Medication Records | 300 |

### Data Quality Validation

- Duplicate `Patient_ID` records: 0
- Null values in the final Gold patient summary: 0
- Gold patient summary records: 100
- Silver and Gold tables validated using PySpark transformations and aggregations

## 🛠️ Key Data Engineering Skills Demonstrated

- PySpark DataFrame transformations
- Data cleaning and validation
- Handling null values and duplicates
- Data type and schema management
- Filtering, grouping, and aggregations
- Joins and derived columns
- Delta Lake tables
- Bronze, Silver, and Gold architecture
- Business-level aggregations
- Data quality validation
- Databricks notebook development
- GitHub-based project versioning

## 🔄 Project Workflow

1. **Data Generation & Ingestion**
   - Created healthcare datasets for patients, encounters, claims, labs, providers, and medications.
   - Stored the raw CSV datasets in the project repository.

2. **Bronze Layer**
   - Loaded the source datasets into Databricks Delta tables.
   - Preserved the raw data as the initial processing layer.

3. **Silver Layer**
   - Performed data cleaning and validation.
   - Handled null values and duplicates.
   - Applied data quality checks.
   - Created derived columns such as `Age`, `Encounter_Year`, `Claim_Year`, `Experience_Category`, `Abnormal_Flag`, and `Duration_Category`.

4. **Gold Layer**
   - Created analytics-ready tables using PySpark aggregations and transformations.
   - Built patient, encounter, claims, laboratory, medication, and provider summaries.

5. **Business Insights**
   - Generated healthcare metrics and analytical views from the Gold layer.
   - Validated overall record counts and aggregate metrics.
