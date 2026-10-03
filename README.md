# Healthcare Data Engineering Pipeline

An end-to-end healthcare data engineering project built using **Python, PySpark, Databricks, Delta Lake, SQL, and GitHub**.

The project implements a layered **Bronze, Silver, and Gold architecture** to transform raw healthcare datasets into cleaned, analytics-ready data and business insights.

---

## 📌 Project Overview

This project demonstrates an end-to-end healthcare data engineering pipeline that processes:

- Patients
- Encounters
- Claims
- Laboratory Tests
- Providers
- Medications

The pipeline performs:

- Data ingestion
- Data quality validation
- Duplicate removal
- Data transformation
- Derived column creation
- Aggregation
- Analytics-ready data preparation
- Business insight generation

### Architecture

**Raw CSV Data → Bronze → Silver → Gold → Business Insights**

---

## 🛠️ Technologies Used

- Python
- PySpark
- Databricks
- Delta Lake
- SQL
- GitHub

---

## 📂 Data Sources

The project uses six healthcare datasets:

| Dataset | Description |
|---|---|
| Patients | Patient demographic and registration information |
| Encounters | Patient healthcare encounters |
| Claims | Healthcare claim information and amounts |
| Labs | Laboratory test results |
| Providers | Healthcare provider information |
| Medications | Patient medication records |

---

## 🏗️ Data Architecture

### Raw Data

The original healthcare datasets are stored as CSV files in:

`data/raw/`

### Bronze Layer

The Bronze layer loads the source datasets into Databricks Delta tables while preserving the source data as the initial processing layer.

Bronze tables:

- `patients`
- `encounters`
- `claims`
- `providers`
- `labs`
- `medications`

### Silver Layer

The Silver layer performs data cleaning, validation, duplicate removal, and transformation using PySpark.

Silver tables:

- `silver_patients`
- `silver_encounters`
- `silver_claims`
- `silver_providers`
- `silver_labs`
- `silver_medications`

### Silver Transformations

The pipeline creates the following derived columns:

| Table | Derived Column |
|---|---|
| `silver_patients` | `Age` |
| `silver_encounters` | `Encounter_Year` |
| `silver_claims` | `Claim_Year` |
| `silver_providers` | `Experience_Category` |
| `silver_labs` | `Abnormal_Flag` |
| `silver_medications` | `Duration_Category` |

Data quality checks include:

- NULL validation
- Duplicate detection/removal
- Category validation
- Numeric value validation
- Business-rule validation

---

## 🥇 Gold Layer

The Gold layer creates analytics-ready datasets using PySpark aggregations and joins.

Gold tables:

- `gold_patient_summary`
- `gold_encounter_summary`
- `gold_claims_summary`
- `gold_labs_summary`
- `gold_medication_summary`
- `gold_provider_summary`

The patient summary combines information from encounters, claims, laboratory tests, and medications at the patient level.

---

## 📊 Business Insights

The Gold layer is used to generate the following business insights:

1. Claims by status
2. Laboratory results
3. Provider specialty
4. Medication usage by duration
5. Patient activity
6. Patient insurance distribution
7. Patients by city
8. Encounters by department
9. Encounters by type
10. Overall healthcare metrics

---

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
| Providers | 20 |

### Data Quality Validation

- Duplicate `Patient_ID` records: **0**
- NULL values in final Gold patient summary: **0**
- Gold patient summary records: **100**
- Silver and Gold tables validated using PySpark transformations and aggregations

---

## 🛠️ Key Data Engineering Skills Demonstrated

- PySpark DataFrame transformations
- Data cleaning and validation
- NULL handling
- Duplicate removal
- Data quality checks
- Filtering and conditional transformations
- Grouping and aggregations
- Joins
- Derived columns
- Delta Lake tables
- Bronze, Silver, and Gold architecture
- Analytics-ready data modeling
- Business-level aggregations
- Databricks notebook development
- GitHub version control

---

## 🔄 Project Workflow

### 1. Data Generation & Ingestion

- Created healthcare datasets for patients, encounters, claims, labs, providers, and medications.
- Stored the raw CSV datasets in the project repository.
- Loaded the datasets into Databricks.

### 2. Bronze Layer

- Loaded source datasets into Databricks Delta tables.
- Preserved the source data as the initial processing layer.

### 3. Silver Layer

- Performed data quality validation.
- Removed duplicate records.
- Validated categorical and numeric fields.
- Created derived columns.
- Stored cleaned datasets as Silver Delta tables.

### 4. Gold Layer

- Joined and aggregated Silver datasets.
- Created patient-level summaries.
- Created encounter, claims, laboratory, medication, and provider summary tables.
- Prepared analytics-ready datasets.

### 5. Business Insights

- Generated business-level healthcare metrics.
- Analyzed claims, labs, providers, medications, patients, and encounters.
- Validated final aggregate metrics.

---

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
