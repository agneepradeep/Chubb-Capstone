
# 🌾 Agricultural Crop Production & Yield Optimization Analytics System

An **end-to-End Data Analytics Capstone Project** implementing a production-grade **ETL + Analytics pipeline** using **Python, Pandas, Databricks (Spark), Apache Airflow, and Power BI**.

---

## 📌 Business Context

Agricultural departments and agribusiness organizations collect large volumes of crop production data across **states, districts, seasons, and crop types**.
However, real-world challenges include:

* Crop data scattered across multiple raw files
* Manual analysis causing delays and inaccuracies
* Limited visibility into yield trends and seasonal performance
* Difficulty identifying high-risk and low-yield regions

### 🎯 Business Goal

To build a **centralized, automated analytics system** that:

* Automates agricultural data processing
* Analyzes production and yield trends
* Supports data-driven agricultural planning
* Provides actionable insights to policymakers and stakeholders

---

## 🎯 Project Objectives

This project demonstrates an **enterprise-style analytics workflow** that:

* Ingests and cleans raw agricultural data using **Python & Pandas**
* Performs large-scale exploration and validation using **PySpark (Databricks)**
* Implements a **Medallion Architecture (Bronze → Silver → Gold)**
* Orchestrates ETL pipelines using **Apache Airflow**
* Builds **interactive Power BI dashboards** with drill-through, KPIs, and trends
* Follows modular, maintainable, and production-ready coding practices

---

## 🧱 Architecture Overview

### 🔹 Medallion Data Architecture

```
Raw CSV
  ↓
Bronze Layer  (Raw Ingestion)
  ↓
Silver Layer  (Cleaned & Enriched)
  ↓
Gold Layer    (Star Schema for BI)
```

* **Bronze** → Raw ingestion with schema standardization
* **Silver** → Data cleaning, normalization, yield recalculation
* **Gold** → Analytics-ready star schema (facts & dimensions)

---

## 🛠️ Technology Stack

### Data Processing

* Python
* Pandas, NumPy

### Big Data & Analytics

* Databricks
* PySpark (for exploration, validation, transformations)

### Workflow Orchestration

* Apache Airflow (Docker-based)

### Data Storage

* CSV files (Bronze / Silver / Gold layers)

### Visualization

* Power BI

### Others

* Git & GitHub
* Jupyter / Databricks Notebooks
* Docker & Docker Compose

---

## 📊 Data Scope

The dataset covers:

* Crop-wise and region-wise production data
* Seasonal and yearly yield records
* Cultivated area and productivity indicators

**Granularity:** District-level across Indian states
**Time Range:** ~1997–2021

---

## 🧩 Core Modules & Features

### A. Data Ingestion (Bronze Layer)

* Reads raw CSV files using Python
* Standardizes column names
* Performs basic validation checks
* Stores raw-but-structured data in Bronze layer

### B. Data Cleaning & Transformation (Silver Layer)

* Handles missing and invalid values
* Normalizes text fields (State, District, Crop, Season)
* Parses year ranges into `year_start` and `year_end`
* Recalculates yield consistently
* Prepares clean analytical dataset

### C. Analytics Modeling (Gold Layer)
![Data Model PowerBI](/screenshots/Data%20Model.png)
* Builds **Star Schema**:

  * `dim_crop`
  * `dim_region`
  * `dim_season`
  * `dim_time`
  * `fact_crop_production`
* Generates surrogate keys
* Produces BI-ready datasets

### D. Workflow Automation (Airflow)

![DAG graph View](/screenshots/Dag%20Graph%20View.png)

* Modular Python tasks for each medallion layer
* Manual trigger-based DAG execution
* Clear task dependencies:

  ```
  ingest_raw_to_bronze
      → transform_bronze_to_silver
          → transform_silver_to_gold
  ```
* Logging and error handling included

### E. Visualization & Reporting (Power BI)

* Executive Overview dashboard
* Yield Analysis by Region & Season
* Detailed Crop & Region Analysis
* State-level drill-through with:

  * Dynamic KPI cards
  * Trend analysis
  * Crop-wise breakdowns
* Advanced DAX for:

  * Filter context control
  * Drill-through stability
  * Dynamic titles & cards

---

## 📂 Project Structure

```
CAPSTONE/
│
├── dags/
│   ├── crop_pipeline_dag.py
│   └── scripts/
│       ├── __init__.py
│       ├── pipeline_config.py
│       └── pipeline_tasks.py
│
├── data/
│   ├── raw/
│   └── gold/
│
├── notebooks/          # Databricks & exploration notebooks
├── screenshots/        # Dashboard & DAG screenshots
│
├── Capstone Power BI Presentation.pbix
├── Capstone Power BI Presentation.pdf
├── docker-compose.yaml
├── Problem Statement.docx
├── README.md
```

---

## 🧪 Testing & Validation

* Data completeness and null checks in Silver layer
* Yield recalculation validation (`production / area`)
* Unit-based data consistency checks
* Power BI dashboard validation against Gold tables

---

## 📦 Deliverables

* Python ETL scripts (Bronze, Silver, Gold)
* Databricks notebooks (Spark-based exploration)
* Airflow DAG & task scripts
* Power BI dashboards (PBIX + PDF)
* DAG graph & dashboard screenshots
* GitHub repository with documentation

---

## 🧠 Key Learnings Demonstrated

* Real-world ETL pipeline design
* Medallion architecture implementation
* Spark vs Pandas trade-offs
* Airflow orchestration concepts
* Power BI filter context & DAX mastery
* Enterprise-style data modeling & documentation

---