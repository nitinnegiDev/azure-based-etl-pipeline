# Azure-Based End-to-End Data Engineering Pipeline

This project demonstrates a production-style data pipeline built on Azure, implementing dynamic data ingestion and incremental data processing using **watermarking**. The pipeline leverages **Azure Data Factory** for orchestration, **Azure Data Lake** for storage, and **Databricks** (PySpark) for scalable data transformation into analytics-ready fact and dimension tables.

## Architecture

Source Systems → Azure Data Factory → Azure Data Lake (Bronze Layer) → Databricks (PySpark Transformations) → Fact & Dimension Tables

## Data Flow

1. Source data is ingested dynamically using parameterized pipelines.
2. Data is stored in Azure Data Lake Storage (Bronze Layer).
3. Incremental data is identified using watermark logic via ADF Lookup activity, with watermark values managed and updated using a SQL stored procedure.
4. Data is processed in Databricks using PySpark transformations.
5. Transformed data is loaded into fact and dimension tables for analytics.


## Pipeline Orchestration

### Source Data Preparation Pipeline
This pipeline is designed to dynamically ingest data from multiple sources using parameterized file paths and dataset configurations.


#### Pipeline View
<img width="295" height="149" alt="source_prep_pipeline" src="https://github.com/user-attachments/assets/1132204f-1270-439b-af7a-b826701641fb" />

#### Pipeline Parameters
<img width="740" height="294" alt="source_prep_pipeline_parameters" src="https://github.com/user-attachments/assets/d4157d4c-7ac1-42e7-978f-3998c21c95d8" />


#### Parameter Usage in Copy Activity
<img width="1698" height="258" alt="source_copy_activity_parameters" src="https://github.com/user-attachments/assets/877ab996-6a14-4cdc-96f7-c37281e176d9" />


### Incremental Data Pipeline
Pipeline performs incremental ingestion using lookup-based watermarking, followed by transformation in Databricks and loading into fact/dimension tables.

#### Pipeline View
<img width="1443" height="499" alt="inc_data_pipeline" src="https://github.com/user-attachments/assets/0db224ab-1ad2-44d1-bcdd-9c14b8324f42" />

## Pipeline Execution

### ADF Orchestration Run
Pipeline execution monitored via ADF Monitor, ensuring successful incremental ingestion and downstream processing.
#### Monitor Overview
<img width="1830" height="289" alt="monitor_inc_pipeline_run" src="https://github.com/user-attachments/assets/0afe3274-4fcf-4ece-a9e3-5e99b6c02469" />

#### Activity-Level Execution
<img width="1837" height="952" alt="inc_pipeline_run_detailed_activity_view" src="https://github.com/user-attachments/assets/8662f0f8-dd1b-4f01-93bf-1b0684fa8039" />


#### Databricks Job Execution
Databricks job execution validates successful transformation of ingested data into analytics-ready datasets.
<img width="1618" height="683" alt="databricks_job_run" src="https://github.com/user-attachments/assets/97055b55-db90-4ad4-af58-6b5963067494" />


## Data Transformation (Databricks)
Data transformation is performed using PySpark in Databricks, where raw data is cleaned, joined, and structured into dimension and fact tables for analytical use.

### Silver Layer Transformation (Bronze → Silver)

- Ingested raw data from Azure Data Lake (Bronze layer)
- Performed column transformations and feature engineering
- Created derived KPIs (e.g., revenue per unit)
- Aggregated data for analytical use cases
- Stored processed data in Silver layer


#### Transformation Logic
<img width="1220" height="787" alt="silver_nb_transfrom_col" src="https://github.com/user-attachments/assets/0b0eb497-1d5e-4752-8b58-f7c7b247fef7" />
<img width="1176" height="725" alt="silver_nb_kpi" src="https://github.com/user-attachments/assets/33972f60-2f70-4116-ad4d-0a55515b1db9" />



#### Aggregation Example
<img width="1209" height="743" alt="silver_nb_agg" src="https://github.com/user-attachments/assets/b97eb290-a171-4d3e-a3c5-0b268e2757d0" />

#### Data Write to Silver Layer
<img width="1207" height="205" alt="silver_nb_sink" src="https://github.com/user-attachments/assets/edcb9a5c-1686-4fb5-a0de-54e29ea2757e" />

### Gold Layer Transformation (Dimensional Modeling)

- Built dimension tables using incremental loading strategy
- Implemented surrogate key generation for unique identification
- Applied Slowly Changing Dimension (SCD Type-1) using Delta Lake MERGE
- Ensured deduplication and consistency of dimension data

#### Dimension Table Logic (Example: dim_model)
<img width="1178" height="195" alt="gold_nb_df_joined" src="https://github.com/user-attachments/assets/b3f6a7c0-e83d-461c-92f9-c1a0f0024d44" />


#### SCD Type-1 Upsert using Delta Lake
<img width="1172" height="461" alt="gold_nb_scd" src="https://github.com/user-attachments/assets/2aa7d9cd-27e0-427e-9fbb-6211273b8a23" />


#### Fact Table Creation (Star Schema)
- Designed fact table by joining multiple dimension tables (star schema)
- Generated analytical dataset with measures and surrogate keys for reporting
<img width="1174" height="870" alt="fact_table_creation" src="https://github.com/user-attachments/assets/1318f5b7-5317-43b0-b904-2f8e4393c338" />


#### Final Fact Table Output
<img width="1167" height="529" alt="delta_write" src="https://github.com/user-attachments/assets/670cb452-b866-496e-9559-c307c7c06534" />


## Key Features

- Incremental data loading using watermarking strategy
- Parameterized pipelines for reusable ingestion
- Modular pipeline design with separation of concerns
- Scalable data processing using PySpark (Databricks)
- Fact and dimension data modeling

## Design Decisions

- Used watermarking to efficiently process only new data
- Parameterized pipelines to enable reuse across datasets
- Separated ingestion and transformation for modular architecture

## Technologies Used

- Azure Data Factory
- Azure Data Lake Storage (ADLS Gen2)
- Azure Databricks
- PySpark
- SQL


## Code Reference

- Databricks Notebooks: [`/databricks`](./databricks)
- SQL Scripts: [`/sql`](./sql)





