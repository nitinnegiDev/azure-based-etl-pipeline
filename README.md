# Azure based end-to-end data engineering pipeline

This project demonstrates a production-style data pipeline built on Azure, implementing dynamic data ingestion and incremental data processing using **watermarking**. The pipeline leverages **Azure Data Factory** for orchestration, **Azure Data Lake** for storage, and **Databricks** (PySpark) for scalable data transformation into analytics-ready fact and dimension tables.

## Architecture

## Data Flow

1. Source data is ingested dynamically using parameterized pipelines.
2. Data is stored in Azure Data Lake Storage container (bronze layer).
3. Incremental data is identified using watermark logic via lookup activity.
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
<img width="1618" height="683" alt="databricks_job_run" src="https://github.com/user-attachments/assets/97055b55-db90-4ad4-af58-6b5963067494" />


## Data Transformation (Databricks)

### Transformation Logic
(image)

### Output Data
(image)

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





