# Azure based end-to-end data engineering pipeline

This project demonstrates a production-style data pipeline built on Azure, implementing dynamic data ingestion and incremental data processing using **watermarking**. The pipeline leverages **Azure Data Factory** for orchestration, **Azure Data Lake** for storage, and **Databricks** (PySpark) for scalable data transformation into analytics-ready fact and dimension tables.

## Architecture

## Source Data Preparation Pipeline
This pipeline is designed to dynamically ingest data from multiple sources using parameterized file paths and dataset configurations.

### Pipeline View
<img width="295" height="149" alt="source_prep_pipeline" src="https://github.com/user-attachments/assets/1132204f-1270-439b-af7a-b826701641fb" />


### Pipeline Parameters
<img width="740" height="294" alt="source_prep_pipeline_parameters" src="https://github.com/user-attachments/assets/d4157d4c-7ac1-42e7-978f-3998c21c95d8" />


### Parameter Usage in Copy Activity
<img width="1698" height="258" alt="source_copy_activity_parameters" src="https://github.com/user-attachments/assets/877ab996-6a14-4cdc-96f7-c37281e176d9" />


## Incremental Data Pipeline
Pipeline performs incremental ingestion using lookup-based watermarking, followed by transformation in Databricks and loading into fact/dimension tables.

### Pipeline View
<img width="1443" height="499" alt="inc_data_pipeline" src="https://github.com/user-attachments/assets/0db224ab-1ad2-44d1-bcdd-9c14b8324f42" />






