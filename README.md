# DataEng_ZC
Data Engineering ZC on GCP and AWS

Original course from DTC on GCP: https://github.com/DataTalksClub/data-engineering-zoomcamp

# Structure repo
├───analyses 			==> necessary for dbt cloud used in week 4  
├───cohorts  
│   ├───2022  
│   │   ├───week_1_basics_n_setup       
│   │   └───week_2_data_ingestion       
│   │       ├───airflow 			==> old since this is for the zoomcamp of 2022  
│   │       ├───homework  
│   │       └───transfer_service  
│   └───2023    
│       └───week_1_docker_sql  
├───data  
├───gcp  
├───images  
│   ├───architecture  
│   └───aws  
├───macros 			==> necessary for dbt cloud used in week 4    
├───models 			==> necessary for dbt cloud used in week 4  
│   ├───core  
│   └───staging  
├───nyc-tlc-data 			==> auxiliary scripts to upload csvs to GCS of S3  
│   ├───fhv_csvs  
│   ├───green_csvs  
│   └───yellow_csvs  
├───seeds 			==> necessary for dbt cloud used in week 4  
├───snapshots  
├───tests 			==> necessary for dbt cloud used in week 4  
├───week_1_basics_n_setup  
│   ├───1_cloudformation_aws  
│   ├───1_terraform_aws  
│   ├───1_terraform_gcp  
│   │   └───terraform  
│   ├───2_docker_sql  
│   ├───2_docker_sql_v1.2.2 			==> code as in the video 1.2.2  
│   │   ├───.ipynb_checkpoints  
│   │   └───ny_taxi_postgres_data  
│   ├───2_docker_sql_v1.2.3 			==> code as in the video 1.2.3  
│   │   ├───.ipynb_checkpoints  
│   │   └───ny_taxi_postgres_data  
│   ├───2_docker_sql_v1.2.4 			==> code as in the video 1.2.4  
│   │   └───ny_taxi_postgres_data  
│   ├───2_docker_sql_v1.2.5 			==> code as in the video 1.2.5 	  
│   │   └───ny_taxi_postgres_data  
│   └───homework  
│       ├───q2  
│       └───q3-4-5-6  
│           └───ny_taxi_postgres_data  
├───week_2_workflow_orchestration  
│   └───prefect-zoomcamp  
│       ├───blocks  
│       ├───exploration  
│       ├───flows  
│       │   ├───01_start  
│       │   ├───02_aws  
│       │   │   └───data  
│       │   ├───02_gcp  
│       │   │   └───data  
│       │   ├───03_deployments  
│       │   └───homework  
│       │       ├───data  
│       └───images  
├───week_3_data_warehouse  
│   ├───airflow 			==> old since this is for the zoomcamp of 2022  
│   ├───aws_private  
│   ├───extras  
│   ├───serving_dir  
│   │   └───tip_model  
├───week_4_analytics_engineering 			==> code not used since in week 4 I used dbt cloud and created the same folders and scripts on the root of the repo. See Github versions in history to check the status of the codes corresponding to the different videos  
├───week_5_batch_processing  
│   ├───code  
│   └───setup  
│       └───config  
├───week_6_stream_processing  
│   ├───java  
│   │   └───kafka_examples  
│   │       ├───gradle  
│   │       └───src  
│   ├───python  
│   │   └───avro_example  
│   └───streams  
└───week_7_project            