from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (GCSToBigQueryOperator)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

PROJECT_ID = "etl-demo-project-500808"
BUCKET_NAME = "landing_zone_002"

default_args = {
    "owner": "padmasai",
    "retries": 0
}

with DAG(
    dag_id="customer_etl_pipeline_007",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["gcp", "etl"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    load_customer = GCSToBigQueryOperator(
        task_id="load_customer",
        bucket=BUCKET_NAME,
        location="US",
        source_objects=["GCP_Files/source_erp/CUST_AZ12.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.bronze.customer_raw",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
	create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )
    
    audit_customer = BigQueryInsertJobOperator(
    	task_id="audit_customer",
    	configuration={
        	"query": {
            		"query": f"""
	MERGE `{PROJECT_ID}.audit.pipeline_audit` T
	USING (
    SELECT
    	'customer_etl_pipeline' AS pipeline_name,
    	'BRONZE' AS layer,
    	'CUST_AZ12.csv' AS source_file,
    	'customer_raw' AS target_table,
    	COUNT(*) AS records_loaded,
    	'SUCCESS' AS status,
    	CURRENT_TIMESTAMP() AS start_time,
    	CURRENT_TIMESTAMP() AS end_time
    FROM `{PROJECT_ID}.bronze.customer_raw`
	) S
    ON
	T.source_file = S.source_file
	AND T.layer = S.layer
	AND T.target_table = S.target_table
	WHEN NOT MATCHED THEN
    INSERT (
    	pipeline_name,
    	layer,
    	source_file,
     	target_table,
    	records_loaded,
    	status,
    	start_time,
   	end_time
	)
    VALUES (
    	S.pipeline_name,
    	S.layer,
    	S.source_file,
    	S.target_table,
    	S.records_loaded,
    	S.status,
    	S.start_time,
    	S.end_time
	);
	""",
            "useLegacySql": False,
        }
    },
	location="US",
	)

    load_location = GCSToBigQueryOperator(
        task_id="load_location",
        bucket=BUCKET_NAME,
        location="US",
        source_objects=["GCP_Files/source_erp/LOC_A101.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.bronze.location_raw",
        source_format="CSV",
        skip_leading_rows=1,
	create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )
    
    audit_location = BigQueryInsertJobOperator(
    	task_id="audit_location",
    	configuration={
           "query": {
            	"query": f"""
	MERGE `{PROJECT_ID}.audit.pipeline_audit` T

	USING (

	SELECT
   	 'customer_etl_pipeline' AS pipeline_name,
    	'BRONZE' AS layer,
    	'LOC_A101.csv' AS source_file,
    	'location_raw' AS target_table,
    	COUNT(*) AS records_loaded,
    	'SUCCESS' AS status,
    	CURRENT_TIMESTAMP() AS start_time,
    	CURRENT_TIMESTAMP() AS end_time,
    	CAST(NULL AS STRING) AS error_message

	FROM `{PROJECT_ID}.bronze.location_raw`

	) S

	ON
	T.source_file = S.source_file
	AND T.layer = S.layer
	AND T.target_table = S.target_table
	WHEN NOT MATCHED THEN
	INSERT (
    	pipeline_name,
    	layer,
    	source_file,
    	target_table,
    	records_loaded,
   	status,
    	start_time,
    	end_time,
    	error_message
	)

     VALUES (
    	S.pipeline_name,
    	S.layer,
    	S.source_file,
    	S.target_table,
    	S.records_loaded,
    	S.status,
    	S.start_time,
    	S.end_time,
    	S.error_message
     );

	""",
          	 "useLegacySql": False,
       	 }
    	},
	location="US",
      )

    load_product = GCSToBigQueryOperator(
        task_id="load_product",
        bucket=BUCKET_NAME,
        location="US",
        source_objects=["GCP_Files/source_erp/PX_CAT_G1V2.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.bronze.product_raw",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
	create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    )
    
    audit_product = BigQueryInsertJobOperator(
    	task_id="audit_product",
    	configuration={
        "query": {
            "query": f"""
	MERGE `{PROJECT_ID}.audit.pipeline_audit` T
	USING (
     SELECT
    	'customer_etl_pipeline' AS pipeline_name,
    	'BRONZE' AS layer,
   	'PX_CAT_G1V2.csv' AS source_file,
    	'product_raw' AS target_table,
    	COUNT(*) AS records_loaded,
    	'SUCCESS' AS status,
    	CURRENT_TIMESTAMP() AS start_time,
    	CURRENT_TIMESTAMP() AS end_time,
    	CAST(NULL AS STRING) AS error_message
     FROM `{PROJECT_ID}.bronze.product_raw`
	) S
     ON
	T.source_file = S.source_file
	AND T.layer = S.layer
	AND T.target_table = S.target_table
	WHEN NOT MATCHED THEN
     INSERT (
    	pipeline_name,
    	layer,
    	source_file,
    	target_table,
    	records_loaded,
    	status,
    	start_time,
    	end_time,
    	error_message
	)
    VALUES (
    	S.pipeline_name,
    	S.layer,
    	S.source_file,
    	S.target_table,
    	S.records_loaded,
    	S.status,
    	S.start_time,
    	S.end_time,
    	S.error_message
      );
   	 """,
        "useLegacySql": False,
        }
   	 },
	location="US",
    )
    
    create_audit_table = BigQueryInsertJobOperator(
    task_id="create_audit_table",
    configuration={
        "query": {
            "query": f"""
            CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.audit.pipeline_audit`
            (
                pipeline_name STRING,
                layer STRING,
                source_file STRING,
                target_table STRING,
                records_loaded INT64,
                status STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                error_message STRING
            )
            """,
            "useLegacySql": False
        }
    },
    location="US"
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >>create_audit_table>>load_customer>>audit_customer>>load_location>>audit_location>>load_product>>audit_product>>end