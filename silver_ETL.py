from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator

PROJECT_ID = "etl-demo-project-500808"

default_args = {
    "owner": "padmasai",
    "retries": 0
}

with DAG(
    dag_id="silver_etl_pipeline_002",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
    tags=["gcp", "silver"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    create_customer_silver = BigQueryInsertJobOperator(
        task_id="create_customer_silver",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.customer` AS
                SELECT
                    TRIM(CID) AS customer_id,
                    BDATE AS birth_date,
                    CASE
                        WHEN UPPER(TRIM(GEN))='M' THEN 'Male'
                        WHEN UPPER(TRIM(GEN))='F' THEN 'Female'
                        ELSE 'Unknown'
                    END AS gender,
                    CURRENT_TIMESTAMP() AS load_timestamp
                    FROM (
                SELECT *,ROW_NUMBER() OVER(PARTITION BY CID ORDER BY BDATE DESC) rn
                    FROM `{PROJECT_ID}.bronze.customer_raw`
                    )WHERE rn=1 AND CID IS NOT NULL;
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )
    audit_customer = BigQueryInsertJobOperator(
    task_id="audit_customer",
    configuration={
        "query": {
            "query": f"""
            INSERT INTO `{PROJECT_ID}.audit.pipeline_audit`
            (
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

            SELECT
                'silver_etl_pipeline_001',
                'SILVER',
                'CUST_AZ12.csv',
                'customer',
                COUNT(*),
                'SUCCESS',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                NULL
            FROM `{PROJECT_ID}.silver.customer`
            WHERE NOT EXISTS (
            SELECT 1
                FROM `etl-demo-project-500808.audit.pipeline_audit`
                WHERE source_file='CUST_AZ12.csv'
                AND layer='SILVER'
                AND target_table='customer'
            );
            """,
            "useLegacySql": False,
        }
    },
    location="US",
    )
    
    create_location_silver = BigQueryInsertJobOperator(
        task_id="create_location_silver",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.location` AS
                SELECT
                    TRIM(string_field_0) AS customer_id,
                    TRIM(string_field_1) AS country,
                    CURRENT_TIMESTAMP() AS load_timestamp
                FROM
                    (
                SELECT *,
                    ROW_NUMBER() OVER(
                    PARTITION BY string_field_0 ORDER BY string_field_0) rn
                FROM `{PROJECT_ID}.bronze.location_raw`)
                    WHERE rn=1 AND string_field_0 IS NOT NULL;
                """,
                "useLegacySql": False,
            }
        },
        location="US",
    )


    audit_location = BigQueryInsertJobOperator(
    task_id="audit_location",
    configuration={
        "query": {
            "query": f"""
            INSERT INTO `{PROJECT_ID}.audit.pipeline_audit`
            (
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

            SELECT
                'silver_etl_pipeline_001',
                'SILVER',
                'LOC_A101.csv',
                'location',
                COUNT(*),
                'SUCCESS',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                NULL
            FROM `{PROJECT_ID}.silver.location`
            WHERE NOT EXISTS (
            SELECT 1
                FROM `etl-demo-project-500808.audit.pipeline_audit`
                WHERE source_file='LOC_A101.csv'
                AND layer='SILVER'
                AND target_table='location'
            );
            """,
            "useLegacySql": False,
        }
    },
    location="US",
    )
    
    create_product_silver = BigQueryInsertJobOperator(
        task_id="create_product_silver",
        configuration={
            "query": {
                "query": f"""
                 CREATE OR REPLACE TABLE `{PROJECT_ID}.silver.product` AS
             SELECT
                TRIM(ID) AS product_id,
                INITCAP(TRIM(CAT)) AS category,
                INITCAP(TRIM(SUBCAT)) AS sub_category,
                CASE
                    WHEN UPPER(TRIM(MAINTENANCE)) IN ('YES','Y','TRUE') THEN 'YES'
                    WHEN UPPER(TRIM(MAINTENANCE)) IN ('NO','N','FALSE') THEN 'NO'
                    ELSE 'UNKNOWN'
                END AS maintenance,
                CURRENT_TIMESTAMP() AS load_timestamp FROM
            (
            SELECT *,
                ROW_NUMBER() OVER
            (
                PARTITION BY ID
                ORDER BY ID
            ) rn
                FROM `{PROJECT_ID}.bronze.product_raw`
            )
                WHERE rn = 1 AND ID IS NOT NULL;
            """,
                "useLegacySql": False,
            }
        },
        location="US",
    )


    audit_product = BigQueryInsertJobOperator(
    task_id="audit_product",
    configuration={
        "query": {
            "query": f"""
            INSERT INTO `{PROJECT_ID}.audit.pipeline_audit`
            (
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

            SELECT
                'silver_etl_pipeline_001',
                'SILVER',
                'PX_CAT_G1V2.csv',
                'product',
                COUNT(*),
                'SUCCESS',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                NULL
            FROM `{PROJECT_ID}.silver.product`
            WHERE NOT EXISTS (
            SELECT 1
                FROM `etl-demo-project-500808.audit.pipeline_audit`
                WHERE source_file='PX_CAT_G1V2.csv'
                AND layer='SILVER'
                AND target_table='product'
            );
            """,
            "useLegacySql": False,
        }
    },
    location="US",
    )
    
    end = EmptyOperator(
        task_id="end"
    )

    start >> create_customer_silver >> audit_customer

    audit_customer >> create_location_silver >> audit_location

    audit_location >> create_product_silver >> audit_product

    audit_product >> end

    
