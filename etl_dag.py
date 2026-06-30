from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (GCSToBigQueryOperator)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

PROJECT_ID = "etl-demo-project-500808"
BUCKET_NAME = "landing_zone_002"

default_args = {
    "owner": "padmasai",
    "retries": 0
}

with DAG(
    dag_id="etl_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["gcp", "etl"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )
	
    with TaskGroup(group_id="bronze_layer") as bronze_group:

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
        
        audit_customer_bronze = BigQueryInsertJobOperator(
            task_id="audit_customer_bronze",
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
        
        audit_location_bronze = BigQueryInsertJobOperator(
            task_id="audit_location_bronze",
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
        
        audit_product_bronze = BigQueryInsertJobOperator(
            task_id="audit_product_bronze",
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
        create_audit_table >> load_customer >> audit_customer_bronze >> \
        load_location >> audit_location_bronze >> \
        load_product >> audit_product_bronze

			
    with TaskGroup(group_id="silver_layer") as silver_group:
	
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
		FROM 
                (
		SELECT *,ROW_NUMBER() OVER(PARTITION BY CID ORDER BY BDATE DESC) rn
		FROM `{PROJECT_ID}.bronze.customer_raw`
		)
                WHERE rn=1 AND CID IS NOT NULL;
		""",
		"useLegacySql": False,
		}
		},
		location="US",
	)
	audit_customer_silver = BigQueryInsertJobOperator(
	task_id="audit_customer_silver",
	configuration={
		"query": {
			"query": f"""
		MERGE `{PROJECT_ID}.audit.pipeline_audit` T
		USING (

		SELECT
			'silver_etl_pipeline_002' AS pipeline_name,
			'SILVER' AS layer,
			'CUST_AZ12.csv' AS source_file,
			'customer' AS target_table,
			COUNT(*) AS records_loaded,
			'SUCCESS' AS status,
			CURRENT_TIMESTAMP() AS start_time,
			CURRENT_TIMESTAMP() AS end_time
		FROM `{PROJECT_ID}.silver.customer`
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


	audit_location_silver = BigQueryInsertJobOperator(
	task_id="audit_location_silver",
	configuration={
	"query": {
		"query": f"""

		MERGE `{PROJECT_ID}.audit.pipeline_audit` T
			USING (
		SELECT
			'silver_etl_pipeline_002' AS pipeline_name,
			'SILVER' AS layer,
			'LOC_A101.csv' AS source_file,
			'location' AS target_table,
			COUNT(*) AS records_loaded,
			'SUCCESS' AS status,
			CURRENT_TIMESTAMP() AS start_time,
			CURRENT_TIMESTAMP() AS end_time
		FROM `{PROJECT_ID}.silver.location`
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
		    WHEN MAINTENANCE = TRUE THEN 'YES'
		    WHEN MAINTENANCE = FALSE THEN 'NO'
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


	audit_product_silver = BigQueryInsertJobOperator(
	task_id="audit_product_silver",
	configuration={
	"query": {
		"query": f"""
		MERGE `{PROJECT_ID}.audit.pipeline_audit` T
		USING (
		SELECT
			'silver_etl_pipeline_002' AS pipeline_name,
			'SILVER' AS layer,
			'PX_CAT_G1V2.csv' AS source_file,
			'product' AS target_table,
			COUNT(*) AS records_loaded,
			'SUCCESS' AS status,
			CURRENT_TIMESTAMP() AS start_time,
			CURRENT_TIMESTAMP() AS end_time
		FROM `{PROJECT_ID}.silver.product`
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
		)
		""",
				"useLegacySql": False,
			}
		},
		location="US",
		)
        create_customer_silver >> audit_customer_silver >> \
        create_location_silver >> audit_location_silver >> \
        create_product_silver >> audit_product_silver
    
    with TaskGroup(group_id="gold_layer") as gold_group:

        gold_customer = BigQueryInsertJobOperator(
        task_id="gold_customer",
        configuration={
            "query": {
                "query": """
                CREATE OR REPLACE TABLE `etl-demo-project-500808.gold.customer` AS
                SELECT *
                FROM `etl-demo-project-500808.silver.customer`;
                """,
                "useLegacySql": False,
            }
        },
        )
	
	audit_customer_gold = BigQueryInsertJobOperator(
	task_id="audit_customer_gold",
	configuration={
	"query": {
		"query": f"""
		MERGE `{PROJECT_ID}.audit.pipeline_audit` T
		USING (
		SELECT
			'silver_etl_pipeline_002' AS pipeline_name,
			'GOLD' AS layer,
			'CUST_AZ12.csv' AS source_file,
			'customer' AS target_table,
			COUNT(*) AS records_loaded,
			'SUCCESS' AS status,
			CURRENT_TIMESTAMP() AS start_time,
			CURRENT_TIMESTAMP() AS end_time
		FROM `{PROJECT_ID}.gold.customer`
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
		)
		""",
				"useLegacySql": False,
			}
		},
		location="US",
		)

	gold_location = BigQueryInsertJobOperator(
	task_id="gold_location",
	configuration={
	"query": {
		"query": """
	CREATE OR REPLACE TABLE `etl-demo-project-500808.gold.location` AS
	SELECT *
		FROM `etl-demo-project-500808.silver.location`;
		""",
		"useLegacySql": False,
		}
		},
	)

	audit_location_gold = BigQueryInsertJobOperator(
	task_id="audit_location_gold",
	configuration={
	"query": {
		"query": f"""
	MERGE `{PROJECT_ID}.audit.pipeline_audit` T
	USING (
	SELECT
		'silver_etl_pipeline_002' AS pipeline_name,
		'GOLD' AS layer,
		'LOC_A101.csv' AS source_file,
		'location' AS target_table,
		COUNT(*) AS records_loaded,
		'SUCCESS' AS status,
		CURRENT_TIMESTAMP() AS start_time,
		CURRENT_TIMESTAMP() AS end_time
		FROM `{PROJECT_ID}.gold.location`
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
		)
		""",
				"useLegacySql": False,
			}
		},
		location="US",
	)

	gold_product = BigQueryInsertJobOperator(
	task_id="gold_product",
	configuration={
	"query": {
		"query": """
		CREATE OR REPLACE TABLE `etl-demo-project-500808.gold.product` AS
		SELECT *
		FROM `etl-demo-project-500808.silver.product`;
		""",
		"useLegacySql": False,
		}
		},
	)
		 
	audit_product_gold = BigQueryInsertJobOperator(
	task_id="audit_product_gold",
	configuration={
	query": {
		"query": f"""
		MERGE `{PROJECT_ID}.audit.pipeline_audit` T
		USING (
		SELECT
			'silver_etl_pipeline_002' AS pipeline_name,
			'GOLD' AS layer,
			'PX_CAT_G1V2.csv' AS source_file,
			'product' AS target_table,
			COUNT(*) AS records_loaded,
			'SUCCESS' AS status,
			CURRENT_TIMESTAMP() AS start_time,
			CURRENT_TIMESTAMP() AS end_time
		FROM `{PROJECT_ID}.gold.product`
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
		)
		""",
			"useLegacySql": False,
			}
		},
		location="US",
	)
        gold_customer >> audit_customer_gold >> \
        gold_location >> audit_location_gold >> \
        gold_product >> audit_product_gold

    end = EmptyOperator(
        task_id="end"
    )
    
    start >> bronze_group >> silver_group >> gold_group >> end
