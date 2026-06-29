pipeline {

    agent any

    environment {
        PROJECT_ID = "etl-demo-project-500808"
        REGION = "us-central1"
        COMPOSER_ENV = "etl-composer-02"
        COMPOSER_BUCKET = "us-central1-etl-composer-02-622ba58e-bucket"
    }

    stages {

        stage('Checkout') {
            steps {
                echo "Checking out GitHub Repository..."
                checkout scm
            }
        }

        stage('Verify Files') {
            steps {
                sh '''
                pwd
                ls -R
                '''
            }
        }

        stage('Deploy DAG') {
            steps {
                sh '''
                echo "Copying Bronze DAG to composer..."
        	gcloud storage cp source_erp_1.py gs://${COMPOSER_BUCKET}/dags/
                echo "Copying sliver dag to Composer..."
                gcloud storage cp silver_ETL.py gs://${COMPOSER_BUCKET}/dags/
                '''
            }
        }

        stage('Wait for airflow') {
            steps {
		echo "Waiting 5 minutes for Airflow to detect the DAG..."	
                sleep(time:5, unit:'MINUTES')
            }
        }

        stage('Trigger Airflow DAG') {
            steps {
                sh '''
                gcloud composer environments run ${COMPOSER_ENV} \
                --location ${REGION} \
                dags trigger -- customer_etl_pipeline_007
                '''
            }
        }
	
	 stage('Wait for Bronze DAG Success') {
    	     steps {
        	sh '''
        	while true
        	do
          	STATUS=$(gcloud composer environments run etl-composer-02 \
		--location us-central1 dags list-runs -- \
		--dag-id customer_etl_pipeline_007 | grep -E "success|failed|running|queued" | head -1)

		echo "$STATUS"

	if echo "$STATUS" | grep -q "success"; then
   	 	echo "SUCCESS"
   	 break
		elif echo "$STATUS" | grep -q "failed"; then
    	echo "FAILED"
   	 exit 1
	fi

	sleep 20
        done
        '''
	    }
	}
	stage('Trigger Silver DAG') {
    	    steps {
       	    	sh '''
            	gcloud composer environments run ${COMPOSER_ENV} \
        	--location ${REGION} \
        	dags trigger -- silver_etl_pipeline_002
        	'''
   		 }
	}
	
	stage('Wait for Silver DAG Success') {
    	    steps {
        	sh '''
        	while true
		do
    		STATUS=$(gcloud composer environments run etl-composer-02 \
        	--location us-central1 dags list-runs -- \
        	--dag-id silver_etl_pipeline_002 | grep -E "success|failed|running|queued" | head -1)

    		echo "$STATUS"

    	if echo "$STATUS" | grep -q "success"; then
       	 	echo "SUCCESS"
        	break
    	elif echo "$STATUS" | grep -q "failed"; then
        	echo "FAILED"
        exit 1
    fi

    sleep 20
	done
        '''

	}

    }

    post {

        success {
            echo "Pipeline completed successfully."
        }

        failure {
            echo "Pipeline failed."
        }

    }

	}
}