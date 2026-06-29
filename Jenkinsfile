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
		echo "Copying Bronze DAG..."
        	gcloud storage cp source_erp_1.py gs://${COMPOSER_BUCKET}/dags/
                echo "Copying DAG to Composer..."
                gcloud storage cp silver_ETL.py gs://${COMPOSER_BUCKET}/dags/
                '''
            }
        }

	stage('Wait for Airflow Sync') {
    	    steps {
        	echo "Waiting 2 minutes for Composer to detect DAGs..."
        	sleep(time: 4, unit: 'MINUTES')
    	    }
	}

        stage('Trigger bronze DAG') {
            steps {
                sh '''
                gcloud composer environments run ${COMPOSER_ENV} \
                --location ${REGION} \
                dags trigger -- customer_etl_pipeline_007
                '''
            }
        }

	stage('Wait for Bronze Completion') {
    	    steps {
        	echo "Waiting for Bronze DAG to complete..."
        	sleep(time: 3, unit: 'MINUTES')
    	    }
	}

	stage('Trigger Silver DAG') {
    	   steps {
        	sh '''
        	gcloud composer environments run ${COMPOSER_ENV} \
        	--location ${REGION} \
        	dags trigger -- silver_etl_pipeline_001
        	'''
    	    }
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