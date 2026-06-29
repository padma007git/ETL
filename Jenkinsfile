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
                echo "Copying DAG to Composer..."
                gcloud storage cp source_erp_1.py gs://${COMPOSER_BUCKET}/dags/
                '''
            }
        }

        stage('Wait') {
            steps {
                sleep(time:30, unit:'SECONDS')
            }
        }

        stage('Trigger Airflow DAG') {
            steps {
                sh '''
                gcloud composer environments run ${COMPOSER_ENV} \
                --location ${REGION} \
                dags trigger -- customer_etl_pipeline_final
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