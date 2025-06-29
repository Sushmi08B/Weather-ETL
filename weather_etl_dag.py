from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pendulum import timezone

# This DAG is designed to run a daily ETL pipeline for weather data.
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email': ['abc@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': True  
}

# The DAG is scheduled to run daily at 21:05 in the America/Chicago timezone.
with DAG(
    dag_id='daily_weather_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 6, 7, 21, 5, tzinfo=timezone("America/Chicago")),
    schedule_interval='5 21 * * *', 
    catchup=False
) as dag:
    # The DAG consists of three tasks: extract, transform, and load.
    # Each task is defined using the BashOperator, which allows us to run bash commands.
    # The bash commands are set to execute Python scripts that perform the respective ETL operations.
    extract = BashOperator(
        task_id='extract_data',
        bash_command="""
        export PYTHONPATH=$PWD/spark_venv/lib/python3.11/site-packages:$PYTHONPATH
        /opt/homebrew/Cellar/apache-spark/4.0.0/bin/spark-submit \
        /PySparkProject/WeatherETL/extract.py"""
    )

    transform = BashOperator(
        task_id='transform_data',
        bash_command="""
        export PYTHONPATH=$PWD/spark_venv/lib/python3.11/site-packages:$PYTHONPATH
        /opt/homebrew/Cellar/apache-spark/4.0.0/bin/spark-submit \
        /PySparkProject/WeatherETL/transform.py"""
    )

    load = BashOperator(
        task_id='load_data',
        bash_command="""
        export PYTHONPATH=$PWD/spark_venv/lib/python3.11/site-packages:$PYTHONPATH
        /opt/homebrew/Cellar/apache-spark/4.0.0/bin/spark-submit \
        --jars /PySparkProject/WeatherETL/postgresql-42.7.6.jar \
        /PySparkProject/WeatherETL/load.py"""
    )

    extract >> transform >> load
