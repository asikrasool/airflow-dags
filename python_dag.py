from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

# Define your DAG parameters
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG
dag = DAG(
    'spark_job_dag',
    default_args=default_args,
    description='A DAG to run a Spark job',
    schedule_interval=timedelta(days=1),  # Adjust the schedule interval as needed
)

# Define the Spark job task using SparkSubmitOperator
spark_job_task = SparkSubmitOperator(
    task_id='submit_spark_job',
    conn_id='kubernetes_default',  # Set the Airflow connection ID for Spark
    application='/kubernetes/hello_world.py',  # Path to your Spark job script or JAR file
    name='python_dag',
    verbose=True,
    dag=dag,
)

# Set the DAG dependencies (if needed)
# task2.set_upstream(task1)

# You can add more tasks and dependencies as needed

# Set the DAG dependencies (if needed)
# dag.set_upstream(another_dag)

# Alternatively, you can use the set_downstream method to set dependencies in the opposite direction
# another_dag.set_downstream(dag)
