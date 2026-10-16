from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'AhmedHatem',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="ecommerce_etl_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Sqoop Extraction via SSHOperator to hive-server
    sqoop_extract_task = SSHOperator(
        task_id="sqoop_extract_task",
        ssh_conn_id='hive_server_ssh',
        command='bash /root/sqoop_ecommerce.sh '
    )

    # Spark Clean & Star Schema Task
    spark_submit_clean_cmd = """
spark-submit \
--master yarn \
--deploy-mode client \
--name ecommerce_clean_and_star_schema \
--conf spark.driver.memory=3g \
--conf spark.executor.memory=3g \
--conf spark.executor.cores=2 \
--conf spark.executor.instances=2 \
--conf spark.sql.shuffle.partitions=8 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.driver.bindAddress=0.0.0.0 \
--conf spark.driver.port=40000 \
--conf spark.driver.blockManager.port=40001 \
--conf spark.blockManager.port=40002 \
/root/airflow/dags/scripts/clean_ecommerce.py
"""
    spark_clean_and_build_star_schema = BashOperator(
        task_id='spark_clean_and_build_star_schema',
        bash_command=spark_submit_clean_cmd
    )

    # Spark Aggregation & Data Marts Task
    spark_submit_agg_cmd = """
spark-submit \
--master yarn \
--deploy-mode client \
--name ecommerce_marts_pipeline \
--conf spark.driver.memory=3g \
--conf spark.executor.memory=3g \
--conf spark.executor.cores=2 \
--conf spark.executor.instances=2 \
--conf spark.sql.shuffle.partitions=8 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.sql.autoBroadcastJoinThreshold=-1 \
--conf spark.driver.bindAddress=0.0.0.0 \
--conf spark.driver.port=40000 \
--conf spark.driver.blockManager.port=40001 \
--conf spark.blockManager.port=40002 \
/root/airflow/dags/scripts/agg_ecommerce.py
"""
    spark_aggregate_and_build_marts = BashOperator(
        task_id='spark_aggregate_and_build_marts',
        bash_command=spark_submit_agg_cmd
    )

    # Pipeline Dependencies Definition
    sqoop_extract_task >> spark_clean_and_build_star_schema >> spark_aggregate_and_build_marts