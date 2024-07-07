from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.providers.slack.notifications.slack import send_slack_notification


args = {
    'owner': 'Airflow',
}

with DAG(
        dag_id='data_pipeline',
        default_args=args,
        schedule_interval='0 0 * * *',
        start_date=days_ago(2),
        tags=['data_pipeline', 'interactions'],
        on_failure_callback=[
            send_slack_notification(
                text="The DAG {{ dag.dag_id }} succeeded",
                channel="#general",
                username="Airflow",
            )],
        on_success_callback=[
            send_slack_notification(
                text="The DAG {{ dag.dag_id }} succeeded",
                channel="#general",
                username="Airflow",
            )]

) as dag:
    # [START howto_operator_spark_submit]

    python_submit_job = SparkSubmitOperator(
        application="/usr/local/spark/src/app/insight_app.py",
        task_id="python_job",
        name = "ETL",
        conn_id="spark_default",
        py_files='/usr/local/spark/src/app/insight_job.py',
        dag=dag
    )