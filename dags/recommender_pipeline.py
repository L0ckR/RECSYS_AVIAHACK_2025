"""
Airflow DAG orchestrating the full recommendation training pipeline:
- ingest source data from S3-compatible storage
- feature engineering on Yandex Data Proc (Spark)
- model training with MLflow logging
- artifact export to an online serving bucket

The DAG is parameterized by data partitions and backfill windows and includes
monitoring callbacks to notify on SLA misses and failures.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Iterable, List

import mlflow
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)


def _notify_slack(context) -> None:
    """Send a short notification to Slack when a failure or SLA miss occurs."""
    hook = SlackWebhookHook(http_conn_id="slack_alerts")
    dag_id = context.get("dag_run").dag_id if context.get("dag_run") else "unknown_dag"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown_task"
    message = f"Airflow alert for {dag_id}.{task_id}: state={context.get('state', 'unknown')}"
    logger.info("Sending slack alert: %s", message)
    hook.send(text=message)


def _resolve_partitions(data_partition: str, backfill_start: str | None, backfill_end: str | None) -> List[str]:
    """Build a list of date-partitions to process inclusive of backfill start/end."""
    start = datetime.strptime(backfill_start or data_partition, "%Y-%m-%d").date()
    end = datetime.strptime(backfill_end or data_partition, "%Y-%m-%d").date()
    step = timedelta(days=1)
    partitions: List[str] = []
    while start <= end:
        partitions.append(start.isoformat())
        start += step
    return partitions


def ingest_from_s3(bucket: str, prefix: str, staging_dir: str, data_partition: str, backfill_start: str | None, backfill_end: str | None, **context) -> List[str]:
    """Pulls partitioned parquet files from an S3 bucket into a staging area for Spark."""
    s3 = S3Hook(aws_conn_id="aws_default")
    partitions = _resolve_partitions(data_partition, backfill_start, backfill_end)
    staged_paths: List[str] = []

    for partition in partitions:
        partition_prefix = f"{prefix}/dt={partition}"
        keys = s3.list_keys(bucket_name=bucket, prefix=partition_prefix) or []
        logger.info("Found %s objects under %s", len(keys), partition_prefix)
        for key in keys:
            dest = f"{staging_dir}/{key.split('/')[-1]}"
            logger.info("Downloading %s to %s", key, dest)
            s3.download_file(key=key, bucket_name=bucket, local_path=dest)
            staged_paths.append(dest)

    context["ti"].xcom_push(key="staged_paths", value=staged_paths)
    context["ti"].xcom_push(key="partitions", value=partitions)
    return staged_paths


def emit_monitoring_metrics(partitions: Iterable[str], run_id: str | None, **_: dict) -> None:
    """Publish lightweight monitoring metrics for observability systems."""
    partitions = list(partitions or [])
    logger.info("Publishing metrics for MLflow run %s and partitions %s", run_id, partitions)
    metrics_payload = {"partitions": partitions, "run_id": run_id}
    logger.info("Monitoring payload: %s", json.dumps(metrics_payload))


def train_and_log(partitions: Iterable[str], tracking_uri: str, experiment: str, model_registry: str, **context) -> str:
    """Train the recommender model and log artifacts to MLflow."""
    partitions = list(partitions or [])
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)

    with mlflow.start_run(run_name=f"recommender-{','.join(partitions)}") as run:
        logger.info("Training model for partitions: %s", partitions)
        mlflow.log_params({"partitions": list(partitions), "registry": model_registry})

        # Replace with the actual training logic.
        dummy_metric = 0.99
        mlflow.log_metric("nDCG@10", dummy_metric)

        # Export a lightweight placeholder artifact.
        artifact_content = "model: recommender\nmetric: 0.99\n"
        artifact_path = "/tmp/model_card.txt"
        with open(artifact_path, "w", encoding="utf-8") as handle:
            handle.write(artifact_content)
        mlflow.log_artifact(artifact_path, artifact_path="cards")

        # Register model for serving.
        mlflow.register_model(model_uri=run.info.artifact_uri, name=model_registry)

        context["ti"].xcom_push(key="run_id", value=run.info.run_id)
        context["ti"].xcom_push(key="model_uri", value=run.info.artifact_uri)
        logger.info("Logged MLflow run %s", run.info.run_id)
        return run.info.run_id


def export_artifacts(destination_bucket: str, serving_prefix: str, model_uri: str, **context) -> str:
    """Copy artifacts from MLflow to an S3-compatible serving bucket for online access."""
    s3 = S3Hook(aws_conn_id="aws_default")
    artifact_key = f"{serving_prefix}/model_export.txt"
    logger.info("Exporting %s to s3://%s/%s", model_uri, destination_bucket, artifact_key)
    temp_path = "/tmp/model_export.txt"
    with open(temp_path, "w", encoding="utf-8") as handle:
        handle.write(f"model_uri: {model_uri}\n")
    s3.load_file(filename=temp_path, key=artifact_key, bucket_name=destination_bucket, replace=True)
    context["ti"].xcom_push(key="export_key", value=artifact_key)
    return artifact_key


default_args = {
    "owner": "ml-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "email": ["ml-oncall@example.com"],
    "sla": timedelta(hours=2),
    "on_failure_callback": _notify_slack,
}


with DAG(
    dag_id="recommender_pipeline",
    description="Daily training pipeline for the recommender system",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params={
        "data_partition": Param(default=None, type="string", description="Date partition to process; defaults to ds"),
        "backfill_start": Param(default=None, type="string", description="Backfill start date (YYYY-MM-DD)"),
        "backfill_end": Param(default=None, type="string", description="Backfill end date (YYYY-MM-DD)"),
        "mlflow_tracking_uri": Param(default="http://mlflow:5000", type="string", description="MLflow tracking URI"),
        "mlflow_experiment": Param(default="recommender", type="string", description="Experiment name"),
        "mlflow_registry": Param(default="recommender_registry", type="string", description="Model registry name"),
        "serving_bucket": Param(default="recsys-serving", type="string", description="S3 bucket for serving artifacts"),
        "serving_prefix": Param(default="models/recommender", type="string", description="Prefix in serving bucket"),
    },
    sla_miss_callback=_notify_slack,
    tags=["recsys", "mlops", "yandex"],
) as dag:
    ingestion = PythonOperator(
        task_id="ingest_from_s3",
        python_callable=ingest_from_s3,
        op_kwargs={
            "bucket": "{{ var.value.s3_raw_bucket }}",
            "prefix": "{{ var.value.s3_raw_prefix }}",
            "staging_dir": "/tmp/staging",
            "data_partition": "{{ params.data_partition or ds }}",
            "backfill_start": "{{ params.backfill_start or (dag_run.conf.get('backfill_start') if dag_run else None) }}",
            "backfill_end": "{{ params.backfill_end or (dag_run.conf.get('backfill_end') if dag_run else None) }}",
        },
        retries=1,
        task_concurrency=4,
    )

    with TaskGroup(group_id="feature_engineering") as feature_engineering:
        spark_job = SparkSubmitOperator(
            task_id="spark_feature_engineering",
            application="s3a://recsys-jobs/feature_engineering.py",
            conn_id="dataproc_spark",
            conf={
                "spark.yarn.maxAppAttempts": "2",
                "spark.dynamicAllocation.enabled": "true",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            },
            application_args=[
                "--input", "{{ ti.xcom_pull(task_ids='ingest_from_s3', key='staged_paths') }}",
                "--output", "s3a://recsys-features/dt={{ params.data_partition or ds }}",
                "--backfill-start", "{{ (ti.xcom_pull(task_ids='ingest_from_s3', key='partitions') or [params.data_partition or ds])[0] }}",
                "--backfill-end", "{{ (ti.xcom_pull(task_ids='ingest_from_s3', key='partitions') or [params.data_partition or ds])[-1] }}",
            ],
            executor_cores=4,
            executor_memory="4g",
            num_executors=6,
        )

    training = PythonOperator(
        task_id="train_and_log_model",
        python_callable=train_and_log,
        op_kwargs={
            "partitions": "{{ ti.xcom_pull(task_ids='ingest_from_s3', key='partitions') or [params.data_partition or ds] }}",
            "tracking_uri": "{{ params.mlflow_tracking_uri }}",
            "experiment": "{{ params.mlflow_experiment }}",
            "model_registry": "{{ params.mlflow_registry }}",
        },
    )

    export = PythonOperator(
        task_id="export_artifacts",
        python_callable=export_artifacts,
        op_kwargs={
            "destination_bucket": "{{ params.serving_bucket }}",
            "serving_prefix": "{{ params.serving_prefix }}/dt={{ params.data_partition or ds }}",
            "model_uri": "{{ ti.xcom_pull(task_ids='train_and_log_model', key='model_uri') }}",
        },
    )

    monitoring = PythonOperator(
        task_id="emit_monitoring_metrics",
        python_callable=emit_monitoring_metrics,
        op_kwargs={
            "partitions": "{{ ti.xcom_pull(task_ids='ingest_from_s3', key='partitions') }}",
            "run_id": "{{ ti.xcom_pull(task_ids='train_and_log_model', key='run_id') }}",
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )

    ingestion >> feature_engineering >> training >> export >> monitoring
