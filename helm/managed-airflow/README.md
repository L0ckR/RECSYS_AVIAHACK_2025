# Managed Airflow Helm chart

This umbrella chart pins values for deploying Apache Airflow with the recommender
DAGs on Yandex Managed Service for Kubernetes (MKS).

## Usage

```bash
helm dependency update helm/managed-airflow
helm upgrade --install recsys-airflow helm/managed-airflow \
  --namespace airflow --create-namespace
```

Key highlights:
- GitSync pulls DAGs from the repo specified in `values.yaml`.
- Connections provision S3/Object Storage (`aws_default`), the Yandex Data Proc Spark master (`dataproc_spark`), and Slack alerts.
- Variables capture S3 locations and MLflow tracking endpoints expected by the DAGs.
- Metrics and persistence are enabled to retain logs and export StatsD signals for monitoring.
