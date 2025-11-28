#!/usr/bin/env bash
set -euo pipefail

# Bootstraps a Yandex Managed Airflow environment and a Yandex Data Proc
# cluster capable of running the recommender pipeline DAGs.
#
# Expected environment variables:
#   YC_FOLDER_ID: target folder id
#   YC_SERVICE_ACCOUNT_ID: service account with storage and dataproc roles
#   YC_BUCKET: object storage bucket for DAGs and Spark inputs
#   YC_REGION: region (e.g., ru-central1)
#   YC_ZONE: availability zone for Data Proc nodes
#
# Optional overrides:
#   AIRFLOW_ENV_NAME: name of the managed Airflow environment
#   DATAPROC_CLUSTER_NAME: name of the Data Proc cluster
#   GIT_REPO_URL: repository containing DAGs
#   GIT_DAGS_BRANCH: branch to sync DAGs from

: "${YC_FOLDER_ID:?YC_FOLDER_ID is required}"
: "${YC_SERVICE_ACCOUNT_ID:?YC_SERVICE_ACCOUNT_ID is required}"
: "${YC_BUCKET:?YC_BUCKET is required}"
: "${YC_REGION:?YC_REGION is required}"
: "${YC_ZONE:?YC_ZONE is required}"

AIRFLOW_ENV_NAME=${AIRFLOW_ENV_NAME:-recsys-airflow}
DATAPROC_CLUSTER_NAME=${DATAPROC_CLUSTER_NAME:-recsys-dataproc}
GIT_REPO_URL=${GIT_REPO_URL:-"https://github.com/example/recsys.git"}
GIT_DAGS_BRANCH=${GIT_DAGS_BRANCH:-"main"}

# Create storage bucket for DAGs, Spark jobs, and serving artifacts.
yc storage bucket create --name "${YC_BUCKET}" --folder-id "${YC_FOLDER_ID}" --region "${YC_REGION}" || true

# Upload DAGs and Spark jobs.
yc storage cp --recursive ./dags "s3://${YC_BUCKET}/dags"
yc storage cp --recursive ./jobs "s3://${YC_BUCKET}/jobs" || true

# Create Airflow environment with Git-sync enabled for DAG delivery.
yc managed-airflow environment create \
  --name "${AIRFLOW_ENV_NAME}" \
  --folder-id "${YC_FOLDER_ID}" \
  --service-account-id "${YC_SERVICE_ACCOUNT_ID}" \
  --webserver-public-ip \
  --dags-git-repo "${GIT_REPO_URL}" \
  --dags-git-branch "${GIT_DAGS_BRANCH}" \
  --dags-git-subdir "dags"

# Store variables and connections for the DAG at environment level.
yc managed-airflow variable set --environment-name "${AIRFLOW_ENV_NAME}" --key s3_raw_bucket --value "${YC_BUCKET}"
yc managed-airflow variable set --environment-name "${AIRFLOW_ENV_NAME}" --key s3_raw_prefix --value "raw/events"

yc managed-airflow connection create aws \
  --environment-name "${AIRFLOW_ENV_NAME}" \
  --conn-id aws_default \
  --conn-type aws \
  --access-key-id "$YC_ACCESS_KEY" \
  --secret-access-key "$YC_SECRET_KEY" \
  --extra "{\"region_name\": \"${YC_REGION}\"}"

# Data Proc cluster sized for daily feature engineering jobs.
yc dataproc cluster create "${DATAPROC_CLUSTER_NAME}" \
  --folder-id "${YC_FOLDER_ID}" \
  --zone-id "${YC_ZONE}" \
  --service-account-id "${YC_SERVICE_ACCOUNT_ID}" \
  --version 2.0 \
  --services yarn,spark,zeppelin \
  --subcluster name="master",role=master,resource-preset=m3-c4-m16,disk-type=network-ssd,disk-size=100 \
  --subcluster name="workers",role=data,resource-preset=m3-c8-m32,disk-type=network-ssd,disk-size=200,hosts-count=3 \
  --ui-proxy

# Ensure MLflow tracking URI is available to the environment.
yc managed-airflow variable set --environment-name "${AIRFLOW_ENV_NAME}" --key mlflow_tracking_uri --value "http://mlflow:5000"

cat <<'EOF'
echo "Deployment complete. Verify webserver URL via:"
echo "yc managed-airflow environment get --name ${AIRFLOW_ENV_NAME}"
EOF
