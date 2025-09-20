from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

DAGS_FOLDER = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
GIT_REPO_HTTPS = os.environ.get("GIT_REPO_HTTPS")
GIT_TOKEN = os.environ.get("GIT_TOKEN")
GIT_REPO_SSH = os.environ.get("GIT_REPO_SSH")
SSH_KEY_PATH = os.environ.get("SSH_KEY_PATH")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="git_dags_updater",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ci_cd"],
) as dag:

    # Task: pull DAGs from GitHub
    git_pull_task = BashOperator(
        task_id="update_dags_from_github",
        bash_command=f"""
        cd {DAGS_FOLDER} || exit 1

        if [ -n "{GIT_TOKEN}" ] && [ -n "{GIT_REPO_HTTPS}" ]; then
            echo "Pulling DAGs via HTTPS..."
            git pull {GIT_REPO_HTTPS} main || echo "HTTPS pull failed"
        elif [ -n "{GIT_REPO_SSH}" ] && [ -f "{SSH_KEY_PATH}" ]; then
            echo "Pulling DAGs via SSH..."
            eval "$(ssh-agent -s)"
            ssh-add {SSH_KEY_PATH}
            git pull {GIT_REPO_SSH} main || echo "SSH pull failed"
        else
            echo "No valid Git credentials provided"
        fi
        """,
    )
