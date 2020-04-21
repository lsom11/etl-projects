from datetime import datetime
import pendulum

from airflow.models import DAG, Variable

from etl_projects.dags.nhl_api import SOURCE
from etl_projects.base.airflow import BaseDAG

DAG_ID = SOURCE
# DATALAKE_BUCKET = Variable.get("datalake_bucket")
# ATHENA_QUERY_RESULT_LOCATION = Variable.get("athena_query_result_location")

# project paths
S3_PREFIX = Variable.get("s3_prefix")
SPARK_JOBS_PATH = f"{S3_PREFIX}/spark_jobs/{SOURCE}/"

# dag params
# use cron expressions in local time
local_tz = pendulum.timezone("America/Sao_Paulo")
MAIN_START_DATE = datetime(2019, 5, 31, 0, 0, 0, tzinfo=local_tz)
MAIN_SCHEDULE_INTERVAL = "0 1 * * *"
JOB_LOCATION = SPARK_JOBS_PATH + "load_nhl_api_into_datalake.py"

dag = DAG(
    dag_id=DAG_ID,
    default_args={
        "owner": BaseDAG.DEFAULT_OWNER,
        "wait_for_downstream": False,
        "depends_on_past": False,
    },
    start_date=MAIN_START_DATE,
    schedule_interval=MAIN_SCHEDULE_INTERVAL,
)


nhl_api_to_datalake_raw_task = BaseDAG.build_bash_operator(
    task_id="nhl_api-to-datalake-raw",
    dag=dag,
    bash_command=f"python {JOB_LOCATION}",
    provide_context=True,
)


nhl_api_to_datalake_raw_task
