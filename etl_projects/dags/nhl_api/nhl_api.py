from datetime import datetime
import pendulum
import json
import boto3

from airflow.models import DAG, Variable

from etl_projects.dags.nhl_api import SOURCE
from etl_projects.base.airflow import BaseDAG
from etl_projects.adapters.nhl_api_adapter import NHLApiClientAdapter
from etl_projects.dags.nhl_api import RESOURCE_MAPPING, SOURCE
# from etl_projects.loaders.s3_loader import S3Loader

DAG_ID = SOURCE

# dag params
# use cron expressions in local time
local_tz = pendulum.timezone("America/Sao_Paulo")
MAIN_START_DATE = datetime(2020, 4, 27, 0, 0, 0, tzinfo=local_tz)
MAIN_SCHEDULE_INTERVAL = "0 1 * * *"


def upload_file(json_data, bucket, file_name):
        s3_resource = boto3.resource('s3')
        s3_object = s3_resource.Object(bucket, f'{file_name}.json')

        s3_object.put(
            Body=(bytes(json.dumps(json_data).encode('UTF-8')))
        )

def load_nhl_api_into_datalake():
    nhl_adapter = NHLApiClientAdapter()
    # s3_loader = S3Loader()

    for k, v in RESOURCE_MAPPING:
        path = v["path"]
        res = nhl_adapter._request(path=path, method="get")
        data = res.json()
        # s3_loader.upload_file(
        #     data, SOURCE, k)



dag = DAG(
    dag_id=DAG_ID,
    default_args={
        "owner": BaseDAG.DEFAULT_OWNER,
        "wait_for_downstream": False,
        "depends_on_past": False,
    },
    start_date=MAIN_START_DATE,
    schedule_interval=MAIN_SCHEDULE_INTERVAL,
    catchup=False
)


nhl_api_to_datalake_raw_task = BaseDAG.build_python_operator(
    task_id="nhl_api-to-datalake-raw",
    dag=dag,
    python_callable=load_nhl_api_into_datalake,
    provide_context=True,
)


nhl_api_to_datalake_raw_task
