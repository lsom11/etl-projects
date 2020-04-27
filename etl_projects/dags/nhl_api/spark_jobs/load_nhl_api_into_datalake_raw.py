from etl_projects.adapters.nhl_api_adapter import NHLApiClientAdapter
from etl_projects.dags.nhl_api.nhl_api import RESOURCE_MAPPING, SOURCE
from etl_projects.loaders.s3_loader import S3Loader

def load_nhl_api_into_datalake():
    nhl_adapter = NHLApiClientAdapter()
    s3_loader = S3Loader()

    for k, v in RESOURCE_MAPPING:
        path = v["path"]
        res = nhl_adapter._request(path=path, method="get")
        data = res.json()
        s3_loader.upload_file(
            data, SOURCE, k)

