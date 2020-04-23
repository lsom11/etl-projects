from etl_projects.adapters.nhl_api_adapter import NHLApiClientAdapter
from etl_projects.dags.nhl_api.nhl_api import RESOURCE_MAPPING


if __name__ == "__main__":
    nhl_adapter = NHLApiClientAdapter()
    for resource in RESOURCE_MAPPING.values():
        path = resource["path"]
        res = nhl_adapter._request(path=path, method="get")
        data = res.json()
