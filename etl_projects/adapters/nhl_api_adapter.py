from etl_projects.adapters.base_http_adapter import BaseHttpApiAdapter


class NHLApiClientAdapter(BaseHttpApiAdapter):
    BASE_URL = "https://statsapi.web.nhl.com/api/v1/"

    def __init__(self):
        super().__init__(url=self.BASE_URL)
