import logging
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)


class BaseHttpApiAdapter:
    SUCCESS_CODE = 200
    CREATED_CODE = 201

    INTERNAL_SERVER_ERROR_CODE = 500
    BAD_GATEWAY_CODE = 502
    SERVICE_UNAVAILABLE_CODE = 503
    GATEWAY_TIMEOUT_CODE = 504

    def __init__(self, url, user=None, password=None):
        self.url = url
        self.user = user
        self.password = password
        self.default_headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "Accept": "application/json",
        }
        retries = Retry(
            total=3,
            raise_on_status=False,
            status_forcelist=[
                self.INTERNAL_SERVER_ERROR_CODE,
                self.BAD_GATEWAY_CODE,
                self.SERVICE_UNAVAILABLE_CODE,
                self.GATEWAY_TIMEOUT_CODE,
            ],
        )
        self.session = requests.Session()
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def get(self, **kwargs):
        return self._request(method="get", **kwargs)

    def post(self, **kwargs):
        return self._request(method="post", **kwargs)

    def put(self, **kwargs):
        return self._request(method="put", **kwargs)

    def _request(self, method, **kwargs):
        request_params = self._get_request_parameters(**kwargs)
        logger.info(
            f"[BaseHttpApiAdapter] request {method.upper()} params: {request_params}"
        )
        return self.session.request(method=method, **request_params)

    def _get_request_parameters(self, path="", data=None, params={}, headers={}):
        return {
            "url": f"{self.url}/{path}",
            "params": params,
            "data": data,
        }
