# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import requests
import six
import time
from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import timedelta
from requests import exceptions as requests_exceptions
from requests.auth import AuthBase
from time import sleep

# --------------------------------------------------------------------------------------
# |                                                                                    |
# |                                                                                    |
# |                                 HOOKS                                              |
# |                                                                                    |
# |                                                                                    |
# --------------------------------------------------------------------------------------

try:
    from urllib import parse as urlparse
except ImportError:
    import urlparse

RESTART_CLUSTER_ENDPOINT = ("POST", "api/2.0/clusters/restart")
START_CLUSTER_ENDPOINT = ("POST", "api/2.0/clusters/start")
CREATE_CLUSTER_ENDPOINT = ("POST", "api/2.0/clusters/create")
TERMINATE_CLUSTER_ENDPOINT = ("POST", "api/2.0/clusters/delete")
GET_CLUSTER_ENDPOINT = ("GET", "api/2.0/clusters/get")

INSTALL_LIBRARIES_ENDPOINT = ("POST", "api/2.0/libraries/install")
LIBRARIES_CLUSTER_STATUS_ENDPOINT = ("GET", "api/2.0/libraries/cluster-status")

RUN_NOW_ENDPOINT = ("POST", "api/2.0/jobs/run-now")
SUBMIT_RUN_ENDPOINT = ("POST", "api/2.0/jobs/runs/submit")
GET_RUN_ENDPOINT = ("GET", "api/2.0/jobs/runs/get")
CANCEL_RUN_ENDPOINT = ("POST", "api/2.0/jobs/runs/cancel")

USER_AGENT_HEADER = {"user-agent": "airflow-{v}".format(v=__version__)}


def get_xcom_value(context, key):
    """
    Get the xcom value pushed by the DAG of the current task or by the parent DAG in
    case of existing.
    :param context: Execution context
    :param key: xcom key
    :return: xcom value
    """
    xcom_value = context["task_instance"].xcom_pull(key=key)
    # if not found, check if the xcom is in the parent dag in case of existing
    if not xcom_value and context["dag"].is_subdag:
        xcom_value = context["task_instance"].xcom_pull(
            dag_id=context["dag"].parent_dag.dag_id, key=key
        )
    return xcom_value


class ETLProjectsDatabricksHook(BaseHook, LoggingMixin):
    """
    Interact with Databricks.
    """

    def __init__(
        self,
        databricks_conn_id="databricks_default",
        timeout_seconds=180,
        retry_limit=3,
        retry_delay=1.0,
    ):
        """
        :param databricks_conn_id: The name of the databricks connection to use.
        :type databricks_conn_id: string
        :param timeout_seconds: The amount of time in seconds the requests library
            will wait before timing-out.
        :type timeout_seconds: int
        :param retry_limit: The number of times to retry the connection in case of
            service outages.
        :type retry_limit: int
        :param retry_delay: The number of seconds to wait between retries (it
            might be a floating point number).
        :type retry_delay: float
        """
        self.databricks_conn_id = databricks_conn_id
        self.databricks_conn = self.get_connection(databricks_conn_id)
        self.timeout_seconds = timeout_seconds
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    @staticmethod
    def _parse_host(host):
        """
        The purpose of this function is to be robust to improper connections
        settings provided by users, specifically in the host field.


        For example -- when users supply ``https://xx.cloud.databricks.com`` as the
        host, we must strip out the protocol to get the host.
        >>> h = ETLProjectsDatabricksHook()
        >>> assert h._parse_host('https://xx.cloud.databricks.com') == \
            'xx.cloud.databricks.com'

        In the case where users supply the correct ``xx.cloud.databricks.com`` as the
        host, this function is a no-op.
        >>> assert h._parse_host('xx.cloud.databricks.com') == 'xx.cloud.databricks.com'
        """
        urlparse_host = urlparse.urlparse(host).hostname
        if urlparse_host:
            # In this case, host = https://xx.cloud.databricks.com
            return urlparse_host
        else:
            # In this case, host = xx.cloud.databricks.com
            return host

    def _do_api_call(self, endpoint_info, json):
        """
        Utility function to perform an API call with retries
        :param endpoint_info: Tuple of method and endpoint
        :type endpoint_info: (string, string)
        :param json: Parameters for this API call.
        :type json: dict
        :return: If the api call returns a OK status code,
            this function returns the response in JSON. Otherwise,
            we throw an AirflowException.
        :rtype: dict
        """
        method, endpoint = endpoint_info
        url = "https://{host}/{endpoint}".format(
            host=self._parse_host(self.databricks_conn.host), endpoint=endpoint
        )
        if "token" in self.databricks_conn.extra_dejson:
            self.log.info("Using token auth.")
            auth = _TokenAuth(self.databricks_conn.extra_dejson["token"])
        else:
            self.log.info("Using basic auth.")
            auth = (self.databricks_conn.login, self.databricks_conn.password)
        if method == "GET":
            request_func = requests.get
        elif method == "POST":
            request_func = requests.post
        else:
            raise AirflowException("Unexpected HTTP Method: " + method)

        attempt_num = 1
        while True:
            try:
                response = request_func(
                    url,
                    json=json,
                    auth=auth,
                    headers=USER_AGENT_HEADER,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                return response.json()
            except requests_exceptions.RequestException as e:
                if not _retryable_error(e):
                    # In this case, the user probably made a mistake.
                    # Don't retry.
                    raise AirflowException(
                        "Response: {0}, Status Code: {1}".format(
                            e.response.content, e.response.status_code
                        )
                    )

                self._log_request_error(attempt_num, e)

            if attempt_num == self.retry_limit:
                raise AirflowException(
                    (
                        "API requests to Databricks failed {} times. " + "Giving up."
                    ).format(self.retry_limit)
                )

            attempt_num += 1
            sleep(self.retry_delay)

    def _log_request_error(self, attempt_num, error):
        self.log.error(
            "Attempt %s API Request to Databricks failed with reason: %s",
            attempt_num,
            error,
        )

    def run_now(self, json):
        """
        Utility function to call the ``api/2.0/jobs/run-now`` endpoint.

        :param json: The data used in the body of the request to the ``run-now``
            endpoint.
        :type json: dict
        :return: the run_id as a string
        :rtype: string
        """
        response = self._do_api_call(RUN_NOW_ENDPOINT, json)
        return response["run_id"]

    def submit_run(self, json):
        """
        Utility function to call the ``api/2.0/jobs/runs/submit`` endpoint.

        :param json: The data used in the body of the request to the ``submit``
            endpoint.
        :type json: dict
        :return: the run_id as a string
        :rtype: string
        """
        response = self._do_api_call(SUBMIT_RUN_ENDPOINT, json)
        return response["run_id"]

    def get_run_page_url(self, run_id):
        json = {"run_id": run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        return response["run_page_url"]

    def get_run_state(self, run_id):
        json = {"run_id": run_id}
        response = self._do_api_call(GET_RUN_ENDPOINT, json)
        state = response["state"]
        life_cycle_state = state["life_cycle_state"]
        # result_state may not be in the state if not terminal
        result_state = state.get("result_state", None)
        state_message = state["state_message"]
        return RunState(life_cycle_state, result_state, state_message)

    def get_cluster_state(self, cluster_id):
        json = {"cluster_id": cluster_id}
        response = self._do_api_call(GET_CLUSTER_ENDPOINT, json)
        state = response["state"]
        state_message = response["state_message"]
        return ClusterState(state, state_message)

    def get_libraries_cluster_status(self, cluster_id):
        json = {"cluster_id": cluster_id}
        response = self._do_api_call(LIBRARIES_CLUSTER_STATUS_ENDPOINT, json)
        library_statuses = response["library_statuses"]
        library_status_list = []
        for library_status in library_statuses:
            library_status_list.append(
                LibraryState(
                    library_status["library"],
                    library_status["status"],
                    library_status["is_library_for_all_clusters"],
                )
            )

        return library_status_list

    def cancel_run(self, run_id):
        json = {"run_id": run_id}
        self._do_api_call(CANCEL_RUN_ENDPOINT, json)

    def restart_cluster(self, json):
        self._do_api_call(RESTART_CLUSTER_ENDPOINT, json)

    def start_cluster(self, json):
        self._do_api_call(START_CLUSTER_ENDPOINT, json)

    def create_cluster(self, json):
        response = self._do_api_call(CREATE_CLUSTER_ENDPOINT, json)
        return response["cluster_id"]

    def install_libraries(self, cluster_id, libraries):
        json = {"cluster_id": cluster_id, "libraries": libraries}
        self._do_api_call(INSTALL_LIBRARIES_ENDPOINT, json)

    def terminate_cluster(self, cluster_id):
        json = {"cluster_id": cluster_id}
        self._do_api_call(TERMINATE_CLUSTER_ENDPOINT, json)


def _retryable_error(exception):
    return (
        isinstance(exception, requests_exceptions.ConnectionError)
        or isinstance(exception, requests_exceptions.Timeout)
        or exception.response is not None
        and exception.response.status_code >= 500
    )


RUN_LIFE_CYCLE_STATES = [
    "PENDING",
    "RUNNING",
    "TERMINATING",
    "TERMINATED",
    "SKIPPED",
    "INTERNAL_ERROR",
]


class RunState:
    """
    Utility class for the run state concept of Databricks runs.
    """

    def __init__(self, life_cycle_state, result_state, state_message):
        self.life_cycle_state = life_cycle_state
        self.result_state = result_state
        self.state_message = state_message

    @property
    def is_terminal(self):
        if self.life_cycle_state not in RUN_LIFE_CYCLE_STATES:
            raise AirflowException(
                (
                    "Unexpected life cycle state: {}: If the state has "
                    "been introduced recently, please check the Databricks user "
                    "guide for troubleshooting information"
                ).format(self.life_cycle_state)
            )
        return self.life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")

    @property
    def is_successful(self):
        return self.result_state == "SUCCESS"

    def __eq__(self, other):
        return (
            self.life_cycle_state == other.life_cycle_state
            and self.result_state == other.result_state
            and self.state_message == other.state_message
        )

    def __repr__(self):
        return str(self.__dict__)


class ClusterState:
    """
    Utility class for the cluster state concept of Databricks clusters.
    """

    STATES = [
        "PENDING",
        "RUNNING",
        "RESTARTING",
        "RESIZING",
        "TERMINATING",
        "TERMINATED",
        "ERROR",
        "UNKNOWN",
    ]

    def __init__(self, state, state_message):
        self.state = state
        self.state_message = state_message

    @property
    def is_terminal(self):
        if self.state not in ClusterState.STATES:
            raise AirflowException(
                (
                    "Unexpected cluster state: {}: If the state has "
                    "been introduced recently, please check the Databricks user "
                    "guide for troubleshooting information"
                ).format(self.state)
            )
        return self.state in ("RUNNING", "TERMINATED", "ERROR", "UNKNOWN")

    @property
    def is_successful(self):
        return self.state == "RUNNING"

    @property
    def is_terminated(self):
        return self.state == "TERMINATED"

    def __eq__(self, other):
        return self.state == other.state and self.state_message == other.state_message

    def __repr__(self):
        return str(self.__dict__)


class LibraryState:
    """
    Utility class for the library state concept of Databricks libraries.
    """

    STATUSES = [
        "PENDING",
        "RESOLVING",
        "INSTALLING",
        "INSTALLED",
        "FAILED",
        "UNINSTALL_ON_RESTART",
    ]

    def __init__(self, library, status, is_library_for_all_clusters):
        self.library = library
        self.status = status
        self.is_library_for_all_clusters = is_library_for_all_clusters

    @property
    def is_terminal(self):
        if self.status not in LibraryState.STATUSES:
            raise AirflowException(
                (
                    "Unexpected library state: {}: If the state has "
                    "been introduced recently, please check the Databricks user "
                    "guide for troubleshooting information"
                ).format(self.status)
            )
        return self.status in ("INSTALLED", "FAILED", "UNINSTALL_ON_RESTART")

    @property
    def is_successful(self):
        return self.status == "INSTALLED"

    def __eq__(self, other):
        return (
            self.library == other.library
            and self.status == other.status
            and self.is_library_for_all_clusters == other.is_library_for_all_clusters
        )

    def __repr__(self):
        return str(self.__dict__)


class _TokenAuth(AuthBase):
    """
    Helper class for requests Auth field. AuthBase requires you to implement the
    __call__ magic function.
    """

    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["Authorization"] = "Bearer " + self.token
        return r


# --------------------------------------------------------------------------------------
# |                                                                                    |
# |                                                                                    |
# |                                 OPERATORS                                          |
# |                                                                                    |
# |                                                                                    |
# --------------------------------------------------------------------------------------


XCOM_RUN_ID_KEY = "run_id"
XCOM_CLUSTER_ID_KEY = "cluster_id"
XCOM_RUN_PAGE_URL_KEY = "run_page_url"
AIRFLOW_TASK_DEFAULT_EXECUTION_TIMEOUT = timedelta(hours=2)


def _deep_string_coerce(content, json_path="json"):
    """
    Coerces content or all values of content if it is a dict to a string. The
    function will throw if content contains non-string or non-numeric types.

    The reason why we have this function is because the ``self.json`` field must be a
     dict with only string values. This is because ``render_template`` will fail
    for numerical values.
    """
    c = _deep_string_coerce
    if isinstance(content, six.string_types):
        return content
    elif isinstance(content, six.integer_types + (float,)):
        # Databricks can tolerate either numeric or string types in the API backend.
        return str(content)
    elif isinstance(content, (list, tuple)):
        return [c(e, "{0}[{1}]".format(json_path, i)) for i, e in enumerate(content)]
    elif isinstance(content, dict):
        return {
            k: c(v, "{0}[{1}]".format(json_path, k)) for k, v in list(content.items())
        }
    else:
        param_type = type(content)
        msg = "Type {0} used for parameter {1} is not a number or a string".format(
            param_type, json_path
        )
        raise AirflowException(msg)


def _handle_databricks_operator_execution(operator, hook, log, context):
    """
    Handles the Airflow + Databricks lifecycle logic for a Databricks operator
    :param operator: Databricks operator being handled
    :param context: Airflow context
    """
    if operator.do_xcom_push:
        context["ti"].xcom_push(key=XCOM_RUN_ID_KEY, value=operator.run_id)
    log.info("Run submitted with run_id: %s", operator.run_id)
    run_page_url = hook.get_run_page_url(operator.run_id)
    if operator.do_xcom_push:
        context["ti"].xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=run_page_url)

    log.info("View run status, Spark UI, and logs at %s", run_page_url)
    while True:
        run_state = hook.get_run_state(operator.run_id)
        if run_state.is_terminal:
            if run_state.is_successful:
                log.info("%s completed successfully.", operator.task_id)
                log.info("View run status, Spark UI, and logs at %s",
                         run_page_url)
                return
            else:
                error_message = "{t} failed with terminal state: {s}".format(
                    t=operator.task_id, s=run_state
                )
                raise AirflowException(error_message)
        else:
            log.info("%s in run state: %s", operator.task_id, run_state)
            log.info("View run status, Spark UI, and logs at %s", run_page_url)
            log.info("Sleeping for %s seconds.",
                     operator.polling_period_seconds)
            time.sleep(operator.polling_period_seconds)


def _handle_databricks_create_cluster_operator_execution(operator, hook, log, context):
    """
    Handles the Airflow + Databricks cluster for
    ETLProjectsCreateClusterDatabricksOperator
    :param operator: Databricks operator being handled
    :param context: Airflow context
    """
    if operator.do_xcom_push:
        context["ti"].xcom_push(key=XCOM_CLUSTER_ID_KEY,
                                value=operator.cluster_id)
    log.info("Cluster created with id: %s", operator.cluster_id)

    while True:
        cluster_state = hook.get_cluster_state(operator.cluster_id)
        if cluster_state.is_terminal:
            if cluster_state.is_successful:
                log.info("%s completed successfully.", operator.task_id)
                return
            else:
                error_message = "{t} failed with terminal state: {s}".format(
                    t=operator.task_id, s=cluster_state
                )
                raise AirflowException(error_message)
        else:
            log.info("%s in run state: %s", operator.task_id, cluster_state)
            log.info("Sleeping for %s seconds.",
                     operator.polling_period_seconds)
            time.sleep(operator.polling_period_seconds)


def _handle_databricks_terminate_cluster_operator_execution(operator, hook, log):
    """
    Handles the Airflow + Databricks cluster for
    ETLProjectsDatabricksTerminateClusterOperator
    :param operator: Databricks operator being handled
    :param context: Airflow context
    """
    while True:
        cluster_state = hook.get_cluster_state(operator.cluster_id)
        if cluster_state.is_terminal:
            if cluster_state.is_terminated:
                log.info("%s terminated successfully.", operator.task_id)
                return
            else:
                error_message = "{t} failed with terminal state: {s}".format(
                    t=operator.task_id, s=cluster_state
                )
                raise AirflowException(error_message)
        else:
            log.info("%s in run state: %s", operator.task_id, cluster_state)
            log.info("Sleeping for %s seconds.",
                     operator.polling_period_seconds)
            time.sleep(operator.polling_period_seconds)


def _handle_databricks_cluster_libraries_operator_execution(operator, hook, log):
    """
    Handles the Airflow + Databricks cluster libraries for a Databricks operator
    :param operator: Databricks operator being handled
    :param hook: Hook for connecting to Databricks
    :param log: log instance
    """
    while True:
        libraries_status = hook.get_libraries_cluster_status(
            operator.cluster_id)
        all_libraries_completed_successfully = True
        for library_status in libraries_status:
            if not library_status.is_terminal:
                all_libraries_completed_successfully = False
                continue

            if not library_status.is_successful:
                error_message = "{t} failed with terminal state: {s}".format(
                    t=library_status.library, s=library_status
                )
                raise AirflowException(error_message)

            log.info("Library %s installed successfully.",
                     library_status.library)
            # removing from list to avoid reprocessing it
            libraries_status.remove(library_status)

        if not all_libraries_completed_successfully:
            log.info(
                "Libraries [%s] have not been installed yet", libraries_status)
            log.info("Sleeping for %s seconds.",
                     operator.polling_period_seconds)
            time.sleep(operator.polling_period_seconds)
            continue

        log.info("All libraries have been successfully installed!")
        return


class ETLProjectsDatabricksSubmitRunOperator(BaseOperator):
    """
    Submits a Spark job run to Databricks using the
    `api/2.0/jobs/runs/submit
    <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_
    API endpoint.

    There are two ways to instantiate this operator.

    In the first way, you can take the JSON payload that you typically use
    to call the ``api/2.0/jobs/runs/submit`` endpoint and pass it directly
    to our ``DatabricksSubmitRunOperator`` through the ``json`` parameter.
    For example ::
        json = {
          'new_cluster': {
            'spark_version': '2.1.0-db3-scala2.11',
            'num_workers': 2
          },
          'notebook_task': {
            'notebook_path': '/Users/airflow@example.com/PrepareData',
          },
        }
        notebook_run = DatabricksSubmitRunOperator(task_id='notebook_run', json=json)

    Another way to accomplish the same thing is to use the named parameters
    of the ``DatabricksSubmitRunOperator`` directly. Note that there is exactly
    one named parameter for each top level parameter in the ``runs/submit``
    endpoint. In this method, your code would look like this: ::
        new_cluster = {
          'spark_version': '2.1.0-db3-scala2.11',
          'num_workers': 2
        }
        notebook_task = {
          'notebook_path': '/Users/airflow@example.com/PrepareData',
        }
        notebook_run = DatabricksSubmitRunOperator(
            task_id='notebook_run',
            new_cluster=new_cluster,
            notebook_task=notebook_task)

    In the case where both the json parameter **AND** the named parameters
    are provided, they will be merged together. If there are conflicts during the merge,
    the named parameters will take precedence and override the top level ``json`` keys.

    Currently the named parameters that ``DatabricksSubmitRunOperator`` supports are
        - ``spark_jar_task``
        - ``notebook_task``
        - ``new_cluster``
        - ``existing_cluster_id``
        - ``libraries``
        - ``run_name``
        - ``timeout_seconds``

    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.0/jobs/runs/submit`` endpoint. The other named
        parameters (i.e. ``spark_jar_task``, ``notebook_task``..) to this operator will
        be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will
        take precedence and override the top level json keys. (templated)

        .. seealso::
            For more information about templating see :ref:`jinja-templating`.
            https://docs.databricks.com/api/latest/jobs.html#runs-submit
    :type json: dict
    :param spark_jar_task: The main class and parameters for the JAR task. Note that
        the actual JAR is specified in the ``libraries``.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#jobssparkjartask
    :type spark_jar_task: dict
    :param notebook_task: The notebook path and parameters for the notebook task.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#jobsnotebooktask
    :type notebook_task: dict
    :param new_cluster: Specs for a new cluster on which this task will be run.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#jobsclusterspecnewcluster
    :type new_cluster: dict
    :param existing_cluster_id: ID for existing cluster on which to run this task.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified.
        This field will be templated.
    :type existing_cluster_id: string
    :param libraries: Libraries which this run will use.
        This field will be templated.

        .. seealso::
        https://docs.databricks.com/api/latest/libraries.html#managedlibrarieslibrary
    :type libraries: list of dicts
    :param run_name: The run name used for this task.
        By default this will be set to the Airflow ``task_id``. This ``task_id`` is a
        required parameter of the superclass ``BaseOperator``.
        This field will be templated.
    :type run_name: string
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
        This field will be templated.
    :type timeout_seconds: int32
    :param databricks_conn_id: The name of the Airflow connection to use.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection.
    :type databricks_conn_id: string
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :type polling_period_seconds: int
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :type databricks_retry_limit: int
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :type databricks_retry_delay: float
    :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
    :type do_xcom_push: boolean
    :param retries: the number of retries that should be performed before
        failing the task
    :type retries: int
    :param retry_delay: delay between retries
    :type retry_delay: datetime.timedelta
    :param max_retry_delay: maximum delay interval between retries
    :type max_retry_delay: datetime.timedelta
    :param execution_timeout: the airflow BaseOperator execution_timeout. When not set
        the default AIRFLOW_TASK_DEFAULT_EXECUTION_TIMEOUT will be set. When set as None
        then no timeout will be applied
    :type execution_timeout: datetime.timedelta
    """

    # Used in airflow.models.BaseOperator
    template_fields = ("json",)
    # Databricks brand color (blue) under white text
    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"

    @apply_defaults
    def __init__(
        self,
        json=None,
        spark_jar_task=None,
        notebook_task=None,
        new_cluster=None,
        existing_cluster_id=None,
        libraries=None,
        run_name=None,
        timeout_seconds=None,
        databricks_conn_id="databricks_default",
        polling_period_seconds=30,
        databricks_retry_limit=3,
        databricks_retry_delay=1,
        do_xcom_push=False,
        retries=3,
        retries_delay=timedelta(minutes=3),
        max_retry_delay=timedelta(minutes=3),
        execution_timeout=AIRFLOW_TASK_DEFAULT_EXECUTION_TIMEOUT,
        **kwargs,
    ):
        """
        Creates a new ``DatabricksSubmitRunOperator``.
        """
        kwargs = {
            **kwargs,
            "retries": retries,
            "retries_delay": retries_delay,
            "max_retry_delay": max_retry_delay,
            "execution_timeout": execution_timeout,
        }
        super(ETLProjectsDatabricksSubmitRunOperator, self).__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        if spark_jar_task is not None:
            self.json["spark_jar_task"] = spark_jar_task
        if notebook_task is not None:
            self.json["notebook_task"] = notebook_task
        if new_cluster is not None:
            self.json["new_cluster"] = new_cluster
        if existing_cluster_id is not None:
            self.json["existing_cluster_id"] = existing_cluster_id
        if libraries is not None:
            self.json["libraries"] = libraries
        if run_name is not None:
            self.json["run_name"] = run_name
        if timeout_seconds is not None:
            self.json["timeout_seconds"] = timeout_seconds
        if "run_name" not in self.json:
            self.json["run_name"] = run_name or kwargs["task_id"]

        self.json = _deep_string_coerce(self.json)
        # This variable will be used in case our task gets killed.
        self.run_id = None
        self.do_xcom_push = do_xcom_push

    def get_hook(self):
        return ETLProjectsDatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
        )

    def execute(self, context):
        if not self.json.get("existing_cluster_id") and not self.json.get(
            "new_cluster"
        ):
            existing_cluster_id = get_xcom_value(context, XCOM_CLUSTER_ID_KEY)
            self.json["existing_cluster_id"] = existing_cluster_id
            self.log.info(
                "Assigned existing_cluster_id json field with xcom value "
                "`{}`".format(existing_cluster_id)
            )

        hook = self.get_hook()
        self.run_id = hook.submit_run(self.json)

        _handle_databricks_operator_execution(self, hook, self.log, context)

    def on_kill(self):
        hook = self.get_hook()
        hook.cancel_run(self.run_id)
        self.log.info(
            "Task: %s with run_id: %s was requested to be cancelled.",
            self.task_id,
            self.run_id,
        )


class ETLProjectsDatabricksRunNowOperator(BaseOperator):
    """
    Runs an existing Spark job run to Databricks using the
    `api/2.0/jobs/run-now
    <https://docs.databricks.com/api/latest/jobs.html#run-now>`_
    API endpoint.

    There are two ways to instantiate this operator.

    In the first way, you can take the JSON payload that you typically use
    to call the ``api/2.0/jobs/run-now`` endpoint and pass it directly
    to our ``DatabricksRunNowOperator`` through the ``json`` parameter.
    For example ::
        json = {
          "job_id": 42,
          "notebook_params": {
            "dry-run": "true",
            "oldest-time-to-consider": "1457570074236"
          }
        }

        notebook_run = DatabricksRunNowOperator(task_id='notebook_run', json=json)

    Another way to accomplish the same thing is to use the named parameters
    of the ``DatabricksRunNowOperator`` directly. Note that there is exactly
    one named parameter for each top level parameter in the ``run-now``
    endpoint. In this method, your code would look like this: ::

        job_id=42

        notebook_params = {
            "dry-run": "true",
            "oldest-time-to-consider": "1457570074236"
        }

        python_params = ["douglas adams", "42"]

        spark_submit_params = ["--class", "org.apache.spark.examples.SparkPi"]

        notebook_run = DatabricksRunNowOperator(
            job_id=job_id,
            notebook_params=notebook_params,
            python_params=python_params,
            spark_submit_params=spark_submit_params
        )

    In the case where both the json parameter **AND** the named parameters
    are provided, they will be merged together. If there are conflicts during the merge,
    the named parameters will take precedence and override the top level ``json`` keys.

    Currently the named parameters that ``DatabricksRunNowOperator`` supports are
        - ``job_id``
        - ``json``
        - ``notebook_params``
        - ``python_params``
        - ``spark_submit_params``


    :param job_id: the job_id of the existing Databricks job.
        This field will be templated.
        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#run-now
    :type job_id: string
    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.0/jobs/run-now`` endpoint. The other named parameters
        (i.e. ``notebook_params``, ``spark_submit_params``..) to this operator will
        be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will
        take precedence and override the top level json keys. (templated)

        .. seealso::
            For more information about templating see :ref:`jinja-templating`.
            https://docs.databricks.com/api/latest/jobs.html#run-now
    :type json: dict
    :param notebook_params: A dict from keys to values for jobs with notebook task,
        e.g. "notebook_params": {"name": "john doe", "age":  "35"}.
        The map is passed to the notebook and will be accessible through the
        dbutils.widgets.get function. See Widgets for more information.
        If not specified upon run-now, the triggered run will use the
        jobâ€™s base parameters. notebook_params cannot be
        specified in conjunction with jar_params. The json representation
        of this field (i.e. {"notebook_params":{"name":"john doe","age":"35"}})
        cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/user-guide/notebooks/widgets.html
    :type notebook_params: dict
    :param python_params: A list of parameters for jobs with python tasks,
        e.g. "python_params": ["john doe", "35"].
        The parameters will be passed to python file as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified in
        job setting.
        The json representation of this field (i.e. {"python_params":["john doe","35"]})
        cannot exceed 10,000 bytes.
        This field will be templated.

        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#run-now
    :type python_params: array of strings
    :param spark_submit_params: A list of parameters for jobs with spark submit task,
        e.g. "spark_submit_params": ["--class", "org.apache.spark.examples.SparkPi"].
        The parameters will be passed to spark-submit script as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified
        in job setting.
        The json representation of this field cannot exceed 10,000 bytes.
        This field will be templated.
        .. seealso::
            https://docs.databricks.com/api/latest/jobs.html#run-now
    :type spark_submit_params: array of strings
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
        This field will be templated.
    :type timeout_seconds: int32
    :param databricks_conn_id: The name of the Airflow connection to use.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection.
    :type databricks_conn_id: string
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :type polling_period_seconds: int
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :type databricks_retry_limit: int
    :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
    :type do_xcom_push: boolean
    :param retries: the number of retries that should be performed before
        failing the task
    :type retries: int
    :param retry_delay: delay between retries
    :type retry_delay: datetime.timedelta
    :param max_retry_delay: maximum delay interval between retries
    :type max_retry_delay: datetime.timedelta
    :param execution_timeout: the airflow BaseOperator execution_timeout. When not set
        the default AIRFLOW_TASK_DEFAULT_EXECUTION_TIMEOUT will be set. When set as None
        then no timeout will be applied
    :type execution_timeout: datetime.timedelta
    """

    # Used in airflow.models.BaseOperator
    template_fields = ("json",)
    # Databricks brand color (blue) under white text
    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"

    @apply_defaults
    def __init__(
        self,
        job_id,
        json=None,
        notebook_params=None,
        python_params=None,
        spark_submit_params=None,
        databricks_conn_id="databricks_default",
        polling_period_seconds=30,
        databricks_retry_limit=3,
        databricks_retry_delay=1,
        do_xcom_push=False,
        retries=3,
        retries_delay=timedelta(minutes=3),
        max_retry_delay=timedelta(minutes=3),
        execution_timeout=AIRFLOW_TASK_DEFAULT_EXECUTION_TIMEOUT,
        **kwargs,
    ):
        """
        Creates a new ``DatabricksRunNowOperator``.
        """
        kwargs = {
            **kwargs,
            "retries": retries,
            "retries_delay": retries_delay,
            "max_retry_delay": max_retry_delay,
            "execution_timeout": execution_timeout,
        }
        super(ETLProjectsDatabricksRunNowOperator, self).__init__(**kwargs)
        self.json = json or {}
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay

        if job_id is not None:
            self.json["job_id"] = job_id
        if notebook_params is not None:
            self.json["notebook_params"] = notebook_params
        if python_params is not None:
            self.json["python_params"] = python_params
        if spark_submit_params is not None:
            self.json["spark_submit_params"] = spark_submit_params

        self.json = _deep_string_coerce(self.json)
        # This variable will be used in case our task gets killed.
        self.run_id = None
        self.do_xcom_push = do_xcom_push

    def get_hook(self):
        return ETLProjectsDatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
        )

    def execute(self, context):
        hook = self.get_hook()
        self.run_id = hook.run_now(self.json)
        _handle_databricks_operator_execution(self, hook, self.log, context)

    def on_kill(self):
        hook = self.get_hook()
        hook.cancel_run(self.run_id)
        self.log.info(
            "Task: %s with run_id: %s was requested to be cancelled.",
            self.task_id,
            self.run_id,
        )


class ETLProjectsDatabricksCreateClusterOperator(BaseOperator):
    """
    Create a Spark cluster and install specific libraries using the
    `api/2.0/clusters/create
    <https://docs.databricks.com/api/latest/clusters.html#create>`_
    and `api/2.0/libraries/install
    <https://docs.databricks.com/api/latest/libraries.html#install>`_
    API endpoints.

    This operator can be instantiated in the following way:
    - Passing a cluster configuration through the ``cluster_configuration`` parameter.
        cluster_configuration={
            "autoscale": {"min_workers": 3, "max_workers": 4},
            "cluster_name": "CLUSTER_NAME",
            "spark_version": "5.4.x-scala2.11",
            "spark_conf": {"spark.speculation": "true"},
            "aws_attributes": {
                "first_on_demand": 1,
                "availability": "SPOT_WITH_FALLBACK",
                "zone_id": "ZONE_ID",
                "instance_profile_arn": "INSTANCE_PROFILE_ARN",
                "spot_bid_price_percent": 100,
                "ebs_volume_count": 0,
            },
            "node_type_id": "i3.xlarge",
            "driver_node_type_id": "i3.xlarge",
            "ssh_public_keys": [],
            "custom_tags": {},
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "autotermination_minutes": 60,
            "enable_elastic_disk": True,
            "cluster_source": "API",
            "init_scripts": [],
        }
    - Passing also a libraries json list through the ``libraries`` libraries parameter.
        libraries=[
        {
            "whl": "s3://BUCKET/CUSTOM.WHL"
        },
        {
            "jar": "s3://BUCKET/CUSTOM.JAR"
        }
    ]

    :param cluster_configuration: A JSON object containing API parameters which will be
        passed directly to the ``api/2.0/clusters/create`` endpoint. (templated)
    :type cluster_configuration: dict
    :param libraries: A JSON object list containing API parameters which will be passed
        directly to the ``api/2.0/libraries/install`` endpoint.
    :type libraries: list
    :param databricks_conn_id: The name of the Airflow connection to use.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection.
    :type databricks_conn_id: string
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :type polling_period_seconds: int
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :type databricks_retry_limit: int
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :type databricks_retry_delay: float
    :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
    :type do_xcom_push: boolean
    :param retries: the number of retries that should be performed before
        failing the task
    :type retries: int
    :param retry_delay: delay between retries
    :type retry_delay: datetime.timedelta
    :param max_retry_delay: maximum delay interval between retries
    :type max_retry_delay: datetime.timedelta
    :param execution_timeout: the airflow BaseOperator execution_timeout. When not set
        the default AIRFLOW_TASK_DEFAULT_EXECUTION_TIMEOUT will be set. When set as None
        then no timeout will be applied
    :type execution_timeout: datetime.timedelta
    """

    # Used in airflow.models.BaseOperator
    template_fields = ("cluster_configuration",)
    # Databricks brand color (blue) under white text
    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"

    @apply_defaults
    def __init__(
        self,
        cluster_configuration,
        libraries=None,
        databricks_conn_id="databricks_default",
        polling_period_seconds=30,
        databricks_retry_limit=3,
        databricks_retry_delay=1,
        do_xcom_push=True,
        retries=3,
        retries_delay=timedelta(minutes=3),
        max_retry_delay=timedelta(minutes=3),
        execution_timeout=AIRFLOW_TASK_DEFAULT_EXECUTION_TIMEOUT,
        **kwargs,
    ):
        """
        Creates a new ``ETLProjectsDatabricksCreateClusterOperator``.
        """
        kwargs = {
            **kwargs,
            "retries": retries,
            "retries_delay": retries_delay,
            "max_retry_delay": max_retry_delay,
            "execution_timeout": execution_timeout,
        }
        super(ETLProjectsDatabricksCreateClusterOperator, self).__init__(**kwargs)
        self.libraries = libraries or []
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay

        self.cluster_configuration = _deep_string_coerce(cluster_configuration)
        # This variable will be used in case our task gets killed.
        self.cluster_id = None
        self.do_xcom_push = do_xcom_push

    def get_hook(self):
        return ETLProjectsDatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
        )

    def execute(self, context):
        hook = self.get_hook()
        self.cluster_id = hook.create_cluster(self.cluster_configuration)
        _handle_databricks_create_cluster_operator_execution(
            self, hook, self.log, context
        )

        # install libraries in the newly created cluster
        try:
            self.log.info("Installing libraries in cluster...")
            hook.install_libraries(self.cluster_id, self.libraries)
            _handle_databricks_cluster_libraries_operator_execution(
                self, hook, self.log
            )
        except Exception as e:
            self.log.info(
                "Exception raised while installing libraries. Terminating cluster %s",
                self.cluster_id,
            )
            hook.terminate_cluster(self.cluster_id)
            raise e

    def on_kill(self):
        hook = self.get_hook()
        hook.terminate_cluster(self.cluster_id)
        self.log.info(
            "Task: %s with cluster_id: %s was requested to be cancelled.",
            self.task_id,
            self.cluster_id,
        )


class ETLProjectsDatabricksTerminateClusterOperator(BaseOperator):
    """
    Terminate a Spark cluster using the
    `api/2.0/clusters/delete
    <https://docs.databricks.com/api/latest/clusters.html#delete>`_
    API endpoint.

    This operator can be instantiated by passing a ``cluster_id`` parameter.

    :param cluster_id: The ID of the cluster which will be terminated. (templated)
    :type cluster_id: str
    :param databricks_conn_id: The name of the Airflow connection to use.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection.
    :type databricks_conn_id: string
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :type polling_period_seconds: int
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :type databricks_retry_limit: int
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :type databricks_retry_delay: float
    :param retries: the number of retries that should be performed before
        failing the task
    :type retries: int
    :param retry_delay: delay between retries
    :type retry_delay: datetime.timedelta
    :param max_retry_delay: maximum delay interval between retries
    :type max_retry_delay: datetime.timedelta
    :param execution_timeout: the airflow BaseOperator execution_timeout. When not set
        the default AIRFLOW_TASK_DEFAULT_EXECUTION_TIMEOUT will be set. When set as None
        then no timeout will be applied
    :type execution_timeout: datetime.timedelta
    """

    # Used in airflow.models.BaseOperator
    template_fields = ("cluster_id",)
    # Databricks brand color (blue) under white text
    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"

    @apply_defaults
    def __init__(
        self,
        cluster_id=None,
        databricks_conn_id="databricks_default",
        polling_period_seconds=30,
        databricks_retry_limit=3,
        databricks_retry_delay=1,
        retries=3,
        retries_delay=timedelta(minutes=3),
        max_retry_delay=timedelta(minutes=3),
        execution_timeout=AIRFLOW_TASK_DEFAULT_EXECUTION_TIMEOUT,
        **kwargs,
    ):
        """
        Creates a new ``ETLProjectsDatabricksTerminateClusterOperator``.
        """
        kwargs = {
            **kwargs,
            "retries": retries,
            "retries_delay": retries_delay,
            "max_retry_delay": max_retry_delay,
            "execution_timeout": execution_timeout,
        }
        super(ETLProjectsDatabricksTerminateClusterOperator,
              self).__init__(**kwargs)
        self.cluster_id = cluster_id
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay

    def get_hook(self):
        return ETLProjectsDatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
        )

    def execute(self, context):
        if not self.cluster_id:
            self.cluster_id = get_xcom_value(context, XCOM_CLUSTER_ID_KEY)
            self.log.info(
                "Assigned cluster_id field with xcom value "
                "`{}`".format(self.cluster_id)
            )

        hook = self.get_hook()
        hook.terminate_cluster(self.cluster_id)
        _handle_databricks_terminate_cluster_operator_execution(
            self, hook, self.log)

    def on_kill(self):
        hook = self.get_hook()
        hook.terminate_cluster(self.cluster_id)
        self.log.info(
            "Task: %s with cluster_id: %s was requested to be cancelled.",
            self.task_id,
            self.cluster_id,
        )


class ETLProjectsDatabricksPlugin(AirflowPlugin):
    name = "etlprojects_databricks"
    operators = [
        ETLProjectsDatabricksCreateClusterOperator,
        ETLProjectsDatabricksRunNowOperator,
        ETLProjectsDatabricksSubmitRunOperator,
        ETLProjectsDatabricksTerminateClusterOperator,
    ]
    hooks = [ETLProjectsDatabricksHook]
