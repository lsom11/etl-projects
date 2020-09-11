import os

from etl_projects.platform.db.database_enum import (
    DatabaseEnum,
    DatabaseTypeEnum,
)
from etl_projects.platform.db.datalake_metastore_service import (
    DatalakeMetastoreService,
)
from etl_projects.platform.db.dw_metastore_service import DWMetastoreService

DB_SQL_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../db")

DATALAKE_SQL_DIR = f"{DB_SQL_PATH}/datalake"  # TODO: refactor to DATALAKE_SQL_PATH
QUERIES_DATALAKE_PATH = (
    DATALAKE_SQL_DIR + "/queries/"
)  # TODO: refactor to DATALAKE_QUERY_PATH
DDL_DATALAKE_PATH = DATALAKE_SQL_DIR + "/ddl/"  # TODO: refactor to DATALAKE_DDL_PATH

DW_SQL_PATH = f"{DB_SQL_PATH}/dw"
DW_QUERY_PATH = DW_SQL_PATH + "/queries/"
