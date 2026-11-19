import os
from typing import TYPE_CHECKING, Dict, Iterator, Optional, Sequence, List, Any, Union
from urllib.parse import urlparse, urlunparse

from dlt.common import logger
from dlt.common.configuration.specs import (
    AzureCredentialsWithoutDefaults,
    AzureServicePrincipalCredentialsWithoutDefaults,
)
from dlt.common.destination.client import (
    FollowupJobRequest,
    HasFollowupJobs,
    LoadJob,
    PreparedTableSchema,
    RunnableLoadJob,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema import TColumnSchema, TColumnHint, Schema
from dlt.common.schema.typing import TColumnType

from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.destinations._adbc_jobs import AdbcParquetCopyJob
from dlt.destinations.sql_jobs import SqlStagingReplaceFollowupJob, SqlMergeFollowupJob

from dlt.destinations.insert_job_client import InsertValuesJobClient
from dlt.destinations.job_client_impl import CopyRemoteFileLoadJob

from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration, build_odbc_dsn
from dlt.destinations.sql_client import SqlClientBase


HINT_TO_MSSQL_ATTR: Dict[TColumnHint, str] = {"unique": "UNIQUE"}
VARCHAR_MAX_N: int = 4000
VARBINARY_MAX_N: int = 8000


class MsSqlStagingReplaceJob(SqlStagingReplaceFollowupJob):
    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlClientBase[Any],
    ) -> List[str]:
        sql: List[str] = []
        for table in table_chain:
            with sql_client.with_staging_dataset():
                staging_table_name = sql_client.make_qualified_table_name(table["name"])
            table_name = sql_client.make_qualified_table_name(table["name"])
            # drop destination table
            sql.append(f"DROP TABLE IF EXISTS {table_name}")
            # moving staging table to destination schema
            sql.append(
                f"ALTER SCHEMA {sql_client.fully_qualified_dataset_name()} TRANSFER"
                f" {staging_table_name}"
            )
            # recreate staging table
            sql.append(f"SELECT * INTO {staging_table_name} FROM {table_name} WHERE 1 = 0")
        return sql


class MsSqlMergeJob(SqlMergeFollowupJob):
    @classmethod
    def supports_alias_in_delete(cls) -> bool:
        return False

    @classmethod
    def _to_temp_table(
        cls,
        select_sql: str,
        temp_table_name: str,
        unique_column: str,
        sql_client: SqlClientBase[Any],
    ) -> str:
        return f"SELECT * INTO {temp_table_name} FROM ({select_sql}) as t"

    @classmethod
    def _new_temp_table_name(cls, table_name: str, op: str, sql_client: SqlClientBase[Any]) -> str:
        return SqlMergeFollowupJob._new_temp_table_name("#" + table_name, op, sql_client)


class MssqlParquetCopyJob(AdbcParquetCopyJob):
    _config: MsSqlClientConfiguration
    # mssql ADBC driver buffers the full input stream in memory; flush per row-group
    # to bound peak memory and avoid OOM on large parquet files
    _ingest_per_rowgroup: bool = True

    if TYPE_CHECKING:
        from adbc_driver_manager.dbapi import Connection

    def _connect(self) -> "Connection":
        from adbc_driver_manager import dbapi

        self._config = self._job_client.config  # type: ignore[assignment]
        conn_dsn = self.odbc_to_go_mssql_dsn(self._config.credentials.get_odbc_dsn_dict())
        conn_str = build_odbc_dsn(conn_dsn)
        return dbapi.connect(driver="mssql", db_kwargs={"uri": conn_str})

    @staticmethod
    def odbc_to_go_mssql_dsn(dsn: Dict[str, Any]) -> Dict[str, Any]:
        """Converts odbc connection string to go connection string used by ADBC"""
        # DSN keys are already normalized to upper case
        result: Dict[str, Any] = {}

        for upper, value in dsn.items():
            if value is None:
                continue

            v = str(value)

            if upper == "ENCRYPT":
                v = v.strip().lower()

                # ODBC: yes/mandatory/true/1 → go-mssqldb: true (TLS on)
                if v in {"yes", "true", "1", "mandatory"}:
                    v = "true"

                # ODBC: strict → go-mssqldb strict (if supported by the driver)
                elif v in {"strict"}:
                    v = "strict"

                # ODBC: optional → go-mssqldb optional (login only)
                elif v in {"optional"}:
                    v = "optional"

                # ODBC: no/false/0/disabled → go-mssqldb disable (no TLS at all)
                # This mirrors your previous string hack:
                #   .replace("=yes", "=1").replace("=no", "=disable")
                elif v in {"no", "false", "0", "disabled", "disable"}:
                    v = "disable"

            elif upper == "TRUSTSERVERCERTIFICATE":
                v = v.strip().lower()

                # ODBC uses yes/no; go-mssqldb expects true/false (but is lenient);
                # we normalize explicitly.
                if v in {"yes", "true", "1"}:
                    v = "true"
                elif v in {"no", "false", "0"}:
                    v = "false"

            result[upper] = v

        return result


class CopyIntoFileLoadJob(CopyRemoteFileLoadJob):
    """Loads a staged parquet file with T-SQL `COPY INTO`, as Synapse and Fabric support it.

    Subclasses add the storage-specific `WITH` options in `_get_copy_options`.
    """

    def run(self) -> None:
        self._sql_client = self._job_client.sql_client
        ext = os.path.splitext(self._bucket_path)[1][1:]
        if ext != "parquet":
            raise ValueError(
                f"Unsupported file type `{ext}` for `{self._job_client.config.destination_type}`."
            )

        staging_credentials = self._staging_credentials
        assert isinstance(
            staging_credentials,
            (AzureCredentialsWithoutDefaults, AzureServicePrincipalCredentialsWithoutDefaults),
        )
        https_path = self._get_https_path(
            self._bucket_path, staging_credentials.azure_storage_account_name
        )
        with_options = ["FILE_TYPE = 'PARQUET'", *self._get_copy_options(staging_credentials)]
        table_name = self._load_table["name"]

        with self._sql_client.begin_transaction():
            dataset_name = self._sql_client.dataset_name
            with_clause = ",\n    ".join(with_options)
            self._sql_client.execute_sql(
                f"COPY INTO [{dataset_name}].[{table_name}]\n"
                f"FROM '{https_path}'\n"
                f"WITH (\n    {with_clause}\n)"
            )

    def _get_copy_options(
        self,
        staging_credentials: Union[
            AzureCredentialsWithoutDefaults, AzureServicePrincipalCredentialsWithoutDefaults
        ],
    ) -> List[str]:
        """Returns the `WITH` options that follow `FILE_TYPE`, such as the `CREDENTIAL`."""
        return []

    def _get_https_path(self, bucket_path: str, storage_account_name: str) -> str:
        """Converts `az://<container_name>/<path>` to
        `https://<storage_account_name>.blob.core.windows.net/<container_name>/<path>`.
        """
        bucket_url = urlparse(bucket_path)
        # "blob" endpoint has better performance than "dfs" endpoint
        # https://learn.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=azure-sqldw-latest#external-locations
        endpoint = "blob"
        _path = "/" + bucket_url.netloc + bucket_url.path
        https_url = bucket_url._replace(
            scheme="https",
            netloc=f"{storage_account_name}.{endpoint}.core.windows.net",
            path=_path,
        )
        return urlunparse(https_url)


class MsSqlJobClient(InsertValuesJobClient):
    def __init__(
        self,
        schema: Schema,
        config: MsSqlClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        dataset_name, staging_dataset_name = InsertValuesJobClient.create_dataset_names(
            schema, config
        )
        sql_client = PyOdbcMsSqlClient(
            dataset_name,
            staging_dataset_name,
            config.credentials,
            capabilities,
        )
        super().__init__(schema, config, sql_client)
        self.config: MsSqlClientConfiguration = config
        self.sql_client = sql_client
        self.active_hints = HINT_TO_MSSQL_ATTR if self.config.create_indexes else {}
        self.type_mapper = capabilities.get_type_mapper()

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)
        if not job:
            parsed_file = ParsedLoadJobFileName.parse(file_path)
            if parsed_file.file_format == "parquet":
                job = MssqlParquetCopyJob(file_path)
        return job

    def _create_merge_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return [MsSqlMergeJob.from_table_chain(table_chain, self.sql_client)]

    def _make_add_column_sql(
        self, new_columns: Sequence[TColumnSchema], table: PreparedTableSchema = None
    ) -> List[str]:
        # Override because mssql requires multiple columns in a single ADD COLUMN clause
        return ["ADD \n" + ",\n".join(self._get_column_def_sql(c, table) for c in new_columns)]

    def _get_column_def_sql(self, c: TColumnSchema, table: PreparedTableSchema = None) -> str:
        sc_type = c["data_type"]
        if sc_type == "text" and c.get("unique"):
            # MSSQL does not allow index on large TEXT columns
            db_type = "nvarchar(%i)" % (c.get("precision") or 900)
        else:
            db_type = self.type_mapper.to_destination_type(c, table)

        hints_str = self._get_column_hints_sql(c)
        column_name = self.sql_client.escape_column_name(c["name"])
        return f"{column_name} {db_type} {hints_str} {self._gen_not_null(c.get('nullable', True))}"

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        root_table = table_chain[0]
        if root_table["x-replace-strategy"] == "staging-optimized":  # type: ignore[typeddict-item]
            return [MsSqlStagingReplaceJob.from_table_chain(table_chain, self.sql_client)]
        return super()._create_replace_followup_jobs(table_chain)

    def _from_db_type(
        self, pq_t: str, precision: Optional[int], scale: Optional[int]
    ) -> TColumnType:
        return self.type_mapper.from_destination_type(pq_t, precision, scale)
