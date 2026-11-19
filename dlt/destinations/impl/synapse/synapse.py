from typing import Sequence, List, Dict, Any, Optional, cast, Union
from copy import deepcopy

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.client import (
    PreparedTableSchema,
    SupportsStagingDestination,
    FollowupJobRequest,
    LoadJob,
)

from dlt.common.schema import TColumnSchema, Schema, TColumnHint
from dlt.common.schema.utils import (
    get_inherited_table_hint,
)

from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.configuration.specs import (
    AzureCredentialsWithoutDefaults,
    AzureServicePrincipalCredentialsWithoutDefaults,
)

from dlt.destinations.job_impl import ReferenceFollowupJobRequest
from dlt.destinations.job_client_impl import SqlJobClientBase

from dlt.destinations.impl.mssql.mssql import (
    CopyIntoFileLoadJob,
    MsSqlJobClient,
    VARCHAR_MAX_N,
    VARBINARY_MAX_N,
)

from dlt.destinations.impl.synapse.sql_client import SynapseSqlClient
from dlt.destinations.impl.synapse.configuration import SynapseClientConfiguration
from dlt.destinations.impl.synapse.synapse_adapter import (
    TABLE_INDEX_TYPE_HINT,
    TTableIndexType,
)


HINT_TO_SYNAPSE_ATTR: Dict[TColumnHint, str] = {
    "primary_key": "PRIMARY KEY NONCLUSTERED NOT ENFORCED",
    "unique": "UNIQUE NOT ENFORCED",
}
TABLE_INDEX_TYPE_TO_SYNAPSE_ATTR: Dict[TTableIndexType, str] = {
    "heap": "HEAP",
    "clustered_columnstore_index": "CLUSTERED COLUMNSTORE INDEX",
}


class SynapseClient(MsSqlJobClient, SupportsStagingDestination):
    def __init__(
        self,
        schema: Schema,
        config: SynapseClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: SynapseClientConfiguration = config
        dataset_name, staging_dataset_name = MsSqlJobClient.create_dataset_names(schema, config)
        self.sql_client = SynapseSqlClient(
            dataset_name,
            staging_dataset_name,
            config.credentials,
            capabilities,
        )

        self.active_hints = deepcopy(HINT_TO_SYNAPSE_ATTR)
        if not self.config.create_indexes:
            self.active_hints.pop("primary_key", None)
            self.active_hints.pop("unique", None)

    def _get_table_update_sql(
        self, table_name: str, new_columns: Sequence[TColumnSchema], generate_alter: bool
    ) -> List[str]:
        table = self.prepare_load_table(table_name)
        if self.in_staging_dataset_mode:
            # Staging tables should always be heap tables, because "when you are
            # temporarily landing data in dedicated SQL pool, you may find that
            # using a heap table makes the overall process faster."
            table[TABLE_INDEX_TYPE_HINT] = "heap"  # type: ignore[typeddict-unknown-key]

        table_index_type = cast(TTableIndexType, table.get(TABLE_INDEX_TYPE_HINT))
        if self.in_staging_dataset_mode:
            final_table = self.prepare_load_table(table_name)
            final_table_index_type = cast(TTableIndexType, final_table.get(TABLE_INDEX_TYPE_HINT))
        else:
            final_table_index_type = table_index_type
        if final_table_index_type == "clustered_columnstore_index":
            # Even if the staging table has index type "heap", we still adjust
            # the column data types to prevent errors when writing into the
            # final table that has index type "clustered_columnstore_index".
            new_columns = self._get_columstore_valid_columns(table, new_columns)

        _sql_result = SqlJobClientBase._get_table_update_sql(
            self, table_name, new_columns, generate_alter
        )
        if not generate_alter:
            table_index_type_attr = TABLE_INDEX_TYPE_TO_SYNAPSE_ATTR[table_index_type]
            sql_result = [_sql_result[0] + f"\n WITH ( {table_index_type_attr} );"]
        else:
            sql_result = _sql_result
        return sql_result

    def _get_columstore_valid_columns(
        self, table: PreparedTableSchema, new_columns: Sequence[TColumnSchema]
    ) -> Sequence[TColumnSchema]:
        return [self._get_columstore_valid_column(table, c) for c in new_columns]

    def _get_columstore_valid_column(
        self, table: PreparedTableSchema, c: TColumnSchema
    ) -> TColumnSchema:
        """
        Returns TColumnSchema that maps to a Synapse data type that can participate in a columnstore index.

        varchar(max), nvarchar(max), and varbinary(max) are replaced with
        varchar(n), nvarchar(n), and varbinary(n), respectively, where
        n equals the user-specified precision, or the maximum allowed
        value if the user did not specify a precision.
        """
        dbt = self.capabilities.get_type_mapper().to_destination_type(c, table)
        if "(max)" in dbt and "precision" not in c:
            if c["data_type"] in ("text", "json"):
                return {**c, **{"precision": VARCHAR_MAX_N}}
            elif c["data_type"] in ("binary"):
                return {**c, **{"precision": VARBINARY_MAX_N}}
            else:
                raise ValueError(
                    dbt, f"Unexpected database type `{dbt}` on dlt type `{c['data_type']}`"
                )

        return c

    def _create_replace_followup_jobs(
        self, table_chain: Sequence[PreparedTableSchema]
    ) -> List[FollowupJobRequest]:
        return SqlJobClientBase._create_replace_followup_jobs(self, table_chain)

    def prepare_load_table(self, table_name: str) -> PreparedTableSchema:
        table = super().prepare_load_table(table_name)
        if table_name in self.schema.dlt_table_names():
            # dlt tables should always be heap tables, because "for small lookup
            # tables, less than 60 million rows, consider using HEAP or clustered
            # index for faster query performance."
            table[TABLE_INDEX_TYPE_HINT] = "heap"  # type: ignore[typeddict-unknown-key]
        # https://learn.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-tables-index#heap-tables
        else:
            if TABLE_INDEX_TYPE_HINT not in table:
                # If present in parent table, fetch hint from there.
                table[TABLE_INDEX_TYPE_HINT] = get_inherited_table_hint(  # type: ignore[typeddict-unknown-key]
                    self.schema.tables, table_name, TABLE_INDEX_TYPE_HINT, allow_none=True
                )
        if table[TABLE_INDEX_TYPE_HINT] is None:  # type: ignore[typeddict-item]
            # Hint still not defined, fall back to default.
            table[TABLE_INDEX_TYPE_HINT] = self.config.default_table_index_type  # type: ignore[typeddict-unknown-key]
        return table

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        job = super().create_load_job(table, file_path, load_id, restore)
        if not job:
            assert ReferenceFollowupJobRequest.is_reference_job(
                file_path
            ), "Synapse must use staging to load files"
            job = SynapseCopyFileLoadJob(
                file_path,
                self.config.staging_config.credentials,  # type: ignore[arg-type]
                self.config.staging_use_msi,
            )
        return job

    def should_truncate_table_before_load_on_staging_destination(self, table_name: str) -> bool:
        return self.config.truncate_tables_on_staging_destination_before_load


class SynapseCopyFileLoadJob(CopyIntoFileLoadJob):
    def __init__(
        self,
        file_path: str,
        staging_credentials: Optional[
            Union[AzureCredentialsWithoutDefaults, AzureServicePrincipalCredentialsWithoutDefaults]
        ] = None,
        staging_use_msi: bool = False,
    ) -> None:
        self.staging_use_msi = staging_use_msi
        super().__init__(file_path, staging_credentials)

    def _get_copy_options(
        self,
        staging_credentials: Union[
            AzureCredentialsWithoutDefaults, AzureServicePrincipalCredentialsWithoutDefaults
        ],
    ) -> List[str]:
        if self.staging_use_msi:
            credential = "IDENTITY = 'Managed Identity'"
        elif isinstance(staging_credentials, AzureCredentialsWithoutDefaults):
            # re-use staging credentials for copy into Synapse
            sas_token = staging_credentials.azure_storage_sas_token
            credential = f"IDENTITY = 'Shared Access Signature', SECRET = '{sas_token}'"
        elif isinstance(staging_credentials, AzureServicePrincipalCredentialsWithoutDefaults):
            tenant_id = staging_credentials.azure_tenant_id
            endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
            identity = f"{staging_credentials.azure_client_id}@{endpoint}"
            secret = staging_credentials.azure_client_secret
            credential = f"IDENTITY = '{identity}', SECRET = '{secret}'"
        else:
            raise ConfigurationException(
                f"Credentials of type `{type(staging_credentials)}` not supported"
                " when loading data from staging into Synapse using `COPY INTO`."
            )
        # dlt-generated DDL statements will still create the table, but
        # enabling AUTO_CREATE_TABLE prevents a MalformedInputException.
        return [f"CREDENTIAL = ({credential})", "AUTO_CREATE_TABLE = 'ON'"]
