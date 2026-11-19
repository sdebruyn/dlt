from contextlib import suppress
from typing import Any, cast

from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.sql_client import SqlClientBase


class SynapseDropTablesMixin:
    def drop_tables(self, *tables: str) -> None:
        if not tables:
            return
        sql_client = cast(SqlClientBase[Any], self)
        # Synapse does not support DROP TABLE IF EXISTS.
        # Workaround: use DROP TABLE and suppress non-existence errors.
        statements = [f"DROP TABLE {sql_client.make_qualified_table_name(t)}" for t in tables]
        for statement in statements:
            with suppress(DatabaseUndefinedRelation):
                sql_client.execute_sql(statement)


class SynapseSqlClient(SynapseDropTablesMixin, PyOdbcMsSqlClient):
    pass
