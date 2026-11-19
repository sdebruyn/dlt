from dlt.destinations.impl.mssql.mssql_python_client import MsSqlPythonClient
from dlt.destinations.impl.synapse.sql_client import SynapseDropTablesMixin


class SynapseMsSqlPythonClient(SynapseDropTablesMixin, MsSqlPythonClient):
    pass
