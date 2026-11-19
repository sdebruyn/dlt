"""SQL client for Fabric Warehouse"""

from typing import TYPE_CHECKING

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.impl.mssql.mssql_python_client import MsSqlPythonClient

if TYPE_CHECKING:
    from dlt.destinations.impl.fabric.configuration import FabricCredentials


class FabricSqlClient(MsSqlPythonClient):
    """SQL client for Microsoft Fabric Warehouse"""

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: "FabricCredentials",
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(dataset_name, staging_dataset_name, credentials, capabilities)
        self.credentials: "FabricCredentials" = credentials
