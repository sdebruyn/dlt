"""SQL client for Fabric Warehouse"""

from typing import TYPE_CHECKING

from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.impl.mssql.sql_client import PyOdbcMsSqlClient

if TYPE_CHECKING:
    from dlt.destinations.impl.fabric.configuration import FabricCredentials


class FabricSqlClient(PyOdbcMsSqlClient):
    """SQL client for Microsoft Fabric Warehouse"""

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: "FabricCredentials",
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        # FabricCredentials has all required attributes: database, to_odbc_dsn(), connect_timeout
        super().__init__(dataset_name, staging_dataset_name, credentials, capabilities)  # type: ignore[arg-type]
        self.credentials: "FabricCredentials" = credentials  # type: ignore[assignment]
