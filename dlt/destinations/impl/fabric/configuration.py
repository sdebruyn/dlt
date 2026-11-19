"""Configuration for Fabric Warehouse destination with COPY INTO support"""

from typing import Optional, Final, ClassVar, Dict, Any, List
from dlt.common.configuration import configspec, NotResolved
from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.configuration.specs import AzureServicePrincipalCredentials
from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import Annotated, TSecretStrValue
from dlt.common.utils import digest128
from dlt.destinations.impl.mssql.configuration import (
    apply_authentication_to_dsn,
    build_access_token_attrs_before,
    normalize_authentication,
    select_token_provider,
    strip_token_incompatible_keys,
    uses_explicit_token,
    validate_authentication,
)
from dlt import version

_AZURE_STORAGE_EXTRA = f"{version.DLT_PKG_NAME}[az]"


@configspec(init=False)
class FabricCredentials(AzureServicePrincipalCredentials):
    """Credentials for Microsoft Fabric Warehouse with Microsoft Entra ID authentication.

    Inherits from AzureServicePrincipalCredentials for Service Principal fields and
    automatic fallback to DefaultAzureCredential.
    """

    drivername: str = "mssql+pyodbc"
    """SQLAlchemy driver name for SQL Server/Fabric."""

    host: str = None
    """Fabric Warehouse host (e.g., abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com)"""

    port: int = 1433
    """Database port (default: 1433)"""

    database: str = None
    """Fabric Warehouse database name"""

    connect_timeout: int = 15
    """Connection timeout in seconds (default: 15)"""

    authentication: Optional[str] = None
    """Entra ID method `mssql-python` signs in with, e.g. `ActiveDirectoryDefault` (alias `default`).
    Defaults to `ActiveDirectoryServicePrincipal` when a client secret is set, else `ActiveDirectoryDefault`"""

    username: Optional[str] = None
    """User principal name for `ActiveDirectoryPassword`"""

    password: Optional[TSecretStrValue] = None
    """Password for `ActiveDirectoryPassword`"""

    access_token: Optional[TSecretStrValue] = None
    """Pre-acquired Entra ID access token. Takes precedence over `azure_credential` and `authentication`"""

    azure_credential: Annotated[Optional[Any], NotResolved()] = None
    """`TokenCredential` the driver acquires tokens from, e.g. `DefaultAzureCredential()`. Takes precedence over `authentication`"""

    # Override to make optional - not needed for Fabric Warehouse credentials (only for staging)
    azure_storage_account_name: Optional[str] = None
    """Not used for Fabric Warehouse credentials (only staging credentials need this)"""

    def on_partial(self) -> None:
        """Enable fallback to DefaultAzureCredential if no Service Principal secret is provided."""
        self._apply_default_authentication()
        # Resolve if we have warehouse connection details (not storage account name)
        if self.host and self.database:
            self.resolve()

    def on_resolved(self) -> None:
        self._apply_default_authentication()
        validate_authentication(self, self.authentication)

    def _default_authentication(self) -> str:
        if self.azure_client_id and self.azure_client_secret and self.azure_tenant_id:
            return "ActiveDirectoryServicePrincipal"
        return "ActiveDirectoryDefault"

    def _apply_default_authentication(self) -> None:
        if uses_explicit_token(self):
            return
        if not self.authentication:
            self.authentication = self._default_authentication()
        if (
            normalize_authentication(self.authentication) == "ActiveDirectoryDefault"
            and not self.has_default_credentials()
        ):
            try:
                from azure.identity import DefaultAzureCredential
            except ModuleNotFoundError:
                raise MissingDependencyException(self.__class__.__name__, [_AZURE_STORAGE_EXTRA])
            # dbt runners pick the `fabric_default` profile for credentials with a default
            self._set_default_credentials(DefaultAzureCredential())

    def get_odbc_dsn_dict(self) -> Dict[str, Any]:
        """Build the connection string keywords for `mssql-python`, which brings its own driver."""
        params: Dict[str, Any] = {
            "SERVER": f"{self.host},{self.port}",
            "DATABASE": self.database,
            "Encrypt": "yes",
            "TrustServerCertificate": "no",
        }
        apply_authentication_to_dsn(
            self, params, self.authentication or self._default_authentication()
        )
        return params

    def to_odbc_dsn(self) -> str:
        """Build the connection string for `mssql-python`."""
        params = strip_token_incompatible_keys(self, self.get_odbc_dsn_dict())
        return ";".join(f"{k}={v}" for k, v in params.items())

    def to_odbc_attrs_before(self) -> Optional[Dict[int, bytes]]:
        """Return `attrs_before` carrying the pre-acquired `access_token`, or `None`."""
        return build_access_token_attrs_before(self)

    def to_odbc_token_provider(self) -> Optional[Any]:
        """Return the `azure_credential` the driver acquires tokens from, or `None`."""
        return select_token_provider(self)

    def to_native_credentials(self) -> Optional[Any]:
        """Return credentials in a format suitable for the native driver/library."""
        return self.get_odbc_dsn_dict()


@configspec
class FabricClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    """Configuration for Fabric Warehouse destination with staging and collation support.

    Uses FabricCredentials for Service Principal authentication.
    Supports OneLake/Lakehouse or Azure Blob Storage staging with COPY INTO for efficient data loading.

    Example usage with OneLake/Lakehouse staging (recommended):
        fabric(
            credentials={
                "host": "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com",
                "database": "mydb",
                "tenant_id": "your-tenant-id",
                "client_id": "your-client-id",
                "client_secret": "your-client-secret",
            },
            staging=filesystem(
                # IMPORTANT: Must use workspace GUID and lakehouse GUID (not names)
                # Format: abfss://<workspace_guid>@onelake.dfs.fabric.microsoft.com/<lakehouse_guid>/Files
                bucket_url="abfss://12345678-1234-1234-1234-123456789012@onelake.dfs.fabric.microsoft.com/87654321-4321-4321-4321-210987654321/Files",
                # IMPORTANT: Must specify Service Principal credentials (same as warehouse)
                credentials={
                    "azure_storage_account_name": "onelake",
                    "azure_account_host": "onelake.blob.fabric.microsoft.com",
                    "azure_tenant_id": "your-tenant-id",
                    "azure_client_id": "your-client-id",
                    "azure_client_secret": "your-client-secret",
                },
            ),
            collation="Latin1_General_100_BIN2_UTF8",
        )

    Note: The bucket_url must use GUIDs for both workspace and lakehouse, not their display names.
    You can find these GUIDs in the Fabric portal workspace/lakehouse URLs.

    Example usage with Azure Blob Storage staging:
        fabric(
            credentials={
                "host": "abc12345-6789-def0-1234-56789abcdef0.datawarehouse.fabric.microsoft.com",
                "database": "mydb",
                "tenant_id": "your-tenant-id",
                "client_id": "your-client-id",
                "client_secret": "your-client-secret",
            },
            staging=filesystem(
                bucket_url="az://your-container",
                credentials={
                    "azure_storage_account_name": "your-account-name",
                    "azure_storage_account_key": "your-account-key",
                },
            ),
            collation="Latin1_General_100_BIN2_UTF8",
        )
    """

    destination_type: Final[str] = "fabric"  # type: ignore[misc]
    credentials: Optional[FabricCredentials] = None

    collation: Optional[str] = "Latin1_General_100_BIN2_UTF8"
    """Database collation to use for text columns.

    Note: Fabric Warehouse does not support table indexing. Storage is automatically managed by the system.
    """

    # Set to False by default because PRIMARY KEY and UNIQUE constraints
    # are NOT ENFORCED in Fabric and can lead to inaccurate results
    create_indexes: bool = False
    """Whether `primary_key` and `unique` column hints are applied."""

    has_case_sensitive_identifiers: bool = True
    """Whether identifiers (table/column names) are case-sensitive. Depends on database collation."""

    __config_gen_annotations__: ClassVar[List[str]] = [
        "default_table_index_type",
        "create_indexes",
    ]

    """Database collation for varchar columns. Fabric supports:
    - Latin1_General_100_BIN2_UTF8 (default, case-sensitive)
    - Latin1_General_100_CI_AS_KS_WS_SC_UTF8 (case-insensitive)

    Both have UTF-8 encoding.
    """

    def data_location(self) -> str:
        """Returns host:port."""
        if not self.credentials or not self.credentials.host:
            self._no_data_location("the configuration has no host")
        port = self.credentials.port or 1433
        return f"{self.credentials.host}:{port}"

    def fingerprint(self) -> str:
        """Returns a fingerprint of the physical Fabric location."""
        # a fingerprint can say "cannot compute", where a location raises instead
        try:
            return digest128(self.data_location())
        except ConfigurationValueError:
            return ""


__all__ = ["FabricCredentials", "FabricClientConfiguration"]
