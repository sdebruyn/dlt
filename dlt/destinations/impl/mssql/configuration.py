import dataclasses
import struct
from typing import ClassVar, Any, Final, List, Dict, Literal, Optional

from dlt.common.configuration import configspec, NotResolved
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.typing import Annotated, TSecretStrValue
from dlt.common.exceptions import SystemConfigurationException

from dlt.common.destination.client import DestinationClientDwhWithStagingConfiguration
from dlt.common.utils import digest128


def escape_mssql_odbc_value(value: Optional[str]) -> str:
    """Escape a value for MSSQL ADO/ODBC connection string format.

    ODBC format supports `{value}` syntax where:
      - `}` inside braces must be doubled to `}}`
      - `;` can safely appear inside braces

    To safely handle values with special characters, we use ODBC-style bracing:
    - Values containing `;` or `}` are wrapped in `{}`
    - `}` inside the value is escaped as `}}`

    Args:
        value: The value to escape

    Returns:
        Escaped value safe for use in ADO/ODBC connection string
    """
    if not value:
        return ""
    # if value contains ; or }, use braced syntax with }} escaping
    if ";" in value or "}" in value:
        return "{" + value.replace("}", "}}") + "}"
    return value


def build_odbc_dsn(params: Dict[str, Any]) -> str:
    """Build an ADO/ODBC connection string for MSSQL, escaping values

    Args:
        params: Dictionary of connection parameters

    Returns:
        ADO/ODBC connection string
    """
    return ";".join(
        f"{k}={escape_mssql_odbc_value(str(v))}" for k, v in params.items() if v is not None
    )


SQL_COPT_SS_ACCESS_TOKEN = 1256
"""Connection attribute that carries a pre-acquired Entra ID access token in `attrs_before`."""

SUPPORTED_AUTHENTICATION = frozenset(
    {
        "ActiveDirectoryServicePrincipal",
        "ActiveDirectoryPassword",
        "ActiveDirectoryIntegrated",
        "ActiveDirectoryInteractive",
        "ActiveDirectoryMsi",
        "ActiveDirectoryDefault",
        "ActiveDirectoryDeviceCode",
    }
)
"""Entra ID methods `mssql-python` signs in with from the `Authentication` connection keyword."""

_AUTHENTICATION_ALIASES = {"default": "ActiveDirectoryDefault"}

# mssql-python drops these keys once a token is injected, dlt drops them first so only one
# identity reaches the driver
_TOKEN_INCOMPATIBLE_DSN_KEYS = frozenset({"authentication", "uid", "pwd", "trusted_connection"})


def normalize_authentication(authentication: str) -> str:
    """Returns the canonical `ActiveDirectory*` name for an `authentication` value or alias."""
    return _AUTHENTICATION_ALIASES.get(authentication.lower(), authentication)


def uses_explicit_token(credentials: Any) -> bool:
    """Tells if an `access_token` or an `azure_credential` replaces `authentication`."""
    # identity, not truthiness: a credential object may define `__bool__` or `__len__`
    return bool(credentials.access_token) or credentials.azure_credential is not None


def validate_authentication(credentials: Any, authentication: Optional[str]) -> None:
    """Raises `ConfigurationException` for an unsupported or incomplete `authentication`."""
    if uses_explicit_token(credentials) or not authentication:
        return
    normalized = normalize_authentication(authentication)
    if normalized not in SUPPORTED_AUTHENTICATION:
        raise ConfigurationException(
            f"Unsupported `authentication` method `{authentication}`."
            f" Supported methods: {', '.join(sorted(SUPPORTED_AUTHENTICATION))}."
        )
    if normalized == "ActiveDirectoryPassword" and not (
        credentials.username and credentials.password
    ):
        raise ConfigurationException(
            "`authentication = ActiveDirectoryPassword` requires `username` and `password`."
        )


def apply_authentication_to_dsn(
    credentials: Any, params: Dict[str, Any], authentication: Optional[str]
) -> None:
    """Adds the `Authentication`, `UID` and `PWD` keywords for `authentication` to `params`.

    Adds nothing when an explicit token replaces `authentication`, and a plain SQL login when
    `authentication` is empty.
    """
    if uses_explicit_token(credentials):
        return
    if not authentication:
        params["UID"] = credentials.username
        params["PWD"] = credentials.password
        return
    authentication = normalize_authentication(authentication)
    params["AUTHENTICATION"] = authentication
    client_id = getattr(credentials, "azure_client_id", None)
    tenant_id = getattr(credentials, "azure_tenant_id", None)
    client_secret = getattr(credentials, "azure_client_secret", None)
    if authentication == "ActiveDirectoryServicePrincipal" and (
        client_id and tenant_id and client_secret
    ):
        params["UID"] = f"{client_id}@{tenant_id}"
        params["PWD"] = str(client_secret)
    elif authentication in ("ActiveDirectoryServicePrincipal", "ActiveDirectoryPassword") and (
        credentials.username and credentials.password
    ):
        params["UID"] = credentials.username
        params["PWD"] = credentials.password


def build_access_token_attrs_before(credentials: Any) -> Optional[Dict[int, bytes]]:
    """Returns `attrs_before` carrying the pre-acquired `access_token`, or `None`."""
    if not credentials.access_token:
        return None
    encoded_token = str(credentials.access_token).encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(encoded_token)}s", len(encoded_token), encoded_token)
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}


def select_token_provider(credentials: Any) -> Optional[Any]:
    """Returns the `azure_credential` the driver acquires tokens from, or `None`.

    An `access_token` suppresses it, since the driver rejects both token sources at once.
    """
    if credentials.access_token:
        return None
    return credentials.azure_credential


def strip_token_incompatible_keys(credentials: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Drops the connection keywords an explicit token replaces."""
    if not uses_explicit_token(credentials):
        return params
    return {k: v for k, v in params.items() if k.lower() not in _TOKEN_INCOMPATIBLE_DSN_KEYS}


TMsSqlConnector = Literal["pyodbc", "mssql-python"]


@configspec(init=False)
class MsSqlCredentials(ConnectionStringCredentials):
    drivername: Final[str] = dataclasses.field(default="mssql", init=False, repr=False, compare=False)  # type: ignore[misc]
    database: str = None
    username: str = None
    password: TSecretStrValue = None
    host: str = None
    port: int = 1433
    connect_timeout: int = 30
    driver: Optional[str] = None
    """ODBC driver name for `pyodbc`, detected from the installed drivers when not set"""
    connector: TMsSqlConnector = "pyodbc"
    """Python driver to connect with: `pyodbc` or `mssql-python`, which brings its own driver libraries"""
    authentication: Optional[str] = None
    """Entra ID method `mssql-python` signs in with, e.g. `ActiveDirectoryServicePrincipal`. SQL login when not set"""
    access_token: Optional[TSecretStrValue] = None
    """Pre-acquired Entra ID access token for `mssql-python`. Takes precedence over `azure_credential` and `authentication`"""
    azure_credential: Annotated[Optional[Any], NotResolved()] = None
    """`TokenCredential` `mssql-python` acquires tokens from. Takes precedence over `authentication`"""

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "connect_timeout"]

    SUPPORTED_DRIVERS: ClassVar[List[str]] = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
    ]

    def parse_native_representation(self, native_value: Any) -> None:
        # TODO: Support ODBC connection string or sqlalchemy URL
        super().parse_native_representation(native_value)
        if self.query is not None:
            self.query = {k.lower(): v for k, v in self.query.items()}  # Make case-insensitive.
        self.driver = self.query.get("driver", self.driver)
        self.connector = self.query.pop("connector", self.connector)
        self.connect_timeout = int(self.query.get("connect_timeout", self.connect_timeout))

    def on_resolved(self) -> None:
        if self.connector == "mssql-python":
            validate_authentication(self, self.authentication)
        else:
            if self.authentication or uses_explicit_token(self):
                raise ConfigurationException(
                    "`authentication`, `access_token` and `azure_credential` require"
                    ' `connector = "mssql-python"`.'
                )
            # a later resolution pass may still select mssql-python, so a missing ODBC driver is
            # reported only when pyodbc connects
            self.driver = self.driver or self._detect_driver()
            if self.driver and self.driver not in self.SUPPORTED_DRIVERS:
                raise SystemConfigurationException(
                    f"The specified driver `{self.driver}` is not supported."
                    f" Choose one of the supported drivers: {', '.join(self.SUPPORTED_DRIVERS)}."
                )
        self.database = self.database.lower()

    def get_query(self) -> Dict[str, Any]:
        query = dict(super().get_query())
        query["connect_timeout"] = self.connect_timeout
        if self.connector != "pyodbc":
            query["connector"] = self.connector
        return query

    def on_partial(self) -> None:
        if not self.is_partial():
            self.resolve()

    def _detect_driver(self) -> Optional[str]:
        try:
            import pyodbc
        except ModuleNotFoundError:
            return None
        available_drivers = pyodbc.drivers()
        for d in self.SUPPORTED_DRIVERS:
            if d in available_drivers:
                return d
        return None

    def _get_driver(self) -> str:
        driver = self.driver or self._detect_driver()
        if driver:
            return driver
        docs_url = "https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16"
        raise SystemConfigurationException(
            f"No supported ODBC driver found for MS SQL Server.  See {docs_url} for information on"
            f" how to install the `{self.SUPPORTED_DRIVERS[0]}` on your platform."
        )

    def get_odbc_dsn_dict(self) -> Dict[str, Any]:
        params: Dict[str, Any]
        if self.connector == "mssql-python":
            params = {"SERVER": f"{self.host},{self.port}", "DATABASE": self.database}
            apply_authentication_to_dsn(self, params, self.authentication)
        else:
            params = {
                "DRIVER": self._get_driver(),
                "SERVER": f"{self.host},{self.port}",
                "DATABASE": self.database,
                "UID": self.username,
                "PWD": self.password,
            }
        if self.query is not None:
            params.update({k.upper(): v for k, v in self.query.items()})
        return params

    def to_odbc_dsn(self) -> str:
        params = strip_token_incompatible_keys(self, self.get_odbc_dsn_dict())
        return build_odbc_dsn(params)

    def to_odbc_attrs_before(self) -> Optional[Dict[int, bytes]]:
        """Return `attrs_before` carrying the pre-acquired `access_token`, or `None`."""
        return build_access_token_attrs_before(self)

    def to_odbc_token_provider(self) -> Optional[Any]:
        """Return the `azure_credential` the driver acquires tokens from, or `None`."""
        return select_token_provider(self)


@configspec
class MsSqlClientConfiguration(DestinationClientDwhWithStagingConfiguration):
    destination_type: Final[str] = dataclasses.field(default="mssql", init=False, repr=False, compare=False)  # type: ignore[misc]
    credentials: MsSqlCredentials = None

    create_indexes: bool = False
    has_case_sensitive_identifiers: bool = False

    def fingerprint(self) -> str:
        """Returns a fingerprint of the configured host."""
        if self.credentials and self.credentials.host:
            return digest128(self.credentials.host)
        return ""

    def data_location(self) -> str:
        """Returns host:port."""
        if not self.credentials or not self.credentials.host:
            self._no_data_location("the configuration has no host")
        port = self.credentials.port or 1433
        return f"{self.credentials.host}:{port}"
