import os
from typing import Dict

import pyodbc
import pytest

from dlt.common.configuration import ConfigFieldMissingException, resolve_configuration
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.exceptions import SystemConfigurationException
from dlt.common.schema import Schema
from dlt.common.utils import digest128
from dlt.destinations import mssql
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration, MsSqlCredentials

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_mssql_factory() -> None:
    schema = Schema("schema")
    dest = mssql()
    client = dest.client(schema, MsSqlClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.create_indexes is False
    assert client.config.has_case_sensitive_identifiers is False
    assert client.capabilities.has_case_sensitive_identifiers is False
    assert client.capabilities.casefold_identifier is str

    # MSSQL uses ADBC for parquet loading which doesn't support dictionary-encoded arrays
    assert client.capabilities.parquet_format is not None
    assert client.capabilities.parquet_format.supports_dictionary_encoding is False

    # set args explicitly
    dest = mssql(has_case_sensitive_identifiers=True, create_indexes=True)
    client = dest.client(schema, MsSqlClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.create_indexes is True
    assert client.config.has_case_sensitive_identifiers is True
    assert client.capabilities.has_case_sensitive_identifiers is True
    assert client.capabilities.casefold_identifier is str

    # set args via config
    os.environ["DESTINATION__CREATE_INDEXES"] = "True"
    os.environ["DESTINATION__HAS_CASE_SENSITIVE_IDENTIFIERS"] = "True"
    dest = mssql()
    client = dest.client(schema, MsSqlClientConfiguration()._bind_dataset_name("dataset"))
    assert client.config.create_indexes is True
    assert client.config.has_case_sensitive_identifiers is True
    assert client.capabilities.has_case_sensitive_identifiers is True
    assert client.capabilities.casefold_identifier is str


def test_mssql_credentials_defaults() -> None:
    creds = MsSqlCredentials()
    assert creds.port == 1433
    assert creds.connect_timeout == 30
    assert MsSqlCredentials.__config_gen_annotations__ == ["port", "connect_timeout"]
    # port should be optional
    resolve_configuration(creds, explicit_value="mssql://loader:loader@localhost/dlt_data")
    assert creds.port == 1433


@pytest.mark.parametrize(
    "connection_string,expected_fingerprint",
    [
        pytest.param("", "", id="empty"),
        pytest.param(
            "mssql://user1:pass1@host1:1433/db1",
            digest128("host1"),
            id="legacy_host_only_default_port",
        ),
        pytest.param(
            "mssql://user1:pass1@host1:1434/db1",
            digest128("host1"),
            id="legacy_host_only_custom_port",
        ),
    ],
)
def test_mssql_fingerprint(connection_string: str, expected_fingerprint: str) -> None:
    if connection_string:
        credentials = MsSqlCredentials(connection_string)
        config = MsSqlClientConfiguration(credentials=credentials)
    else:
        config = MsSqlClientConfiguration()

    assert config.fingerprint() == expected_fingerprint


def test_parse_native_representation() -> None:
    # Case: unsupported driver specified.
    with pytest.raises(SystemConfigurationException):
        resolve_configuration(
            MsSqlCredentials(
                "mssql://test_user:test_pwd@sql.example.com/test_db?DRIVER=ODBC+Driver+13+for+SQL+Server"
            )
        )
    # Case: password not specified.
    with pytest.raises(ConfigFieldMissingException):
        resolve_configuration(
            MsSqlCredentials(
                "mssql://test_user@sql.example.com/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
            )
        )


def test_to_odbc_dsn_supported_driver_specified() -> None:
    # Case: supported driver specified — ODBC Driver 18 for SQL Server.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 18 for SQL Server",
        "SERVER": "sql.example.com,1433",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
    }

    # Case: supported driver specified — ODBC Driver 17 for SQL Server.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com/test_db?DRIVER=ODBC+Driver+17+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 17 for SQL Server",
        "SERVER": "sql.example.com,1433",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
    }

    # Case: port and supported driver specified.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com:12345/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 18 for SQL Server",
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
    }


def test_to_odbc_dsn_arbitrary_keys_specified() -> None:
    # Case: arbitrary query keys (and supported driver) specified.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com:12345/test_db?FOO=a&BAR=b&DRIVER=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 18 for SQL Server",
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
        "FOO": "a",
        "BAR": "b",
    }

    # Case: arbitrary capitalization.
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com:12345/test_db?FOO=a&bar=b&Driver=ODBC+Driver+18+for+SQL+Server"
        )
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result == {
        "DRIVER": "ODBC Driver 18 for SQL Server",
        "SERVER": "sql.example.com,12345",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
        "FOO": "a",
        "BAR": "b",
    }


available_drivers = [d for d in pyodbc.drivers() if d in MsSqlCredentials.SUPPORTED_DRIVERS]


@pytest.mark.skipif(not available_drivers, reason="no supported driver available")
def test_to_odbc_dsn_driver_not_specified() -> None:
    # Case: driver not specified, but supported driver is available.
    creds = resolve_configuration(
        MsSqlCredentials("mssql://test_user:test_pwd@sql.example.com/test_db")
    )
    dsn = creds.to_odbc_dsn()
    result = {k: v for k, v in (param.split("=") for param in dsn.split(";"))}
    assert result in [
        {
            "DRIVER": d,
            "SERVER": "sql.example.com,1433",
            "DATABASE": "test_db",
            "UID": "test_user",
            "PWD": "test_pwd",
        }
        for d in MsSqlCredentials.SUPPORTED_DRIVERS
    ]


def _dsn_dict(dsn: str) -> Dict[str, str]:
    return dict(param.split("=", 1) for param in dsn.split(";"))


def test_mssql_python_connector_from_connection_string(monkeypatch: pytest.MonkeyPatch) -> None:
    # no ODBC driver is needed when mssql-python connects
    monkeypatch.setattr(pyodbc, "drivers", lambda: [])
    creds = resolve_configuration(
        MsSqlCredentials(
            "mssql://test_user:test_pwd@sql.example.com/test_db?connector=mssql-python"
        )
    )

    assert creds.connector == "mssql-python"
    assert creds.driver is None
    assert _dsn_dict(creds.to_odbc_dsn()) == {
        "SERVER": "sql.example.com,1433",
        "DATABASE": "test_db",
        "UID": "test_user",
        "PWD": "test_pwd",
    }
    assert "connector=mssql-python" in creds.to_native_representation()


@pytest.mark.parametrize(
    "authentication,username,password,expected",
    [
        (
            "ActiveDirectoryDefault",
            None,
            None,
            {"AUTHENTICATION": "ActiveDirectoryDefault"},
        ),
        (
            "default",
            None,
            None,
            {"AUTHENTICATION": "ActiveDirectoryDefault"},
        ),
        (
            "ActiveDirectoryServicePrincipal",
            "client-id@tenant-id",
            "secret",
            {
                "AUTHENTICATION": "ActiveDirectoryServicePrincipal",
                "UID": "client-id@tenant-id",
                "PWD": "secret",
            },
        ),
        (
            "ActiveDirectoryPassword",
            "user@contoso.com",
            "pwd",
            {"AUTHENTICATION": "ActiveDirectoryPassword", "UID": "user@contoso.com", "PWD": "pwd"},
        ),
    ],
    ids=["default", "default-alias", "service-principal", "password"],
)
def test_mssql_python_entra_id_authentication(
    authentication: str, username: str, password: str, expected: Dict[str, str]
) -> None:
    creds = MsSqlCredentials()
    creds.host = "sql.example.com"
    creds.database = "test_db"
    creds.connector = "mssql-python"
    creds.authentication = authentication
    creds.username = username
    creds.password = password
    creds = resolve_configuration(creds)

    assert _dsn_dict(creds.to_odbc_dsn()) == {
        "SERVER": "sql.example.com,1433",
        "DATABASE": "test_db",
        **expected,
    }


def test_mssql_python_access_token_replaces_sql_login() -> None:
    creds = MsSqlCredentials(
        "mssql://test_user:test_pwd@sql.example.com/test_db?connector=mssql-python"
    )
    creds.access_token = "explicit-token"
    creds = resolve_configuration(creds)

    assert _dsn_dict(creds.to_odbc_dsn()) == {
        "SERVER": "sql.example.com,1433",
        "DATABASE": "test_db",
    }
    attrs = creds.to_odbc_attrs_before()
    assert attrs is not None
    assert attrs[1256][4:].decode("utf-16-le") == "explicit-token"
    assert creds.to_odbc_token_provider() is None


@pytest.mark.parametrize(
    "option,value",
    [("authentication", "ActiveDirectoryDefault"), ("access_token", "explicit-token")],
    ids=["authentication", "access-token"],
)
def test_pyodbc_rejects_entra_id_options(option: str, value: str) -> None:
    creds = MsSqlCredentials(
        "mssql://test_user:test_pwd@sql.example.com/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
    )
    setattr(creds, option, value)
    with pytest.raises(ConfigurationException, match="mssql-python"):
        resolve_configuration(creds)


@pytest.mark.parametrize(
    "connector,expected_client",
    [("pyodbc", "PyOdbcMsSqlClient"), ("mssql-python", "MsSqlPythonClient")],
    ids=["pyodbc", "mssql-python"],
)
def test_mssql_job_client_uses_selected_connector(connector: str, expected_client: str) -> None:
    creds = MsSqlCredentials(
        "mssql://test_user:test_pwd@sql.example.com/test_db?DRIVER=ODBC+Driver+18+for+SQL+Server"
    )
    creds.connector = connector  # type: ignore[assignment]
    config = MsSqlClientConfiguration(credentials=creds)._bind_dataset_name("dataset")
    client = mssql().client(Schema("schema"), config)

    assert type(client.sql_client).__name__ == expected_client
