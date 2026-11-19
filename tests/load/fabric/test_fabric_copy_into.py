from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List, Union
from unittest.mock import MagicMock

import pytest

from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.configuration.specs import (
    AzureCredentialsWithoutDefaults,
    AzureServicePrincipalCredentialsWithoutDefaults,
)
from dlt.common.schema import Schema

pytest.importorskip("dlt.destinations.impl.fabric.fabric", reason="MSSQL ODBC driver not installed")

from dlt.destinations.impl.fabric.fabric import FabricCopyFileLoadJob

ONELAKE_PATH = "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid/Files/ds/event/1.parquet"
BLOB_PATH = "az://container/ds/event/1.parquet"


class _RecordingSqlClient:
    dataset_name = "ds"

    def __init__(self) -> None:
        self.statements: List[str] = []

    @contextmanager
    def begin_transaction(self) -> Iterator[None]:
        yield

    def execute_sql(self, sql: str) -> None:
        self.statements.append(sql)


def _sas_credentials() -> AzureCredentialsWithoutDefaults:
    return AzureCredentialsWithoutDefaults(
        azure_storage_account_name="account", azure_storage_sas_token="sv=token"
    )


def _principal_credentials() -> AzureServicePrincipalCredentialsWithoutDefaults:
    return AzureServicePrincipalCredentialsWithoutDefaults(
        azure_storage_account_name="onelake",
        azure_tenant_id="tenant",
        azure_client_id="client",
        azure_client_secret="secret",
    )


def _job(
    tmp_path: Path,
    bucket_path: str,
    credentials: Union[
        AzureCredentialsWithoutDefaults, AzureServicePrincipalCredentialsWithoutDefaults
    ],
) -> FabricCopyFileLoadJob:
    reference = tmp_path / "event.abc123.0.reference"
    reference.write_text(bucket_path, encoding="utf-8")
    return FabricCopyFileLoadJob(str(reference), credentials)


def test_onelake_with_principal_initializes_token_and_sends_no_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    initialize = MagicMock()
    monkeypatch.setattr(FabricCopyFileLoadJob, "_ensure_fabric_token_initialized", initialize)
    credentials = _principal_credentials()

    job = _job(tmp_path, ONELAKE_PATH, credentials)

    assert job._get_copy_options(credentials) == []
    initialize.assert_called_once_with(credentials)


def test_onelake_with_sas_sends_no_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    initialize = MagicMock()
    monkeypatch.setattr(FabricCopyFileLoadJob, "_ensure_fabric_token_initialized", initialize)
    credentials = _sas_credentials()

    assert _job(tmp_path, ONELAKE_PATH, credentials)._get_copy_options(credentials) == []
    initialize.assert_not_called()


def test_blob_storage_sends_sas_credential(tmp_path: Path) -> None:
    credentials = _sas_credentials()

    assert _job(tmp_path, BLOB_PATH, credentials)._get_copy_options(credentials) == [
        "CREDENTIAL = (IDENTITY = 'Shared Access Signature', SECRET = 'sv=token')"
    ]


def test_blob_storage_rejects_principal(tmp_path: Path) -> None:
    credentials = _principal_credentials()

    with pytest.raises(ConfigurationException):
        _job(tmp_path, BLOB_PATH, credentials)._get_copy_options(credentials)


@pytest.mark.parametrize(
    "bucket_path,expected",
    [
        (
            ONELAKE_PATH,
            "https://onelake.dfs.fabric.microsoft.com/ws-guid/lh-guid/Files/ds/event/1.parquet",
        ),
        (BLOB_PATH, "https://account.blob.core.windows.net/container/ds/event/1.parquet"),
    ],
    ids=["onelake", "blob-storage"],
)
def test_https_path(tmp_path: Path, bucket_path: str, expected: str) -> None:
    job = _job(tmp_path, bucket_path, _sas_credentials())

    assert job._get_https_path(bucket_path, "account") == expected


def test_run_copies_blob_file_without_auto_create(tmp_path: Path) -> None:
    sql_client = _RecordingSqlClient()
    job = _job(tmp_path, BLOB_PATH, _sas_credentials())
    job._job_client = MagicMock(sql_client=sql_client)
    job.set_run_vars("load_id", Schema("schema"), {"name": "event"})

    job.run()

    assert sql_client.statements == [
        "COPY INTO [ds].[event]\n"
        "FROM 'https://account.blob.core.windows.net/container/ds/event/1.parquet'\n"
        "WITH (\n"
        "    FILE_TYPE = 'PARQUET',\n"
        "    CREDENTIAL = (IDENTITY = 'Shared Access Signature', SECRET = 'sv=token')\n"
        ")"
    ]
