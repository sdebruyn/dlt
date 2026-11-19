from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List, Union
from unittest.mock import MagicMock

import pytest

from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.configuration.specs import (
    AzureCredentialsWithoutDefaults,
    AzureServicePrincipalCredentialsWithoutDefaults,
    CredentialsConfiguration,
)
from dlt.common.schema import Schema

from dlt.destinations.impl.synapse.synapse import SynapseCopyFileLoadJob

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
        azure_storage_account_name="account",
        azure_tenant_id="tenant",
        azure_client_id="client",
        azure_client_secret="secret",
    )


def _job(
    tmp_path: Path,
    credentials: Union[
        AzureCredentialsWithoutDefaults, AzureServicePrincipalCredentialsWithoutDefaults
    ],
    staging_use_msi: bool = False,
) -> SynapseCopyFileLoadJob:
    reference = tmp_path / "event.abc123.0.reference"
    reference.write_text(BLOB_PATH, encoding="utf-8")
    return SynapseCopyFileLoadJob(str(reference), credentials, staging_use_msi)


@pytest.mark.parametrize(
    "credentials,staging_use_msi,expected_credential",
    [
        (
            _sas_credentials(),
            False,
            "IDENTITY = 'Shared Access Signature', SECRET = 'sv=token'",
        ),
        (
            _principal_credentials(),
            False,
            (
                "IDENTITY = 'client@https://login.microsoftonline.com/tenant/oauth2/token',"
                " SECRET = 'secret'"
            ),
        ),
        (_sas_credentials(), True, "IDENTITY = 'Managed Identity'"),
    ],
    ids=["sas", "principal", "msi"],
)
def test_copy_options(
    tmp_path: Path,
    credentials: Union[
        AzureCredentialsWithoutDefaults, AzureServicePrincipalCredentialsWithoutDefaults
    ],
    staging_use_msi: bool,
    expected_credential: str,
) -> None:
    job = _job(tmp_path, credentials, staging_use_msi)

    assert job._get_copy_options(credentials) == [
        f"CREDENTIAL = ({expected_credential})",
        "AUTO_CREATE_TABLE = 'ON'",
    ]


def test_copy_options_reject_other_credentials(tmp_path: Path) -> None:
    job = _job(tmp_path, _sas_credentials())

    with pytest.raises(ConfigurationException):
        job._get_copy_options(CredentialsConfiguration())  # type: ignore[arg-type]


def test_run_copies_file_with_auto_create(tmp_path: Path) -> None:
    sql_client = _RecordingSqlClient()
    job = _job(tmp_path, _sas_credentials())
    job._job_client = MagicMock(sql_client=sql_client)
    job.set_run_vars("load_id", Schema("schema"), {"name": "event"})

    job.run()

    assert sql_client.statements == [
        "COPY INTO [ds].[event]\n"
        "FROM 'https://account.blob.core.windows.net/container/ds/event/1.parquet'\n"
        "WITH (\n"
        "    FILE_TYPE = 'PARQUET',\n"
        "    CREDENTIAL = (IDENTITY = 'Shared Access Signature', SECRET = 'sv=token'),\n"
        "    AUTO_CREATE_TABLE = 'ON'\n"
        ")"
    ]
