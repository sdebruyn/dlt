import os
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock

import pytest

pytest.importorskip("mssql_python")

from dlt.common.configuration import resolve_configuration
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.exceptions import TerminalException
from dlt.common.libs.pyarrow import pyarrow
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchema
from dlt.destinations import mssql
from dlt.destinations.impl.mssql.bulk_copy import (
    has_native_arrow_bulk_copy,
    make_parquet_file_format_selector,
)
from dlt.destinations.impl.mssql.configuration import MsSqlClientConfiguration, MsSqlCredentials
from dlt.destinations.impl.mssql.mssql import (
    MsSqlBulkCopyArrowJob,
    MsSqlJobClient,
    MssqlParquetCopyJob,
)
from dlt.destinations.impl.mssql.mssql_python_client import MsSqlPythonClient

DOCS_URL = "https://dlthub.com/docs/dlt-ecosystem/destinations/mssql#data-loading"


def _write_parquet(path: str, columns: List[str], num_row_groups: int, rows_per_group: int) -> None:
    schema = pyarrow.schema([(name, pyarrow.int64()) for name in columns])
    with pyarrow.parquet.ParquetWriter(path, schema) as writer:
        for group in range(num_row_groups):
            values = list(range(group * rows_per_group, (group + 1) * rows_per_group))
            writer.write_table(
                pyarrow.table({name: values for name in columns}, schema=schema),
                row_group_size=rows_per_group or None,
            )


def _make_job_client(connector: str = "mssql-python", **credential_kwargs: Any) -> MsSqlJobClient:
    credentials = MsSqlCredentials()
    credentials.host = "sql.example.com"
    credentials.database = "test_db"
    credentials.username = "loader"
    credentials.password = "pass"
    credentials.driver = "ODBC Driver 18 for SQL Server"
    credentials.connector = connector  # type: ignore[assignment]
    for key, value in credential_kwargs.items():
        setattr(credentials, key, value)
    config = MsSqlClientConfiguration(
        credentials=resolve_configuration(credentials)
    )._bind_dataset_name("my_dataset")
    return mssql().client(Schema("schema"), config)


def _mock_connection(rows_copied: int) -> Tuple[MagicMock, MagicMock]:
    cursor = MagicMock()
    cursor.bulkcopy_arrow = MagicMock(return_value={"rows_copied": rows_copied, "batch_count": 1})
    connection = MagicMock()
    connection.cursor = MagicMock(return_value=cursor)
    return connection, cursor


def _make_job(
    file_path: str, rows_copied: int = 12, table_name: str = "items", **credential_kwargs: Any
) -> Tuple[MsSqlBulkCopyArrowJob, MagicMock]:
    """Builds a job on the mssql-python connector wired to a mocked cursor."""
    job_client = _make_job_client(**credential_kwargs)
    connection, cursor = _mock_connection(rows_copied)
    job_client.sql_client._conn = connection  # type: ignore[attr-defined]

    job = MsSqlBulkCopyArrowJob(file_path)
    job._job_client = job_client
    job._load_table = {"name": table_name}
    return job, cursor


def _bulkcopy_kwargs(cursor: MagicMock) -> Dict[str, Any]:
    return cursor.bulkcopy_arrow.call_args.kwargs


def test_bulk_copy_streams_a_reader_over_all_row_groups(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id", "value"], num_row_groups=3, rows_per_group=4)

    job, cursor = _make_job(file_path)
    job.run()

    assert cursor.bulkcopy_arrow.call_count == 1
    source = cursor.bulkcopy_arrow.call_args.args[1]
    # a reader, so the driver pulls row groups instead of dlt buffering the file
    assert isinstance(source, pyarrow.RecordBatchReader)
    assert source.read_all().num_rows == 12
    assert cursor.close.call_count == 1


def test_bulk_copy_reader_is_not_consumed_before_the_driver_pulls_it(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=4, rows_per_group=5)

    job, cursor = _make_job(file_path, rows_copied=20)
    job.run()

    source = cursor.bulkcopy_arrow.call_args.args[1]
    assert [batch.num_rows for batch in source] == [5, 5, 5, 5]


def test_bulk_copy_passes_qualified_table_and_explicit_column_mappings(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id", "name", "created_at"], num_row_groups=1, rows_per_group=2)

    job, cursor = _make_job(file_path, rows_copied=2, table_name="items")
    job.run()

    assert cursor.bulkcopy_arrow.call_args.args[0] == '"my_dataset"."items"'
    assert _bulkcopy_kwargs(cursor)["column_mappings"] == [
        (0, "id"),
        (1, "name"),
        (2, "created_at"),
    ]


def test_bulk_copy_sends_one_transactional_batch(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=3, rows_per_group=2)

    job, cursor = _make_job(file_path, rows_copied=6)
    job._job_client.config.bulk_copy_timeout = 900
    job.run()

    kwargs = _bulkcopy_kwargs(cursor)
    assert kwargs["batch_size"] == 0
    assert kwargs["use_internal_transaction"] is True
    # column defaults must never replace a NULL that dlt wrote
    assert kwargs["keep_nulls"] is True
    assert kwargs["timeout"] == 900


def test_bulk_copy_skips_an_empty_file(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=0, rows_per_group=0)

    job, cursor = _make_job(file_path)
    job.run()

    assert cursor.bulkcopy_arrow.call_count == 0


def test_bulk_copy_fails_when_fewer_rows_land_than_the_file_holds(tmp_path) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=2, rows_per_group=5)

    job, cursor = _make_job(file_path, rows_copied=7)

    with pytest.raises(DestinationTerminalException, match="7 rows copied but the file holds 10"):
        job.run()
    assert cursor.bulkcopy_arrow.call_count == 1


@pytest.mark.parametrize(
    "credential_kwargs,expected",
    [
        ({"access_token": "a-token"}, "access_token"),
        ({"authentication": "ActiveDirectoryPassword"}, "ActiveDirectoryPassword"),
        ({"authentication": "ActiveDirectoryIntegrated"}, "ActiveDirectoryIntegrated"),
    ],
    ids=["access_token", "ad_password", "ad_integrated"],
)
def test_bulk_copy_rejects_credentials_it_cannot_sign_in_with(
    tmp_path, credential_kwargs: Dict[str, str], expected: str
) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=1, rows_per_group=1)

    job, cursor = _make_job(file_path, rows_copied=1, **credential_kwargs)

    with pytest.raises(DestinationTerminalException, match=expected):
        job.run()
    assert cursor.bulkcopy_arrow.call_count == 0


@pytest.mark.parametrize(
    "raised",
    [
        ValueError("String or binary data would be truncated"),
        RuntimeError("Sql Error: 2601: Class 14: Cannot insert duplicate key row"),
        TimeoutError("bulk copy timed out"),
    ],
    ids=["client_value_error", "server_runtime_error", "timeout"],
)
def test_bulk_copy_failures_propagate_so_dlt_retries_them(tmp_path, raised: Exception) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=1, rows_per_group=3)

    job, cursor = _make_job(file_path, rows_copied=3)
    cursor.bulkcopy_arrow.side_effect = raised

    with pytest.raises(type(raised)) as exc_info:
        job.run()

    # a retry is safe because the copy is a single transactional batch
    assert not isinstance(exc_info.value, TerminalException)
    assert cursor.close.call_count == 1


def test_bulk_copy_opens_a_mssql_python_connection_under_pyodbc(tmp_path, monkeypatch) -> None:
    file_path = str(tmp_path / "items.1234.0.parquet")
    _write_parquet(file_path, ["id"], num_row_groups=1, rows_per_group=3)

    job_client = _make_job_client(connector="pyodbc")
    connection, cursor = _mock_connection(rows_copied=3)
    opened_with: List[Any] = []

    def _open_connection(self: MsSqlPythonClient) -> MagicMock:
        opened_with.append(self.credentials)
        self._conn = connection
        return connection

    monkeypatch.setattr(MsSqlPythonClient, "open_connection", _open_connection)
    monkeypatch.setattr(MsSqlPythonClient, "close_connection", lambda self: None)

    job = MsSqlBulkCopyArrowJob(file_path)
    job._job_client = job_client
    job._load_table = {"name": "items"}
    job.run()

    assert cursor.bulkcopy_arrow.call_args.args[0] == '"my_dataset"."items"'
    assert [c.connector for c in opened_with] == ["mssql-python"]
    # the loader keeps its own pyodbc credentials
    assert job_client.config.credentials.connector == "pyodbc"
    assert "DRIVER" not in opened_with[0].to_odbc_dsn()


@pytest.mark.parametrize(
    "connector,bulk_copy,adbc,expected_job",
    [
        ("mssql-python", True, True, MsSqlBulkCopyArrowJob),
        ("mssql-python", True, False, MsSqlBulkCopyArrowJob),
        ("mssql-python", False, True, MssqlParquetCopyJob),
        ("pyodbc", True, True, MssqlParquetCopyJob),
        ("pyodbc", True, False, MsSqlBulkCopyArrowJob),
        ("pyodbc", False, True, MssqlParquetCopyJob),
    ],
    ids=[
        "mssql-python-both-installed",
        "mssql-python-bulk-copy-only",
        "mssql-python-adbc-only",
        "pyodbc-both-installed",
        "pyodbc-bulk-copy-only",
        "pyodbc-adbc-only",
    ],
)
def test_parquet_load_job_follows_connector_and_installed_drivers(
    monkeypatch, connector: str, bulk_copy: bool, adbc: bool, expected_job: type
) -> None:
    monkeypatch.setattr(
        "dlt.destinations.impl.mssql.mssql.has_native_arrow_bulk_copy",
        lambda: (bulk_copy, None),
    )
    monkeypatch.setattr(
        "dlt.destinations.impl.mssql.mssql.has_adbc_driver", lambda driver: (adbc, None)
    )
    job_client = _make_job_client(connector=connector)

    job = job_client._create_parquet_load_job(os.path.join("new_jobs", "items.1234.0.parquet"))

    assert type(job) is expected_job


def test_native_arrow_bulk_copy_is_detected() -> None:
    assert has_native_arrow_bulk_copy() == (True, None)


@pytest.mark.parametrize(
    "adbc,bulk_copy,expected_preferred,parquet_supported",
    [
        (True, True, "parquet", True),
        (True, False, "parquet", True),
        (False, True, "insert_values", True),
        (False, False, "insert_values", False),
    ],
    ids=["adbc-and-bulk-copy", "adbc", "bulk-copy", "none"],
)
def test_file_format_selector(
    monkeypatch,
    adbc: bool,
    bulk_copy: bool,
    expected_preferred: str,
    parquet_supported: bool,
) -> None:
    monkeypatch.setattr(
        "dlt.destinations.impl.mssql.bulk_copy.has_adbc_driver",
        lambda driver: (adbc, None if adbc else "no adbc"),
    )
    monkeypatch.setattr(
        "dlt.destinations.impl.mssql.bulk_copy.has_native_arrow_bulk_copy",
        lambda: (bulk_copy, None if bulk_copy else "no mssql-python"),
    )
    selector = make_parquet_file_format_selector(DOCS_URL)
    table_schema: TTableSchema = {"name": "items", "file_format": "parquet"}

    preferred, supported = selector(
        "insert_values", ["insert_values", "parquet", "model"], table_schema=table_schema
    )

    assert preferred == expected_preferred
    assert ("parquet" in supported) is parquet_supported
