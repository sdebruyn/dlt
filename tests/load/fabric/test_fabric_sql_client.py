from typing import Type

import pytest

from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)

mssql_python = pytest.importorskip("mssql_python")

from dlt.destinations.impl.fabric.sql_client import FabricSqlClient


@pytest.mark.parametrize(
    "error_class,driver_error,message,expected",
    [
        (
            "ProgrammingError",
            "Base table or view not found",
            "Invalid object name 'ds.missing'. (208)",
            DatabaseUndefinedRelation,
        ),
        (
            "ProgrammingError",
            "Syntax error or access violation",
            "Cannot find the object 'ds.missing' because it does not exist. (15151)",
            DatabaseUndefinedRelation,
        ),
        (
            "ProgrammingError",
            "Syntax error or access violation",
            "Incorrect syntax near 'SELECTA'. (102)",
            DatabaseTransientException,
        ),
        (
            "ProgrammingError",
            "Syntax error or access violation",
            "Snapshot isolation transaction failed in database 'dlt_test'. (24556)",
            DatabaseTransientException,
        ),
        (
            "ProgrammingError",
            "COUNT field incorrect",
            "COUNT field incorrect or syntax error",
            DatabaseTransientException,
        ),
        (
            "ProgrammingError",
            "Column not found",
            "Invalid column name 'column_xxx'. (207)",
            DatabaseTerminalException,
        ),
        ("OperationalError", "Connection failure", "Login timeout", DatabaseTransientException),
        (
            "IntegrityError",
            "Integrity constraint violation",
            "Cannot insert the value NULL. (515)",
            DatabaseTerminalException,
        ),
    ],
    ids=[
        "missing-table",
        "missing-object",
        "syntax-error",
        "snapshot-conflict",
        "parameter-count",
        "missing-column",
        "connection-failure",
        "null-violation",
    ],
)
def test_database_exception_classification(
    error_class: str, driver_error: str, message: str, expected: Type[Exception]
) -> None:
    error = getattr(mssql_python, error_class)(driver_error, message)

    mapped = FabricSqlClient._make_database_exception(error)

    assert type(mapped) is expected
    assert FabricSqlClient.is_dbapi_exception(error)
