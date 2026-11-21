"""Detection of the parquet load paths, kept out of `mssql.py` so the factory does not import a driver."""

import functools
from typing import Optional, Sequence, Tuple

from dlt.common import logger
from dlt.common.destination.capabilities import LoaderFileFormatSelector
from dlt.common.exceptions import MissingDependencyException
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TLoaderFileFormat

from dlt.destinations._adbc_jobs import has_adbc_driver


def has_native_arrow_bulk_copy() -> Tuple[bool, Optional[str]]:
    """Checks whether parquet load files can be streamed into mssql-python's Arrow bulk copy.

    Returns:
        Tuple[bool, Optional[str]]: whether the bulk copy is available, and why it is not.
    """
    try:
        from dlt.common.libs.pyarrow import pyarrow  # noqa: F401
    except MissingDependencyException as dep_ex:
        return False, str(dep_ex)

    try:
        from mssql_python import Cursor
    except ImportError as import_ex:
        return False, str(import_ex)

    if not hasattr(Cursor, "bulkcopy_arrow"):
        return False, "the installed mssql-python has no `Cursor.bulkcopy_arrow`, added in 1.13.0"
    return True, None


def _loader_file_format_selector(
    docs_url: str,
    preferred_loader_file_format: TLoaderFileFormat,
    supported_loader_file_formats: Sequence[TLoaderFileFormat],
    /,
    *,
    table_schema: TTableSchema,
) -> Tuple[TLoaderFileFormat, Sequence[TLoaderFileFormat]]:
    adbc_found, adbc_err = has_adbc_driver("mssql")
    if adbc_found:
        # the ADBC driver loads parquet with either connector
        return "parquet", supported_loader_file_formats

    native_found, native_err = has_native_arrow_bulk_copy()
    if native_found:
        # bulk copy skips triggers and constraint checks that `insert_values` honours, so parquet
        # must be requested explicitly
        return preferred_loader_file_format, supported_loader_file_formats

    supported_loader_file_formats = list(supported_loader_file_formats)
    supported_loader_file_formats.remove("parquet")
    if table_schema.get("file_format") == "parquet":
        logger.warning(
            f"parquet file format was requested for table {table_schema['name']} but neither the"
            " ADBC driver for mssql nor the mssql-python Arrow bulk copy is available:\n"
            f" {adbc_err}\n {native_err}\n Read more: "
            + docs_url
        )
    return preferred_loader_file_format, supported_loader_file_formats


def make_parquet_file_format_selector(docs_url: str) -> LoaderFileFormatSelector:
    """Factory for a file format selector that drops parquet when no parquet load path is installed"""
    return functools.partial(_loader_file_format_selector, docs_url)  # type: ignore[return-value]
