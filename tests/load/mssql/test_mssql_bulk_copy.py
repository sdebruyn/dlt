from typing import Any, Iterator

import pytest

import dlt
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.utils import uniq_id

from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import assert_load_info, load_table_counts

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["mssql"]),
    ids=lambda x: x.name,
)
def test_bulk_copy_loads_parquet(destination_config: DestinationTestConfiguration) -> None:
    pytest.importorskip("mssql_python")
    pipeline = destination_config.setup_pipeline("mssql_bulk_copy_" + uniq_id(), dev_mode=True)

    info = pipeline.run(
        [{"id": i, "name": f"n{i}", "maybe": None if i % 2 else i} for i in range(1000)],
        table_name="items",
        loader_file_format="parquet",
    )
    assert_load_info(info)
    assert load_table_counts(pipeline)["items"] == 1000
    with pipeline.sql_client() as client:
        nulls = client.execute_sql(
            f"SELECT COUNT(*) FROM {client.make_qualified_table_name('items')} WHERE maybe IS NULL"
        )[0][0]
    assert nulls == 500

    # a new column lands last in the table but keeps its position in the next load file
    info = pipeline.run(
        [{"id": i, "added": f"a{i}", "name": f"n{i}"} for i in range(1000, 1500)],
        table_name="items",
        loader_file_format="parquet",
    )
    assert_load_info(info)
    assert load_table_counts(pipeline)["items"] == 1500
    with pipeline.sql_client() as client:
        rows = client.execute_sql(
            f"SELECT name, added FROM {client.make_qualified_table_name('items')} WHERE id = 1200"
        )
    assert [tuple(row) for row in rows] == [("n1200", "a1200")]


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["mssql"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("write_disposition", ["replace", "merge"])
def test_bulk_copy_staged_write_dispositions(
    destination_config: DestinationTestConfiguration, write_disposition: TWriteDisposition
) -> None:
    pytest.importorskip("mssql_python")
    pipeline = destination_config.setup_pipeline(
        "mssql_bulk_copy_staged_" + uniq_id(), dev_mode=True
    )

    @dlt.resource(name="items", primary_key="id", write_disposition=write_disposition)
    def items(offset: int) -> Iterator[Any]:
        yield from ({"id": i, "name": f"n{i}"} for i in range(offset, offset + 100))

    assert_load_info(pipeline.run(items(0), loader_file_format="parquet"))
    assert load_table_counts(pipeline)["items"] == 100

    assert_load_info(pipeline.run(items(50), loader_file_format="parquet"))
    expected = 100 if write_disposition == "replace" else 150
    assert load_table_counts(pipeline)["items"] == expected
