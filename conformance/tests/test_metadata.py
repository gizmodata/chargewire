"""Spec §10: metadata with Arrow Flight SQL's result schemas."""

import io

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from test_discovery_auth import assert_problem
from warp import read_table

TEXT = pa.utf8()
SCHEMAS = {
    "catalogs": pa.schema([pa.field("catalog_name", TEXT, nullable=False)]),
    "db-schemas": pa.schema([pa.field("catalog_name", TEXT), pa.field("db_schema_name", TEXT, nullable=False)]),
    "tables": pa.schema([pa.field("catalog_name", TEXT), pa.field("db_schema_name", TEXT),
                         pa.field("table_name", TEXT, nullable=False), pa.field("table_type", TEXT, nullable=False)]),
    "table-types": pa.schema([pa.field("table_type", TEXT, nullable=False)]),
    "primary-keys": pa.schema([pa.field("catalog_name", TEXT), pa.field("db_schema_name", TEXT),
                               pa.field("table_name", TEXT, nullable=False), pa.field("column_name", TEXT, nullable=False),
                               pa.field("key_name", TEXT), pa.field("key_sequence", pa.int32(), nullable=False)]),
}
KEYS = pa.schema([
    pa.field("pk_catalog_name", TEXT), pa.field("pk_db_schema_name", TEXT),
    pa.field("pk_table_name", TEXT, nullable=False), pa.field("pk_column_name", TEXT, nullable=False),
    pa.field("fk_catalog_name", TEXT), pa.field("fk_db_schema_name", TEXT),
    pa.field("fk_table_name", TEXT, nullable=False), pa.field("fk_column_name", TEXT, nullable=False),
    pa.field("key_sequence", pa.int32(), nullable=False), pa.field("fk_key_name", TEXT),
    pa.field("pk_key_name", TEXT), pa.field("update_rule", pa.uint8(), nullable=False),
    pa.field("delete_rule", pa.uint8(), nullable=False)])


def metadata(warp, session_id: str, kind: str, **params) -> pa.Table:
    response = warp.http.get(url=f"{warp.session_url(session_id)}/metadata/{kind}", params=params,
                             headers=warp.headers())
    assert response.status_code == 200, response.text
    return read_table(content=response.content)


@pytest.fixture
def keyed(warp, session_id):
    warp.execute(session_id=session_id,
                 sql="CREATE TABLE IF NOT EXISTS meta_parent (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
    warp.execute(session_id=session_id,
                 sql="CREATE TABLE IF NOT EXISTS meta_child (x INTEGER, y INTEGER, "
                     "FOREIGN KEY (x, y) REFERENCES meta_parent (a, b))")
    return session_id


@pytest.mark.parametrize("kind", list(SCHEMAS))
def test_metadata_schemas_match_flight_sql(warp, keyed, kind):
    params = {"table": "meta_parent"} if kind == "primary-keys" else {}
    table = metadata(warp=warp, session_id=keyed, kind=kind, **params)
    assert table.schema.equals(SCHEMAS[kind]), table.schema


def test_tables_filters_and_schema(warp, keyed):
    table = metadata(warp=warp, session_id=keyed, kind="tables", table_name_filter_pattern="meta\\_%",
                     include_schema="true")
    assert sorted(table.column("table_name").to_pylist()) == ["meta_child", "meta_parent"]
    assert table.schema.field("table_schema").type == pa.binary()
    schema = ipc.read_schema(pa.py_buffer(table.column("table_schema")[0].as_py()))
    assert schema.names in (["a", "b"], ["x", "y"])
    assert b"ARROW:FLIGHT:SQL:TYPE_NAME" in schema.field(0).metadata


def test_primary_keys_in_order(warp, keyed):
    table = metadata(warp=warp, session_id=keyed, kind="primary-keys", table="meta_parent")
    assert table.select(["column_name", "key_sequence"]).to_pylist() == [
        {"column_name": "a", "key_sequence": 1}, {"column_name": "b", "key_sequence": 2}]


@pytest.mark.parametrize("kind,params", [
    ("imported-keys", {"table": "meta_child"}),
    ("exported-keys", {"table": "meta_parent"}),
    ("cross-reference", {"pk_table": "meta_parent", "fk_table": "meta_child"}),
])
def test_foreign_keys(warp, keyed, kind, params):
    table = metadata(warp=warp, session_id=keyed, kind=kind, **params)
    assert table.schema.equals(KEYS), table.schema
    pairs = table.select(["pk_column_name", "fk_column_name", "key_sequence"]).to_pylist()
    assert pairs == [{"pk_column_name": "a", "fk_column_name": "x", "key_sequence": 1},
                     {"pk_column_name": "b", "fk_column_name": "y", "key_sequence": 2}]


def test_server_info_is_json(warp, session_id):
    response = warp.http.get(url=f"{warp.session_url(session_id)}/metadata/server-info", headers=warp.headers())
    assert response.status_code == 200
    info = response.json()
    assert info["server_name"]
    assert info["sql_identifier_quote_char"] == '"'
    assert isinstance(info["sql_transactions_supported"], bool)


def test_unknown_metadata_kind(warp, session_id):
    response = warp.http.get(url=f"{warp.session_url(session_id)}/metadata/nonsense", headers=warp.headers())
    assert_problem(response=response, status=404, code="not_found")


def test_metadata_filters_are_values_not_sql(warp, keyed):
    table = metadata(warp=warp, session_id=keyed, kind="tables",
                     table_name_filter_pattern="x' OR '1'='1")
    assert table.num_rows == 0


def test_table_schema_reports_nullability_and_decimal_precision(warp, session_id):
    warp.execute(session_id=session_id,
                 sql="CREATE TABLE meta_columns (a INTEGER PRIMARY KEY, b VARCHAR NOT NULL, "
                     "c DECIMAL(10, 3))")
    table = metadata(warp=warp, session_id=session_id, kind="tables",
                     table_name_filter_pattern="meta\\_columns", include_schema="true")
    schema = ipc.read_schema(pa.py_buffer(table.column("table_schema")[0].as_py()))
    assert [(field.name, field.nullable) for field in schema] == [("a", False), ("b", False), ("c", True)]
    c = schema.field("c").metadata
    assert (c[b"ARROW:FLIGHT:SQL:PRECISION"], c[b"ARROW:FLIGHT:SQL:SCALE"]) == (b"10", b"3")


def test_keys_of_every_table_in_one_request(warp, keyed):
    primary = metadata(warp=warp, session_id=keyed, kind="primary-keys")
    assert {"meta_parent"} <= set(primary.column("table_name").to_pylist())
    imported = metadata(warp=warp, session_id=keyed, kind="imported-keys")
    assert {("meta_parent", "meta_child")} <= set(
        zip(imported.column("pk_table_name").to_pylist(), imported.column("fk_table_name").to_pylist()))


def test_table_schema_reports_digits_comments_and_defaults(warp, session_id):
    warp.execute(session_id=session_id,
                 sql="CREATE TABLE meta_described (a INTEGER, b BIGINT, c VARCHAR DEFAULT 'x')")
    warp.execute(session_id=session_id, sql="COMMENT ON COLUMN meta_described.a IS 'the a'")
    table = metadata(warp=warp, session_id=session_id, kind="tables",
                     table_name_filter_pattern="meta\\_described", include_schema="true")
    schema = ipc.read_schema(pa.py_buffer(table.column("table_schema")[0].as_py()))
    a, b, c = (schema.field(name).metadata for name in ("a", "b", "c"))
    # PRECISION is in decimal digits, as JDBC's COLUMN_SIZE (not DuckDB's 32 bits).
    assert (a[b"ARROW:FLIGHT:SQL:PRECISION"], b[b"ARROW:FLIGHT:SQL:PRECISION"]) == (b"10", b"19")
    assert a[b"ARROW:FLIGHT:SQL:REMARKS"] == b"the a"
    assert c[b"WARP:SQL:COLUMN_DEFAULT"] == b"'x'"


def test_type_info_lists_types_in_flight_sql_form(warp, session_id):
    every = metadata(warp=warp, session_id=session_id, kind="type-info")
    assert every.schema.names[:3] == ["type_name", "data_type", "column_size"]
    assert every.schema.field("create_params").type == pa.list_(pa.field("item", pa.string(), nullable=False))
    codes = every.column("data_type").to_pylist()
    assert codes == sorted(codes), "ordered by data_type"
    assert {"integer", "varchar", "boolean"} <= set(every.column("type_name").to_pylist())
    integers = metadata(warp=warp, session_id=session_id, kind="type-info", data_type="4")
    assert set(integers.column("data_type").to_pylist()) == {4}
    assert "integer" in integers.column("type_name").to_pylist()
