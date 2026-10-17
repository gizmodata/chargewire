"""The values the spike pushes through nanoarrow: one case per type, each with edge
values and a NULL."""

import datetime
import decimal

import pyarrow as pa

# DuckDB types, as SQL. Each case is a list of SQL expressions, one per row; a NULL
# row is added to every case.
DUCKDB_CASES: dict[str, list[str]] = {
    "boolean": ["true", "false"],
    "tinyint": ["(-128)::TINYINT", "127::TINYINT"],
    "smallint": ["(-32768)::SMALLINT", "32767::SMALLINT"],
    "integer": ["(-2147483648)::INTEGER", "2147483647::INTEGER"],
    "bigint": ["(-9223372036854775808)::BIGINT", "9223372036854775807::BIGINT"],
    "hugeint": [
        "(-170141183460469231731687303715884105728)::HUGEINT",
        "170141183460469231731687303715884105727::HUGEINT",
    ],
    "utinyint": ["0::UTINYINT", "255::UTINYINT"],
    "usmallint": ["0::USMALLINT", "65535::USMALLINT"],
    "uinteger": ["0::UINTEGER", "4294967295::UINTEGER"],
    "ubigint": ["0::UBIGINT", "18446744073709551615::UBIGINT"],
    "uhugeint": ["0::UHUGEINT", "340282366920938463463374607431768211455::UHUGEINT"],
    "float": ["1.5::FLOAT", "(-0.0)::FLOAT", "'inf'::FLOAT", "'-inf'::FLOAT"],
    "double": ["1e308::DOUBLE", "(-0.0)::DOUBLE", "'inf'::DOUBLE", "5e-324::DOUBLE"],
    "decimal_4_2": ["(-99.99)::DECIMAL(4,2)", "99.99::DECIMAL(4,2)"],
    "decimal_18_6": ["(-999999999999.999999)::DECIMAL(18,6)", "0.000001::DECIMAL(18,6)"],
    "decimal_38_10": [
        "(-9999999999999999999999999999.9999999999)::DECIMAL(38,10)",
        "0.0000000001::DECIMAL(38,10)",
    ],
    "varchar": ["''", "'héllo wörld'", "'😀 emoji'", "repeat('x', 100000)"],
    "blob": ["'\\x00\\xFF\\x10'::BLOB", "''::BLOB"],
    "date": ["'0001-01-01'::DATE", "'9999-12-31'::DATE", "'1970-01-01'::DATE"],
    "date_infinity": ["'infinity'::DATE", "'-infinity'::DATE"],
    "time": ["'00:00:00'::TIME", "'23:59:59.999999'::TIME"],
    "time_ns": ["'12:34:56.123456789'::TIME_NS"],
    "timetz": ["'12:34:56+05:30'::TIMETZ", "'00:00:00-08:00'::TIMETZ"],
    "timestamp_s": ["'2026-10-16 12:34:56'::TIMESTAMP_S"],
    "timestamp_ms": ["'2026-10-16 12:34:56.789'::TIMESTAMP_MS"],
    "timestamp": ["'2026-10-16 12:34:56.123456'::TIMESTAMP", "'0001-01-01 00:00:00'::TIMESTAMP"],
    "timestamp_ns": ["'2026-10-16 12:34:56.123456789'::TIMESTAMP_NS"],
    "timestamp_infinity": ["'infinity'::TIMESTAMP", "'-infinity'::TIMESTAMP"],
    "timestamptz": ["'2026-10-16 12:34:56.123456+02'::TIMESTAMPTZ"],
    "timestamptz_ns": ["'2026-10-16 12:34:56.123456789+02'::TIMESTAMPTZ_NS"],
    "interval": [
        "INTERVAL '1 year 2 months 3 days 04:05:06.789012'",
        "INTERVAL '-5 days'",
        "INTERVAL '36 hours'",
    ],
    "uuid": ["'00000000-0000-0000-0000-000000000000'::UUID",
             "'7d444840-9dc0-11d1-b245-5ffdce74fad2'::UUID"],
    "enum": ["'b'::ENUM('a', 'b', 'c')", "'a'::ENUM('a', 'b', 'c')"],
    "list": ["[1, NULL, 3]", "[]::INTEGER[]"],
    "array": ["[1, 2, 3]::INTEGER[3]", "[NULL, 5, 6]::INTEGER[3]"],
    "struct": ["{'a': 1, 'b': 'x'}", "{'a': NULL, 'b': NULL}"],
    "map": ["MAP {'k1': 1, 'k2': NULL}", "MAP {}::MAP(VARCHAR, INTEGER)"],
    "union": [
        "union_value(num := 2)::UNION(num INTEGER, str VARCHAR)",
        "union_value(str := 'two')::UNION(num INTEGER, str VARCHAR)",
    ],
    "bit": ["'1010'::BIT", "'1'::BIT"],
    "bignum": ["'1234567890123456789012345678901234567890'::BIGNUM", "'-1'::BIGNUM"],
    "json": ["'{\"a\": 1}'::JSON", "'[1, 2]'::JSON"],
    "variant": ["42::VARIANT", "'text'::VARIANT", "[1, 2]::VARIANT"],
    "geometry": ["'POINT(1 2)'::GEOMETRY", "'LINESTRING(0 0, 1 1)'::GEOMETRY"],
    "nested": [
        "[{'id': 1, 'tags': ['a', 'b']}, {'id': 2, 'tags': []}]",
        "[{'id': NULL, 'tags': NULL}]",
    ],
    "map_of_lists": ["MAP {'x': [1, 2], 'y': NULL}"],
}

# Settings a server may choose between. "lossless" asks DuckDB for Arrow extension
# types (UUID, JSON, a TIMETZ that keeps its offset, …); "views" adds the newest
# Arrow layouts on top (binary views, list views).
ARROW_SETTINGS: dict[str, str] = {
    "default": "",
    "lossless": "SET arrow_lossless_conversion = true;",
    "views": "SET arrow_lossless_conversion = true; SET arrow_output_version = '1.5'; "
    "SET arrow_output_list_view = true",
}


def duckdb_case_sql(name: str) -> str:
    """The query for one DuckDB case: its values plus a NULL, in a column named v."""
    values = ", ".join(f"({row})" for row in DUCKDB_CASES[name])
    return f"SELECT v FROM (VALUES {values}, (NULL)) t(v)"


def _days(n: int) -> datetime.date:
    return datetime.date(1970, 1, 1) + datetime.timedelta(days=n)


# Arrow types a client may upload, as pyarrow arrays (each with a null).
ARROW_CASES: dict[str, pa.Array] = {
    "bool": pa.array([True, False, None]),
    "int8": pa.array([-128, 127, None], type=pa.int8()),
    "int16": pa.array([-32768, 32767, None], type=pa.int16()),
    "int32": pa.array([-(2**31), 2**31 - 1, None], type=pa.int32()),
    "int64": pa.array([-(2**63), 2**63 - 1, None], type=pa.int64()),
    "uint8": pa.array([0, 255, None], type=pa.uint8()),
    "uint16": pa.array([0, 65535, None], type=pa.uint16()),
    "uint32": pa.array([0, 2**32 - 1, None], type=pa.uint32()),
    "uint64": pa.array([0, 2**64 - 1, None], type=pa.uint64()),
    "float32": pa.array([1.5, float("inf"), None], type=pa.float32()),
    "float64": pa.array([1e308, -0.0, None], type=pa.float64()),
    "decimal32": pa.array([decimal.Decimal("-99.99"), decimal.Decimal("1.23"), None],
                          type=pa.decimal32(4, 2)),
    "decimal64": pa.array([decimal.Decimal("123456789.123456"), None, None],
                          type=pa.decimal64(15, 6)),
    "decimal128": pa.array([decimal.Decimal("-9999999999999999999999999999.9999999999"), None, None],
                           type=pa.decimal128(38, 10)),
    "decimal256": pa.array([decimal.Decimal("1" * 60 + ".5"), None, None],
                           type=pa.decimal256(76, 10)),
    "string": pa.array(["", "héllo", None], type=pa.string()),
    "large_string": pa.array(["a", "b" * 1000, None], type=pa.large_string()),
    "string_view": pa.array(["short", "a longer string than twelve bytes", None],
                            type=pa.string_view()),
    "binary": pa.array([b"\x00\xff", b"", None], type=pa.binary()),
    "large_binary": pa.array([b"\x01", None, b""], type=pa.large_binary()),
    "binary_view": pa.array([b"x", b"y" * 40, None], type=pa.binary_view()),
    "fixed_size_binary": pa.array([b"abc", b"xyz", None], type=pa.binary(3)),
    "date32": pa.array([_days(-719162), _days(2932896), None], type=pa.date32()),
    "date64": pa.array([datetime.date(2026, 10, 16), None, None], type=pa.date64()),
    "time32_s": pa.array([datetime.time(23, 59, 59), None, None], type=pa.time32("s")),
    "time32_ms": pa.array([datetime.time(12, 0, 0, 123000), None, None], type=pa.time32("ms")),
    "time64_us": pa.array([datetime.time(12, 0, 0, 123456), None, None], type=pa.time64("us")),
    "time64_ns": pa.array([45296123456789, None, None], type=pa.time64("ns")),
    "timestamp_s": pa.array([1_700_000_000, None, None], type=pa.timestamp("s")),
    "timestamp_ms": pa.array([1_700_000_000_123, None, None], type=pa.timestamp("ms")),
    "timestamp_us": pa.array([1_700_000_000_123_456, None, None], type=pa.timestamp("us")),
    "timestamp_ns": pa.array([1_700_000_000_123_456_789, None, None], type=pa.timestamp("ns")),
    "timestamp_us_utc": pa.array([1_700_000_000_123_456, None, None],
                                 type=pa.timestamp("us", tz="UTC")),
    "timestamp_ns_tz": pa.array([1_700_000_000_123_456_789, None, None],
                                type=pa.timestamp("ns", tz="Europe/Berlin")),
    "duration_s": pa.array([3600, -1, None], type=pa.duration("s")),
    "duration_us": pa.array([1_500_000, None, None], type=pa.duration("us")),
    "duration_ns": pa.array([1_500_000_123, None, None], type=pa.duration("ns")),
    "month_day_nano": pa.array([pa.MonthDayNano([14, 3, 4_000_000_123]), None, None],
                               type=pa.month_day_nano_interval()),
    "list": pa.array([[1, None, 3], [], None], type=pa.list_(pa.int32())),
    "large_list": pa.array([[1, 2], None, []], type=pa.large_list(pa.int64())),
    "fixed_size_list": pa.array([[1, 2], [None, 4], None], type=pa.list_(pa.int16(), 2)),
    "list_view": pa.array([[1, 2], [], None], type=pa.list_view(pa.int32())),
    "struct": pa.array([{"a": 1, "b": "x"}, {"a": None, "b": None}, None],
                       type=pa.struct([("a", pa.int32()), ("b", pa.string())])),
    "map": pa.array([[("k", 1), ("j", None)], [], None], type=pa.map_(pa.string(), pa.int32())),
    "dense_union": pa.UnionArray.from_dense(
        types=pa.array([0, 1, 0], type=pa.int8()),
        value_offsets=pa.array([0, 0, 1], type=pa.int32()),
        children=[pa.array([1, None], type=pa.int32()), pa.array(["two"])],
        field_names=["num", "str"]),
    "sparse_union": pa.UnionArray.from_sparse(
        types=pa.array([0, 1, 0], type=pa.int8()),
        children=[pa.array([1, None, 3], type=pa.int32()), pa.array(["a", "two", "c"])],
        field_names=["num", "str"]),
    "dictionary": pa.array(["x", "y", None, "x"]).dictionary_encode(),
    "run_end_encoded": pa.RunEndEncodedArray.from_arrays(
        run_ends=pa.array([2, 3], type=pa.int32()), values=pa.array(["r", None])),
    "uuid": pa.array([b"\x00" * 16, b"\x12" * 16, None], type=pa.uuid()),
    "json": pa.array(['{"a": 1}', "[1]", None], type=pa.json_()),
    "null": pa.array([None, None, None], type=pa.null()),
}
