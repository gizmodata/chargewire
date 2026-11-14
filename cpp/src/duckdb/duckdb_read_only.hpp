// What a read-only session (spec §5.8) may run on DuckDB, as lists: every decision about
// what read-only allows is one list here, with its tests in tests/duckdb_read_only_test.cc
// and conformance/tests/test_read_only*.py.
//
// DuckDB has no read-only mode for a connection, so a read-only session is held by three
// things together (duckdb_session.cc):
//   1. every transaction of the session is one the adapter starts with BEGIN TRANSACTION
//      READ ONLY, and the client's transaction statements are run by the adapter, never by
//      DuckDB: DuckDB's planner then refuses every statement that modifies a catalog;
//   2. only the kinds of statement listed here run at all (DuckDB's parser names the kind);
//   3. a statement runs only when every operator and table function of its physical plan
//      (DuckDB's planner, EXPLAIN (FORMAT JSON)) is listed here.
// Everything is an allow-list: what is not listed is refused with read_only, so a statement
// kind, an operator or a function that a later DuckDB or an extension adds is refused until
// someone has decided that it only reads.
//
// What this does not see: what a scalar function does. DuckDB's own that change something
// (nextval) are refused by the read-only transaction; a scalar function of an extension
// that an administrator installed and that writes somewhere is not seen here. Read-only is
// not a sandbox (spec §5.8).
#pragma once

#include <functional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "duckdb_v2.hpp"

namespace chargewire_duckdb {

struct ReadOnlyPolicy {
  using Names = std::set<std::string, std::less<>>;

  // The kinds of statement that run at all. Not listed, so refused: every write (INSERT,
  // UPDATE, DELETE, MERGE, CREATE, DROP, ALTER: temporary objects too, since the parser's
  // kind does not tell CREATE TEMP TABLE from CREATE SECRET, nor DROP TABLE from DROP
  // SECRET), COPY and EXPORT (they write files with the server's rights), ATTACH, DETACH,
  // LOAD and INSTALL, CALL (CHECKPOINT is one), VACUUM and ANALYZE, and SQL PREPARE, EXECUTE
  // and DEALLOCATE (the parser says only EXECUTE, not what it runs; prepared statements of
  // the protocol, spec §7, are checked at every execution).
  std::set<DUCKDB_V2_STATEMENT_TYPE> statement_kinds;

  // Settings a session may SET or RESET, lower case. They are applied at session scope
  // whatever scope the statement wrote (DuckDB's plain SET changes some of them for every
  // session), and SET GLOBAL is refused. Not listed: anything that reaches files, the
  // server or other sessions (profiling and query-log output, directories, extensions,
  // memory and threads) and what the plan check relies on (explain_output).
  Names settings;

  // PRAGMAs that only read, lower case.
  Names pragmas;

  // DuckDB's physical operators that read, as EXPLAIN (FORMAT JSON) names them (upper case).
  Names plan_operators;

  // Table functions that only read, as the plan names them (upper case: READ_PARQUET).
  // A server that embeds this adapter adds its own here (set_read_only_policy), or replaces
  // the plan check altogether (DuckDBSession::CheckReadOnlyPlan).
  Names table_functions;

  // The lists this adapter ships.
  static ReadOnlyPolicy Default();
};

// "SELECT", "CREATE", ...: DuckDB's name for a statement kind, for error details.
std::string_view StatementKindName(DUCKDB_V2_STATEMENT_TYPE kind);

// The read_only error (spec §11) for something a read-only session may not run. `what` is
// a kind or a name the server chose ("COPY", "the table function X"), never the statement.
chargewire::Error ReadOnlyRefusal(std::string_view what);

// A statement's tokens from DuckDB's tokenizer, without comments and terminators.
struct SqlToken {
  DUCKDB_V2_TOKEN_TYPE type;
  std::string text;
  std::size_t start;
  std::size_t end;
  // Whether it is this word (a keyword or an unquoted identifier), ignoring case.
  bool Is(std::string_view word) const;
  bool IsOperator(std::string_view op) const { return type == DUCKDB_V2_TOKEN_TYPE_OPERATOR && text == op; }
};
std::vector<SqlToken> SqlTokens(duckdb_v2_connection_handle connection, std::string_view sql);

// The names in a physical plan (the JSON of EXPLAIN (FORMAT JSON)) that `policy` does not
// list: each node's operator and, where the node names one, its function. Empty: the plan
// only reads. Throws read_only for a plan it cannot read.
std::vector<std::string> PlanNamesNotAllowed(std::string_view plan_json, const ReadOnlyPolicy& policy);

}  // namespace chargewire_duckdb
