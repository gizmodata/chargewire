// What a read-only session (spec §5.8) may run on DuckDB, as lists: every decision about
// what read-only allows is one list here, with its tests in tests/duckdb_read_only_test.cc
// and conformance/tests/test_read_only*.py.
//
// The rule the adapter is built on: what is checked is exactly what runs, and nothing of a
// statement executes before its check has passed. DuckDB plans a statement once, and hands
// the plan it is about to execute to the session's guard (duckdb_read_only_guard.cc, the one
// file of this adapter that uses DuckDB's internal headers: the v2 C API has no way to look at
// a bound statement, nor to execute one that was looked at). The guard refuses the plan or
// lets that same plan run; the statement is never planned a second time, so there is no second
// plan that could differ from the checked one. The same holds for every statement DuckDB
// makes out of the client's (the parts of a statement it expands into several) and for the
// executions of a prepared statement, which a read-only session plans, checks and runs anew
// each time.
//
// A read-only session is held by three things:
//   1. every transaction of the session is one the adapter starts with BEGIN TRANSACTION
//      READ ONLY, and the client's transaction statements are run by the adapter, never by
//      DuckDB;
//   2. only the kinds of statement listed here are handed to DuckDB at all (its parser names
//      the kind, before anything is bound);
//   3. the guard: a plan runs only when it modifies no catalog (the temporary one included)
//      and every operator, table function, setting and pragma in it is listed here.
// Everything is an allow-list: what is not listed is refused with read_only, so a statement
// kind, an operator or a function that a later DuckDB or an extension adds is refused until
// someone has decided that it only reads.
//
// What DuckDB does before the guard sees a plan is binding: it resolves names, expands views
// and macros, evaluates the arguments of table functions, the value of a setting and the
// parameters of a pragma (calling any function in them), and runs the bind step of each table
// function. No hook of DuckDB's comes before that, and what a function did there is done when
// the plan is refused (a sequence advanced or set stays so). So before DuckDB binds a
// statement of a read-only session, the parser's tree is screened (ScreenBeforeBinding): what
// binding would evaluate (those, and the version or time a table is read AT) may hold values only, the table functions that take SQL text or a
// table's name are refused by name, and the same holds for the definition of every view and
// macro the statement reaches (DuckDBSession::ScreenReadOnly). A statement that DuckDB expands
// by running a query first (a PIVOT whose values come from the data) is refused by its kind.
// What is left of binding is the bind step of a table function called with plain values:
// those of DuckDB and of the tested extensions change nothing (the tests assert that a
// refused function left nothing changed); one of an extension that does would have done it
// before the guard refuses the function.
//
// A read-only session's transactions are ended by a rollback, never a commit: a reader has
// nothing to commit, and DuckDB treats the commit of a READ ONLY transaction in which
// something was changed after all as fatal for the whole database.
//
// What this does not see: what a scalar function does when the plan runs. DuckDB's own that
// change a catalog (advancing a sequence, a temporary one too) are refused, because the plan
// says which catalogs it modifies; a scalar function that writes somewhere else (DuckDB's own
// log function, a function of an extension that an administrator installed) is not seen
// here. Read-only is not a sandbox (spec §5.8).
#pragma once

#include <memory>
#include <optional>
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

  // DuckDB's physical operators that read, by the name DuckDB gives them (upper case).
  Names plan_operators;

  // Table functions that only read (upper case: READ_PARQUET), whether a plan scans them or
  // calls them once per input row. A server that embeds this adapter adds the ones it knows
  // to read (DuckDBSession::set_read_only_policy, before the session becomes read-only).
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

// The guard of one read-only session: DuckDB shows it every plan of the session's connection
// right before it executes that plan, and the guard refuses the plan (the statement then
// fails, nothing of it having run) or lets it run. Installed once, for the connection's life.
class ReadOnlyGuard {
 public:
  virtual ~ReadOnlyGuard() = default;

  // Why the guard refused the statement that just failed, once: the detail of its read_only
  // error, in the guard's own words (a kind or a name, never the statement).
  virtual std::optional<std::string> TakeRefusal() = 0;

  // Around a statement of the adapter's own that is not the client's to send: the READ ONLY
  // transaction's BEGIN, COMMIT and ROLLBACK.
  class OwnStatement {
   public:
    explicit OwnStatement(ReadOnlyGuard& guard) : guard_(guard) { guard_.own_statements_++; }
    ~OwnStatement() { guard_.own_statements_--; }
    OwnStatement(const OwnStatement&) = delete;
    OwnStatement& operator=(const OwnStatement&) = delete;

   private:
    ReadOnlyGuard& guard_;
  };

 protected:
  int own_statements_ = 0;  // the session's engine thread only
};

// A parsed SET or RESET that names no scope is given the session's: DuckDB applies such a
// statement to every session for some settings and to the session for others, and a
// read-only session sets nothing for anyone else. The parser's own statement is changed,
// before anything is bound; no text is rewritten. Other statements are left alone.
void ScopeSettingToSession(duckdb_v2_sql_statement_handle statement);

// Refuses, before DuckDB binds anything of it, a statement with something that binding
// itself would run and that is not a plain value: DuckDB evaluates the arguments of table
// functions, the value of a setting and the parameters of a pragma while it binds, before a
// plan exists for the guard to refuse, and calls any function in them. Also refused there:
// the table functions that take SQL text or a table's name as their argument, since what they
// make DuckDB bind is not in the statement. Throws read_only. Returns the names the
// statement refers to (lower case), because DuckDB binds the definitions of the views and
// macros among them with the statement: the adapter screens those definitions the same way
// (a view's CREATE VIEW statement is accepted here for that).
struct ReadOnlyReferences {
  std::set<std::string> functions;  // every function called, table functions and macros among them
  std::set<std::string> relations;  // every table or view named
  std::set<std::string> evaluated;  // the functions called where DuckDB evaluates while binding
  // Strings in what DuckDB evaluates while binding: each can name a table or view that DuckDB
  // then binds (a function that describes a table by its name), as written and without its
  // qualification. `computed_names`: such a string is not spelled out in the statement (a
  // bind parameter, a concatenation, a variable), so the statement can name anything.
  std::set<std::string> named;
  bool computed_names = false;
};
ReadOnlyReferences ScreenBeforeBinding(duckdb_v2_sql_statement_handle statement);

// What changes whenever a view or macro is created, replaced or dropped in any DuckDB catalog
// the connection sees, as its open transaction sees it: the catalogs with their versions.
// Empty: DuckDB gives no version now, and nothing may be reused. For the adapter's copy of the
// views' and macros' definitions, which it would otherwise read again for every statement.
std::string ReadOnlyCatalogStamp(duckdb_v2_connection_handle connection);

// Installs the guard on `connection`. From then on every plan of the connection is held to
// `policy`. There is no way to remove it.
std::shared_ptr<ReadOnlyGuard> InstallReadOnlyGuard(duckdb_v2_connection_handle connection,
                                                    std::shared_ptr<const ReadOnlyPolicy> policy);

}  // namespace chargewire_duckdb
