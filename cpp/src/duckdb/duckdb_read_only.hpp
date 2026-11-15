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
// and macros, evaluates the arguments of table functions, the value of a setting, the
// parameters of a pragma, the file name and the options of a COPY and the version or time a
// table is read AT (calling any function in them), and runs the bind step of each table
// function. No hook of DuckDB's comes before that, and what a function did there is done when
// the plan is refused (a sequence advanced or set stays so). So before DuckDB binds a
// statement of a read-only session, the parser's tree is screened (ScreenBeforeBinding), and
// the screen is an allow-list like everything else here:
//   - it walks the whole statement itself, every kind of statement, query, table reference
//     and expression by name, and refuses a kind it does not know how to walk. What stands
//     under an EXPLAIN is walked as the statement it is: a query, a COPY to a file, a CALL or
//     a setting. Every other kind is refused there, the ones that change a catalog (INSERT,
//     UPDATE, DELETE, MERGE, also as the body of a CTE) and the ones a read-only session has
//     no use for (PREPARE, EXECUTE, ATTACH, ...): nothing of them is bound;
//   - what binding would evaluate may hold values only;
//   - a table function is bound only when it is listed (table_functions,
//     explained_table_functions) or is a table macro of a user's that the adapter has read
//     the definition of: the bind step of any other never runs. That is what keeps out the
//     functions that bind or run SQL the statement does not hold (query, query_table,
//     json_execute_serialized_sql, the pass-through functions of other systems), which are
//     refused by name besides;
//   - a PRAGMA is bound only when it is listed (pragmas): DuckDB replaces some pragmas by
//     the text of a statement that it then binds;
//   - json_serialize_plan, a scalar function that binds SQL it is given as text, is refused
//     by name;
//   - the definitions of the views and macros a statement names, and of those these name,
//     are screened the same way (DuckDBSession::ScreenReadOnly).
// What the screen does not reach is a definition that DuckDB binds and the statement does
// not lead to by a name the adapter has read: a view or macro the session's listing of
// definitions does not show (hidden from the user by a server's access control, made or
// granted after the adapter last read them where the catalog has no version to tell), a view
// that a catalog function binds when it runs (CREATE VIEW ... WITH (DEFER_BINDING), read
// through duckdb_columns()), a view named inside a string in a way the adapter does not
// take apart, a macro's argument that its definition passes to a table function, and a
// macro of a user's that DuckDB's own text binds in place of a function of the same name.
// A read-only session can still advance a sequence through those.
// A statement that DuckDB expands by running a query first (a PIVOT whose values come from
// the data) is refused by its kind.
// What is left of binding is the bind step of a listed table function called with plain
// values, of a function DuckDB itself puts in place of a file's name, and of a COPY's format:
// those of DuckDB and of the tested extensions change nothing (the tests assert that a
// refused function left nothing changed).
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

  // Table functions that change something when they run and nothing when DuckDB binds them
  // (upper case: CHECKPOINT). A statement that names one may be bound, so that a plain
  // EXPLAIN shows its plan; the guard still refuses every plan that would run one. A table
  // function on neither list is refused before DuckDB binds it.
  Names explained_table_functions;

  // The lists this adapter ships.
  static ReadOnlyPolicy Default();
};

// "SELECT", "CREATE", ...: DuckDB's name for a statement kind, for error details.
std::string_view StatementKindName(DUCKDB_V2_STATEMENT_TYPE kind);

// The read_only error (spec §11) for something a read-only session may not run. `what` is
// a kind or a name the server chose ("COPY", "the table function X"), never the statement.
chargewire::Error ReadOnlyRefusal(std::string_view what);

// The read_only error for an operator or a table function that is on no list, by its name.
chargewire::Error ReadOnlyNotListed(std::string_view name);

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

// Refuses, before DuckDB binds anything of it, a statement that binding itself would do
// something with (the rules are at the top of this file). Throws read_only. Returns the
// names the statement refers to (lower case), because DuckDB binds the definitions of the
// views and macros among them with the statement: the adapter screens those definitions the
// same way.
struct ReadOnlyReferences {
  std::set<std::string> functions;  // every function called, table functions and macros among them
  std::set<std::string> relations;  // every table or view named
  std::set<std::string> evaluated;  // the functions called where DuckDB evaluates while binding
  // Names in what DuckDB evaluates while binding: each can name a table or view that DuckDB
  // then binds (a function that describes a table by its name), as written and without its
  // qualification. `computed_names`: such a string is not spelled out in the statement (a
  // bind parameter, a concatenation, a variable), so the statement can name anything.
  std::set<std::string> named;
  bool computed_names = false;
  // The table functions called that are on neither list of the policy. One is bound only
  // if it is a table macro whose definition the adapter has read (it then is in `functions`
  // too, and followed): the adapter refuses the statement otherwise, before DuckDB binds it.
  std::set<std::string> unlisted_table_functions;
};

// What the screen holds a statement to, besides its own rules.
struct ReadOnlyScreen {
  const ReadOnlyPolicy* policy = nullptr;
};

// What is screened: a statement of the client's, or the definition of a view (its CREATE
// VIEW statement) or of a macro (a query), which holds nothing but a query.
enum class ReadOnlyScreened { kStatement, kDefinition };

ReadOnlyReferences ScreenBeforeBinding(duckdb_v2_sql_statement_handle statement, const ReadOnlyScreen& screen,
                                       ReadOnlyScreened what = ReadOnlyScreened::kStatement);

// Whether DuckDB knows `name` (lower case) as a table function or pragma of an extension it
// loads on demand: a table macro of that name need not be what a statement binds.
bool ReadOnlyExtensionFunction(std::string_view name);

// What DuckDB itself holds for a connection, whatever the adapter believes: whether a
// transaction is open, and whether it has changed anything in any database (rows, the
// catalog, a sequence's value, temporary objects too) or been told it will.
struct ConnectionTransaction {
  bool active = false;
  bool changes = false;
};
ConnectionTransaction TransactionOf(duckdb_v2_connection_handle connection);

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
