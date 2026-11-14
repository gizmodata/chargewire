// The guard of a read-only session (spec §5.8; duckdb_read_only.hpp): the one file of this
// adapter that uses DuckDB's internal headers. DuckDB's v2 C API can parse, bind and run a
// statement, but shows nothing of a bound statement and cannot run one that was inspected,
// so a check through it is a second planning, of a plan that need not be the one that runs.
// DuckDB itself hands every plan of a connection to the connection's registered states right
// after it has planned it and before it executes it (ClientContext::CreatePreparedStatement):
// that plan, refused or let through here, is the one that runs.
#include <duckdb.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/execution/operator/helper/physical_pragma.hpp>
#include <duckdb/execution/operator/helper/physical_reset.hpp>
#include <duckdb/execution/operator/helper/physical_set.hpp>
#include <duckdb/execution/operator/scan/physical_table_scan.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/capi_v2/capi_v2_internal.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context_state.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/subquery_expression.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_data/pragma_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/query_node.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/explain_statement.hpp>
#include <duckdb/parser/statement/pragma_statement.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/statement/set_statement.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/joinref.hpp>
#include <duckdb/parser/tableref/pivotref.hpp>
#include <duckdb/parser/tableref/showref.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>

#include <algorithm>
#include <cctype>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "duckdb_read_only.hpp"

namespace chargewire_duckdb {

namespace {

namespace dd = duckdb;
using OpType = dd::PhysicalOperatorType;

constexpr const char* kStateName = "chargewire_read_only_session";

// DuckDB names things with std::string in some places and its Identifier in others.
inline std::string Str(const std::string& name) { return name; }
inline std::string Str(const dd::Identifier& name) { return name.GetIdentifierName(); }
inline std::string Str(const dd::String& name) { return name.ToStdString(); }

std::string Upper(std::string text) {
  std::transform(text.begin(), text.end(), text.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  while (!text.empty() && text.back() == ' ') text.pop_back();
  return text;
}

std::string Lower(std::string text) {
  std::transform(text.begin(), text.end(), text.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return text;
}

// A name in an error detail: never more of it than a name needs.
std::string Shown(const std::string& name) { return name.substr(0, 64); }

class Guard final : public dd::ClientContextState, public ReadOnlyGuard {
 public:
  explicit Guard(std::shared_ptr<const ReadOnlyPolicy> policy) : policy_(std::move(policy)) {}

  // DuckDB shows a plan to its registered states only when one of them may ask for a
  // rebind. This one never asks: it refuses, or lets the plan it was shown run.
  bool CanRequestRebind() override { return true; }

  dd::RebindQueryInfo OnFinalizePrepare(dd::ClientContext&, dd::PreparedStatementData& prepared,
                                        dd::PreparedStatementMode) override {
    if (own_statements_ == 0) Check(prepared);
    return dd::RebindQueryInfo::DO_NOT_REBIND;
  }

  std::optional<std::string> TakeRefusal() override { return std::exchange(refusal_, std::nullopt); }

 private:
  [[noreturn]] void Refuse(std::string detail) {
    refusal_ = std::move(detail);
    throw dd::PermissionException(*refusal_);
  }
  [[noreturn]] void NotListed(const std::string& what) {
    Refuse(Shown(what) + " is not on this server's list of what a read-only session may run: a read-only "
                         "session runs only the operations and table functions the server knows to read");
  }

  void Check(dd::PreparedStatementData& prepared) {
    refusal_.reset();
    // What DuckDB's own planner says the statement modifies: any catalog, the session's
    // temporary one too (a temporary table, a temporary sequence advanced by a query).
    for (const auto& modified : prepared.properties.modified_databases) {
      Refuse("a statement that changes the catalog " + Shown(Str(modified.first)) + " is not allowed in a read-only session");
    }
    // Without a plan nothing can run: the statement has parameters without values yet, and
    // is planned, and shown here, again when it is executed with them.
    if (!prepared.physical_plan) return;
    // Every operator of the plan, without recursion: a plan is as deep as its statement.
    std::vector<dd::const_reference<dd::PhysicalOperator>> pending;
    pending.push_back(prepared.physical_plan->Root());
    while (!pending.empty()) {
      const dd::PhysicalOperator& op = pending.back().get();
      pending.pop_back();
      CheckOperator(op);
      for (const auto& child : op.GetChildren()) pending.push_back(child);
    }
  }

  void CheckOperator(const dd::PhysicalOperator& op) {
    switch (op.type) {
      case OpType::TABLE_SCAN: {
        // A table (DuckDB's own scan function) or a table function, by the function's name.
        const std::string name = Upper(Str(op.Cast<dd::PhysicalTableScan>().function.GetDefinition()->name));
        if (!policy_->plan_operators.contains(name) && !policy_->table_functions.contains(name)) NotListed(name);
        return;
      }
      case OpType::INOUT_FUNCTION: {
        // A table function called for each row of its input. The operator keeps its function
        // to itself; its description names it.
        const auto described = op.ParamsToString();
        const auto name = described.find("Name");
        if (name == described.end()) NotListed("a table function the server cannot name");
        if (!policy_->table_functions.contains(Upper(name->second))) NotListed(Upper(name->second));
        return;
      }
      case OpType::SET: {
        const auto& set = op.Cast<dd::PhysicalSet>();
        CheckSetting(Str(set.name), set.scope);
        return;
      }
      case OpType::RESET: {
        const auto& reset = op.Cast<dd::PhysicalReset>();
        CheckSetting(Str(reset.name), reset.scope);
        return;
      }
      case OpType::SET_VARIABLE:
        return;  // the session's own variable; the plan of its value is walked like any other
      case OpType::PRAGMA: {
        const std::string name = Lower(Str(op.Cast<dd::PhysicalPragma>().info->function.name));
        if (!policy_->pragmas.contains(name)) Refuse("PRAGMA " + Shown(name) + " is not allowed in a read-only session");
        return;
      }
      default: {
        const std::string name = Upper(op.GetName());
        if (!policy_->plan_operators.contains(name)) NotListed(name);
        return;
      }
    }
  }

  // A listed setting, at session scope. A SET without a scope, which DuckDB applies to every
  // session for some settings, never gets here from the adapter (ScopeSettingToSession); one
  // that does is refused, not guessed at.
  void CheckSetting(const std::string& name, dd::SetScope scope) {
    if (scope == dd::SetScope::VARIABLE) return;  // RESET VARIABLE
    if (scope == dd::SetScope::GLOBAL) Refuse("SET GLOBAL is not allowed in a read-only session");
    if (!policy_->settings.contains(Lower(name))) Refuse("SET " + Shown(name) + " is not allowed in a read-only session");
    if (scope != dd::SetScope::SESSION) Refuse("a setting without a scope is not allowed in a read-only session");
  }

  const std::shared_ptr<const ReadOnlyPolicy> policy_;
  std::optional<std::string> refusal_;
};

// What DuckDB evaluates while it binds a statement, before any plan exists that the guard
// could refuse: the arguments of table functions, the value of a setting, the parameters of
// a pragma. An expression there has run by the time its statement is refused (a sequence
// advanced or set stays so), so in a read-only session such an expression may hold values
// only: constants, parameters, columns, casts, operators, lists and structs of those, and a
// few functions that read the session. A function that takes SQL text or a table's name as
// its argument is refused by name, because what DuckDB then binds is not in the statement.
class BindScreen {
 public:
  explicit BindScreen(ReadOnlyReferences& references) : references_(references) {}

  void Statement(dd::SQLStatement& statement) {
    switch (statement.type) {
      case dd::StatementType::CREATE_STATEMENT: {
        // Only ever a view's own definition, which the adapter screens because DuckDB binds
        // it with the statement that reads the view (a client's CREATE is refused by kind).
        auto& info = statement.Cast<dd::CreateStatement>().info;
        if (!info || info->type != dd::CatalogType::VIEW_ENTRY) throw ReadOnlyRefusal("this definition");
        auto& query = info->Cast<dd::CreateViewInfo>().query;
        if (query && query->node) Node(*query->node);
        return;
      }
      case dd::StatementType::SELECT_STATEMENT:
        if (auto& node = statement.Cast<dd::SelectStatement>().node) Node(*node);
        return;
      case dd::StatementType::EXPLAIN_STATEMENT:
        if (auto& inner = statement.Cast<dd::ExplainStatement>().stmt) Statement(*inner);
        return;
      case dd::StatementType::SET_STATEMENT: {
        auto& set = statement.Cast<dd::SetStatement>();
        if (set.set_type != dd::SetType::SET) return;
        auto& value = statement.Cast<dd::SetVariableStatement>().value;
        if (!value) return;
        if (set.scope == dd::SetScope::VARIABLE) {
          Expression(*value);  // a variable's value is planned and checked like a query
        } else {
          ValuesOnly(*value, "the value of a setting");
        }
        return;
      }
      case dd::StatementType::PRAGMA_STATEMENT: {
        auto& info = *statement.Cast<dd::PragmaStatement>().info;
        for (auto& parameter : info.parameters) ValuesOnly(*parameter, "a parameter of a PRAGMA");
        for (auto& parameter : info.named_parameters) ValuesOnly(*parameter.second, "a parameter of a PRAGMA");
        return;
      }
      default:
        return;  // nothing of the other kinds a read-only session runs is evaluated while binding
    }
  }

 private:
  // DuckDB's own enumeration goes through a query once, into every table reference, join,
  // derived table and set operation below it, and calls back for each expression and each
  // table reference: nothing is descended into a second time from here (each query is walked
  // once, however deep the nesting).
  void Node(dd::QueryNode& node) {
    if (!seen_.insert(&node).second) return;
    dd::ParsedExpressionIterator::EnumerateQueryNodeChildren(
        node, [&](dd::unique_ptr<dd::ParsedExpression>& child) { if (child) Expression(*child); },
        [&](dd::TableRef& ref) { Ref(ref); });
  }

  // One table reference (DuckDB has already gone into what is below it).
  void Ref(dd::TableRef& ref) {
    if (ref.type == dd::TableReferenceType::BASE_TABLE) {
      auto& table = ref.Cast<dd::BaseTableRef>();
      references_.relations.insert(Lower(Str(table.GetQualifiedName().Name())));
      // AT (VERSION => ...) / AT (TIMESTAMP => ...): DuckDB evaluates it to find the table's version.
      if (table.at_clause && table.at_clause->ExpressionMutable()) {
        ValuesOnly(*table.at_clause->ExpressionMutable(), "the version or time of a table");
      }
      return;
    }
    if (ref.type == dd::TableReferenceType::SHOW_REF) {
      // SHOW x / DESCRIBE x: DuckDB binds the table or view of that name.
      const std::string name = Lower(Str(ref.Cast<dd::ShowRef>().GetTableName()));
      if (!name.empty()) references_.relations.insert(name);
      return;
    }
    if (ref.type != dd::TableReferenceType::TABLE_FUNCTION) return;
    auto& function = ref.Cast<dd::TableFunctionRef>().function;
    if (!function) return;
    if (function->GetExpressionClass() != dd::ExpressionClass::FUNCTION) {
      ValuesOnly(*function, "an argument of a table function");
      return;
    }
    auto& call = function->Cast<dd::FunctionExpression>();
    const std::string name = Lower(Str(call.FunctionName()));
    references_.functions.insert(name);
    if (name == "query" || name == "query_table") {
      throw ReadOnlyRefusal("the table function " + Upper(name) + ", which takes SQL as its argument,");
    }
    dd::ParsedExpressionIterator::EnumerateChildren(
        call, [&](dd::ParsedExpression& argument) { ValuesOnly(argument, "an argument of a table function"); });
  }

  // An expression of the query itself: evaluated when the plan runs, which the guard
  // decides. Only the queries inside it are followed.
  void Expression(dd::ParsedExpression& expression) {
    if (expression.GetExpressionClass() == dd::ExpressionClass::FUNCTION) {
      references_.functions.insert(Lower(Str(expression.Cast<dd::FunctionExpression>().FunctionName())));
    }
    if (expression.GetExpressionClass() == dd::ExpressionClass::SUBQUERY) {
      auto& subquery = expression.Cast<dd::SubqueryExpression>().SubqueryMutable();
      if (subquery && subquery->node) Node(*subquery->node);
    }
    dd::ParsedExpressionIterator::EnumerateChildren(expression, [&](dd::ParsedExpression& child) { Expression(child); });
  }

  // The text of a string constant; nullopt for anything else.
  static std::optional<std::string> StringConstant(const dd::ParsedExpression& expression) {
    if (expression.GetExpressionClass() != dd::ExpressionClass::CONSTANT) return std::nullopt;
    const std::string text = expression.ToString();
    if (text.size() < 2 || text.front() != '\'' || text.back() != '\'') return std::nullopt;
    std::string out;
    for (std::size_t i = 1; i + 1 < text.size(); i++) {
      if (text[i] == '\'' && i + 2 < text.size() && text[i + 1] == '\'') i++;
      out += text[i];
    }
    return out;
  }

  // Whether an expression can produce a string that the statement does not spell out: a
  // bind parameter, a query, or a function or operator over a string or over one of those.
  static bool ComputesAString(dd::ParsedExpression& expression, bool below_a_call = false) {
    const auto kind = expression.GetExpressionClass();
    if (kind == dd::ExpressionClass::PARAMETER || kind == dd::ExpressionClass::SUBQUERY) return true;
    if (kind == dd::ExpressionClass::CONSTANT) return below_a_call && StringConstant(expression).has_value();
    bool call = below_a_call;
    if (kind == dd::ExpressionClass::FUNCTION) {
      static const ReadOnlyPolicy::Names kContainers = {"list_value", "struct_pack", "row", "map"};
      if (!kContainers.contains(Lower(Str(expression.Cast<dd::FunctionExpression>().FunctionName())))) call = true;
    } else if (kind == dd::ExpressionClass::OPERATOR || kind == dd::ExpressionClass::CASE) {
      call = true;
    }
    bool computes = false;
    dd::ParsedExpressionIterator::EnumerateChildren(
        expression, [&](dd::ParsedExpression& child) { computes = computes || ComputesAString(child, call); });
    return computes;
  }

  // An expression DuckDB evaluates while binding.
  void ValuesOnly(dd::ParsedExpression& expression, const char* where, bool top = true) {
    if (top && ComputesAString(expression)) references_.computed_names = true;
    if (auto text = StringConstant(expression)) {
      // A string there can name a table or a view, which DuckDB then binds: as written, and
      // its last part where it is qualified.
      std::string name = Lower(*text);
      references_.named.insert(name);
      if (const auto dot = name.rfind('.'); dot != std::string::npos) name = name.substr(dot + 1);
      std::erase(name, '"');
      references_.named.insert(name);
    }
    switch (expression.GetExpressionClass()) {
      case dd::ExpressionClass::CONSTANT:
      case dd::ExpressionClass::PARAMETER:
      case dd::ExpressionClass::COLUMN_REF:
      case dd::ExpressionClass::CAST:
      case dd::ExpressionClass::COMPARISON:
      case dd::ExpressionClass::CONJUNCTION:
      case dd::ExpressionClass::OPERATOR:
      case dd::ExpressionClass::COLLATE:
      case dd::ExpressionClass::STAR:
        break;
      case dd::ExpressionClass::SUBQUERY:
        Expression(expression);  // a query as an argument is planned, not evaluated here
        return;
      case dd::ExpressionClass::FUNCTION: {
        auto& call = expression.Cast<dd::FunctionExpression>();
        // Values made of values, the session's own state, and the time (for the version or
        // time a table is read AT).
        static const ReadOnlyPolicy::Names kReadsOnly = {
            "list_value", "struct_pack", "row", "map", "getvariable", "current_schema", "current_database",
            "current_date", "today", "now", "get_current_timestamp", "current_timestamp", "current_localtimestamp",
            "to_years", "to_months", "to_weeks", "to_days", "to_hours", "to_minutes", "to_seconds",
            "to_milliseconds", "to_microseconds", "date_trunc", "make_date", "make_timestamp", "strptime",
            "strftime", "epoch_ms", "trunc", "round", "floor", "ceil", "abs", "concat", "lower", "upper", "format"};
        const std::string called = Lower(Str(call.FunctionName()));
        // (a macro of that name would not be the function: the adapter refuses one, ScreenReadOnly)
        references_.evaluated.insert(called);
        if (!call.IsOperator() && !kReadsOnly.contains(called)) {
          throw ReadOnlyRefusal(std::string(where) + " that calls " + Upper(Shown(Str(call.FunctionName()))) +
                                " (it would run before the statement can be checked)");
        }
        break;
      }
      default:
        throw ReadOnlyRefusal(std::string(where) + " that is not a plain value (it would run before the statement can be checked)");
    }
    dd::ParsedExpressionIterator::EnumerateChildren(
        expression, [&](dd::ParsedExpression& child) { ValuesOnly(child, where, false); });
  }

  ReadOnlyReferences& references_;
  std::unordered_set<const dd::QueryNode*> seen_;  // the queries already walked
};

}  // namespace

ReadOnlyReferences ScreenBeforeBinding(duckdb_v2_sql_statement_handle statement) {
  ReadOnlyReferences references;
  if (dd::SQLStatement* parsed = dd::capiv2::Convert(statement)) BindScreen(references).Statement(*parsed);
  return references;
}

std::string ReadOnlyCatalogStamp(duckdb_v2_connection_handle connection) {
  auto& context = *dd::capiv2::Convert(connection)->context;
  std::string stamp;
  for (const auto& database : dd::DatabaseManager::Get(context).GetDatabases(context)) {
    auto& catalog = database->GetCatalog();
    if (!catalog.IsDuckCatalog()) continue;  // another system's catalog holds no DuckDB views or macros
    const auto version = catalog.GetCatalogVersion(context);
    if (!version.IsValid()) return {};
    stamp += std::to_string(catalog.GetOid()) + ":" + std::to_string(version.GetIndex()) + ";";
  }
  return stamp;
}

void ScopeSettingToSession(duckdb_v2_sql_statement_handle statement) {
  dd::SQLStatement* parsed = dd::capiv2::Convert(statement);
  if (parsed == nullptr || parsed->type != dd::StatementType::SET_STATEMENT) return;
  auto& set = parsed->Cast<dd::SetStatement>();
  if (set.scope == dd::SetScope::AUTOMATIC) set.scope = dd::SetScope::SESSION;
}

std::shared_ptr<ReadOnlyGuard> InstallReadOnlyGuard(duckdb_v2_connection_handle connection,
                                                    std::shared_ptr<const ReadOnlyPolicy> policy) {
  auto& context = *dd::capiv2::Convert(connection)->context;
  auto guard = dd::make_shared_ptr<Guard>(std::move(policy));
  context.registered_state->Insert(kStateName, guard);
  // The adapter's handle keeps DuckDB's own alive (DuckDB's shared_ptr is its own type).
  return std::shared_ptr<ReadOnlyGuard>(static_cast<ReadOnlyGuard*>(guard.get()), [guard](ReadOnlyGuard*) {});
}

}  // namespace chargewire_duckdb
