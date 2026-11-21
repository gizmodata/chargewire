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
#include <duckdb/main/extension_entries.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/parser/common_table_expression_info.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/subquery_expression.hpp>
#include <duckdb/parser/expression/window_expression.hpp>
#include <duckdb/parser/parsed_data/copy_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_data/pragma_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/query_node.hpp>
#include <duckdb/parser/query_node/copy_query_node.hpp>
#include <duckdb/parser/query_node/cte_node.hpp>
#include <duckdb/parser/query_node/recursive_cte_node.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/query_node/set_operation_node.hpp>
#include <duckdb/parser/result_modifier.hpp>
#include <duckdb/parser/statement/call_statement.hpp>
#include <duckdb/parser/statement/copy_statement.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/explain_statement.hpp>
#include <duckdb/parser/statement/pragma_statement.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/statement/set_statement.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/expressionlistref.hpp>
#include <duckdb/parser/tableref/joinref.hpp>
#include <duckdb/parser/tableref/pivotref.hpp>
#include <duckdb/parser/tableref/showref.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/meta_transaction.hpp>

#include <algorithm>
#include <cctype>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "duckdb_read_only.hpp"

// Added to DuckDB by scripts/patch-duckdb-read-only.cmake: called before the bind step of
// every table function. A DuckDB without the patch does not link with this file.
namespace duckdb {
extern void (*chargewire_bind_table_function_hook)(ClientContext& context, const Identifier& function_name);
}

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

  std::optional<std::string> TakeRefusal() override {
    const std::lock_guard lock(refusal_mutex_);
    return std::exchange(refusal_, std::nullopt);
  }

  // Before DuckDB runs the bind step of a table function, wherever its call was written (the
  // client's statement, a view or macro of anyone's, a view that a catalog function binds
  // when it runs): one that is on none of the lists is refused there, its bind step unrun.
  void BeforeTableFunctionBind(const std::string& function_name) {
    if (own_statements_ != 0) return;
    const std::string name = Upper(function_name);
    if (policy_->table_functions.contains(name) || policy_->explained_table_functions.contains(name) ||
        policy_->plan_operators.contains(name)) {
      return;
    }
    // The adapter's own, with which it reads the Arrow schema of a statement it describes
    // (it runs nothing, and no plan with it passes the guard).
    if (name == "CHARGEWIRE_RESULT_SCHEMA") return;
    NotListed(name);
  }

 private:
  [[noreturn]] void Refuse(std::string detail) {
    {
      const std::lock_guard lock(refusal_mutex_);
      refusal_ = detail;
    }
    throw dd::PermissionException(detail);
  }
  [[noreturn]] void NotListed(const std::string& what) { Refuse(ReadOnlyNotListed(Shown(what)).what()); }

  void Check(dd::PreparedStatementData& prepared) {
    {
      const std::lock_guard lock(refusal_mutex_);
      refusal_.reset();
    }
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
  std::mutex refusal_mutex_;
  std::optional<std::string> refusal_;
};

// The hook DuckDB calls before any table function's bind step: for a connection that carries
// a read-only guard, the guard decides; any other connection is not this file's business.
void BeforeTableFunctionBind(dd::ClientContext& context, const dd::Identifier& function_name) {
  auto guard = context.registered_state->Get<Guard>(kStateName);
  if (guard) guard->BeforeTableFunctionBind(Str(function_name));
}

// DuckDB reads the raw hook pointer from every binder. Publish it during process
// startup, before any connection can bind, and keep the policy in session state.
[[maybe_unused]] const bool kTableFunctionBindHookInstalled = [] {
  dd::chargewire_bind_table_function_hook = &BeforeTableFunctionBind;
  return true;
}();

// The screen of a read-only session's statement, before DuckDB binds anything of it
// (duckdb_read_only.hpp). DuckDB evaluates while it binds, before any plan exists that the
// guard could refuse: the arguments of table functions, the value of a setting, the
// parameters of a pragma, the file name and the options of a COPY, the version or time a
// table is read AT. An expression there has run by the time its statement is refused (a
// sequence advanced or set stays so), so in a read-only session such an expression may hold
// values only: constants, parameters, columns, casts, operators, lists and structs of those,
// and a few functions that read the session.
//
// The screen walks the statement itself and decides for every kind of statement, query,
// table reference and expression by name; a kind without a decision is refused. That walk is
// what holds. As a second look at its top level it is held against DuckDB's own enumeration
// of the same query (which goes into table references, joins, derived tables, set operations
// and CTEs, not into subqueries in expressions, AT clauses, PIVOT entries or a table
// function's subquery): a part DuckDB finds there that the walk did not reach refuses the
// statement too.
// Whether DuckDB knows `name` (lower case) as a table function or pragma of an extension it
// installs and loads on demand, when a statement names it.
bool ExtensionTableFunction(std::string_view name) {
  for (const auto& entry : dd::EXTENSION_FUNCTIONS) {
    if (name != entry.name) continue;
    if (entry.type == dd::CatalogType::TABLE_FUNCTION_ENTRY || entry.type == dd::CatalogType::TABLE_MACRO_ENTRY ||
        entry.type == dd::CatalogType::PRAGMA_FUNCTION_ENTRY) {
      return true;
    }
  }
  return false;
}

class BindScreen {
 public:
  BindScreen(ReadOnlyReferences& references, const ReadOnlyScreen& screen) : references_(references), screen_(screen) {}

  // Where a statement stands: sent by the client, under an EXPLAIN, or as a definition.
  enum class Where { kClient, kExplained, kDefinition };

  void Statement(dd::SQLStatement& statement, Where where) {
    using Kind = dd::StatementType;
    switch (statement.type) {
      case Kind::SELECT_STATEMENT: {
        auto& node = statement.Cast<dd::SelectStatement>().node;
        if (!node) Unwalked("a query without a body");
        TopQuery(*node);
        return;
      }
      case Kind::EXPLAIN_STATEMENT: {
        // What an EXPLAIN holds is bound like any statement: walked as the statement it is.
        if (where == Where::kDefinition) break;
        auto& inner = statement.Cast<dd::ExplainStatement>().stmt;
        if (!inner) Unwalked("an EXPLAIN of nothing");
        Statement(*inner, Where::kExplained);
        return;
      }
      case Kind::SET_STATEMENT: {
        if (where == Where::kDefinition) break;
        auto& set = statement.Cast<dd::SetStatement>();
        if (set.set_type != dd::SetType::SET) return;  // RESET names a setting and holds no value
        auto& value = statement.Cast<dd::SetVariableStatement>().value;
        if (!value) return;
        if (set.scope == dd::SetScope::VARIABLE) {
          Expression(*value);  // a variable's value is planned and checked like a query
        } else {
          ValuesOnly(*value, "the value of a setting");
        }
        return;
      }
      case Kind::PRAGMA_STATEMENT: {
        // Only as the client's own statement: DuckDB replaces some pragmas by the text of a
        // statement, which it then binds, so a pragma is bound only when it is listed. (Under
        // an EXPLAIN no pragma that is listed can be planned.)
        if (where != Where::kClient) break;
        auto& info = *statement.Cast<dd::PragmaStatement>().info;
        const std::string name = Lower(Str(info.name));
        if (screen_.policy == nullptr || !screen_.policy->pragmas.contains(name)) {
          throw ReadOnlyRefusal("PRAGMA " + Shown(name));
        }
        for (auto& parameter : info.parameters) ValuesOnly(*parameter, "a parameter of a PRAGMA");
        for (auto& parameter : info.named_parameters) ValuesOnly(*parameter.second, "a parameter of a PRAGMA");
        return;
      }
      case Kind::TRANSACTION_STATEMENT:
        // The adapter runs the client's BEGIN, COMMIT and ROLLBACK itself: DuckDB binds none.
        if (where != Where::kClient) break;
        return;
      case Kind::CREATE_STATEMENT: {
        // Only ever a view's own definition, which the adapter screens because DuckDB binds
        // it with the statement that reads the view (a client's CREATE is refused by kind).
        if (where != Where::kDefinition) break;
        auto& info = statement.Cast<dd::CreateStatement>().info;
        if (!info || info->type != dd::CatalogType::VIEW_ENTRY) throw ReadOnlyRefusal("this definition");
        auto& query = info->Cast<dd::CreateViewInfo>().query;
        if (!query || !query->node) Unwalked("a view without a query");
        TopQuery(*query->node);
        return;
      }
      case Kind::COPY_STATEMENT: {
        // EXPLAIN COPY ... TO shows the plan of a statement that would write a file, and
        // writes none.
        if (where != Where::kExplained) break;
        auto& info = statement.Cast<dd::CopyStatement>().info;
        if (!info) Unwalked("a COPY of nothing");
        Copy(*info);
        return;
      }
      case Kind::CALL_STATEMENT: {
        // EXPLAIN CALL f(...): a table function, bound like one in a FROM clause.
        if (where != Where::kExplained) break;
        auto& function = statement.Cast<dd::CallStatement>().function;
        if (!function || function->GetExpressionClass() != dd::ExpressionClass::FUNCTION) {
          Unwalked("a CALL of something that is not a function");
        }
        Seen(function.get());
        TableFunctionCall(function->Cast<dd::FunctionExpression>());
        return;
      }
      // Never bound in a read-only session, wherever they stand. These change a catalog,
      // whatever they hold (their plan is refused for it: nothing is lost by refusing them
      // before DuckDB evaluates what they hold):
      case Kind::INSERT_STATEMENT:
      case Kind::UPDATE_STATEMENT:
      case Kind::DELETE_STATEMENT:
      case Kind::MERGE_INTO_STATEMENT:
      case Kind::ALTER_STATEMENT:
      case Kind::DROP_STATEMENT:
      case Kind::CREATE_FUNC_STATEMENT:
      case Kind::COPY_DATABASE_STATEMENT:
      // these name a prepared statement of the connection, which the screen has not seen
      // (EXECUTE evaluates its arguments while it binds), or make one:
      case Kind::PREPARE_STATEMENT:
      case Kind::EXECUTE_STATEMENT:
      // and these a read-only session has no use for, explained or not:
      case Kind::ANALYZE_STATEMENT:
      case Kind::VARIABLE_SET_STATEMENT:
      case Kind::EXPORT_STATEMENT:
      case Kind::VACUUM_STATEMENT:
      case Kind::LOAD_STATEMENT:
      case Kind::RELATION_STATEMENT:
      case Kind::EXTENSION_STATEMENT:
      case Kind::LOGICAL_PLAN_STATEMENT:
      case Kind::ATTACH_STATEMENT:
      case Kind::DETACH_STATEMENT:
      case Kind::MULTI_STATEMENT:
      case Kind::UPDATE_EXTENSIONS_STATEMENT:
      case Kind::CONNECT_STATEMENT:
      case Kind::DISCONNECT_STATEMENT:
      case Kind::EXTERNAL_RESOURCE_STATEMENT:
      case Kind::PASSTHROUGH_STATEMENT:
      case Kind::INVALID_STATEMENT:
      case Kind::ENUM_SIZE:
        break;
      // (no default: a kind a later DuckDB adds has no case, and is refused below like these)
    }
    const std::string kind = dd::StatementTypeToString(statement.type);
    if (where == Where::kExplained) throw ReadOnlyRefusal("EXPLAIN of " + kind);
    if (where == Where::kDefinition) throw ReadOnlyRefusal("a definition that holds " + kind);
    throw ReadOnlyRefusal(kind);
  }

 private:
  // Something the screen has no decision for: refused, never bound.
  [[noreturn]] static void Unwalked(const std::string& what) {
    throw ReadOnlyRefusal("a statement the server cannot check before it is bound (" + what + ")");
  }

  void Seen(const void* part) { seen_.insert(part); }

  // A query that is not part of another one: walked, and the walk then held against DuckDB's
  // own enumeration of it (ParsedExpressionIterator), which goes into every table reference,
  // join, derived table, set operation and CTE below it. What DuckDB finds there and the
  // walk did not reach, the screen has not judged.
  void TopQuery(dd::QueryNode& node) {
    Query(node);
    bool complete = true;
    try {
      dd::ParsedExpressionIterator::EnumerateQueryNodeChildren(
          node, [&](dd::unique_ptr<dd::ParsedExpression>& child) { complete = complete && (!child || seen_.contains(child.get())); },
          [&](dd::TableRef& ref) { complete = complete && seen_.contains(&ref); });
    } catch (const std::exception&) {
      complete = false;  // a part DuckDB's own enumeration does not know
    }
    if (!complete) Unwalked("a part of it was not reached");
  }

  void Query(dd::QueryNode& node) {
    using Kind = dd::QueryNodeType;
    switch (node.type) {
      case Kind::SELECT_NODE: {
        auto& select = node.Cast<dd::SelectNode>();
        for (auto& expression : select.select_list) Planned(expression);
        for (auto& expression : select.groups.group_expressions) Planned(expression);
        Planned(select.where_clause);
        Planned(select.having);
        Planned(select.qualify);
        if (select.from_table) Ref(*select.from_table);
        break;  // (its sample is a constant the parser read)
      }
      case Kind::SET_OPERATION_NODE:
        for (auto& child : node.Cast<dd::SetOperationNode>().children) {
          if (!child) Unwalked("a set operation without a side");
          Query(*child);
        }
        break;
      case Kind::RECURSIVE_CTE_NODE: {
        auto& recursive = node.Cast<dd::RecursiveCTENode>();
        if (!recursive.left || !recursive.right) Unwalked("a recursive query without a side");
        Query(*recursive.left);
        Query(*recursive.right);
        for (auto& expression : recursive.key_targets) Planned(expression);
        break;
      }
      case Kind::CTE_NODE: {
        auto& cte = node.Cast<dd::CTENode>();
        if (!cte.query || !cte.child) Unwalked("a CTE without a query");
        Query(*cte.query);
        Query(*cte.child);
        break;
      }
      case Kind::COPY_QUERY_NODE: {
        // WITH c AS (COPY ... TO ...): its plan is refused (it writes a file); under an
        // EXPLAIN it is only shown. Binding it evaluates the file name and the options.
        auto& info = node.Cast<dd::CopyQueryNode>().info;
        if (!info) Unwalked("a COPY of nothing");
        Copy(*info);
        break;
      }
      // A CTE that writes: its plan changes a catalog and is refused for it, whatever it
      // holds. Refused here, before DuckDB evaluates what it holds.
      case Kind::INSERT_QUERY_NODE:
        throw ReadOnlyRefusal("INSERT");
      case Kind::UPDATE_QUERY_NODE:
        throw ReadOnlyRefusal("UPDATE");
      case Kind::DELETE_QUERY_NODE:
        throw ReadOnlyRefusal("DELETE");
      case Kind::MERGE_QUERY_NODE:
        throw ReadOnlyRefusal("MERGE");
      case Kind::STATEMENT_NODE:
      case Kind::BOUND_SUBQUERY_NODE:
      default:
        Unwalked("a kind of query the server does not know");
    }
    for (auto& modifier : node.modifiers) {
      if (!modifier) continue;
      switch (modifier->type) {
        case dd::ResultModifierType::LIMIT_MODIFIER: {
          auto& limit = modifier->Cast<dd::LimitModifier>();
          Planned(limit.limit);
          Planned(limit.offset);
          break;
        }
        case dd::ResultModifierType::LEGACY_LIMIT_PERCENT_MODIFIER: {
          auto& limit = modifier->Cast<dd::LegacyLimitPercentModifier>();
          Planned(limit.limit);
          Planned(limit.offset);
          break;
        }
        case dd::ResultModifierType::ORDER_MODIFIER:
          for (auto& order : modifier->Cast<dd::OrderModifier>().orders) Planned(order.expression);
          break;
        case dd::ResultModifierType::DISTINCT_MODIFIER:
          for (auto& target : modifier->Cast<dd::DistinctModifier>().distinct_on_targets) Planned(target);
          break;
        default:
          Unwalked("a kind of ORDER BY or LIMIT the server does not know");
      }
    }
    for (auto& cte : node.cte_map.map) {
      if (!cte.second) continue;
      for (auto& expression : cte.second->key_targets) Planned(expression);
      for (auto& expression : cte.second->payload_aggregates) Planned(expression);
      if (!cte.second->query_node) Unwalked("a CTE without a query");
      Query(*cte.second->query_node);
    }
  }

  // COPY ... TO, as an EXPLAIN's statement or a CTE's body. DuckDB evaluates the file name
  // and every option while it binds.
  void Copy(dd::CopyInfo& info) {
    if (info.is_from) throw ReadOnlyRefusal("COPY ... FROM");  // writes the table: its plan is refused for it
    if (info.select_statement) {
      Query(*info.select_statement);
    } else {
      // COPY t TO: DuckDB binds the table or view of that name.
      const std::string name = Lower(Str(info.GetQualifiedName().Name()));
      if (!name.empty()) references_.relations.insert(name);
    }
    if (info.file_path_expression) ValuesOnly(*info.file_path_expression, "the file name of a COPY");
    for (auto& option : info.parsed_options) {
      if (option.second) ValuesOnly(*option.second, "an option of a COPY");
    }
  }

  void Ref(dd::TableRef& ref) {
    Seen(&ref);
    using Kind = dd::TableReferenceType;
    switch (ref.type) {
      case Kind::BASE_TABLE: {
        auto& table = ref.Cast<dd::BaseTableRef>();
        references_.relations.insert(Lower(Str(table.GetQualifiedName().Name())));
        // AT (VERSION => ...) / AT (TIMESTAMP => ...): DuckDB evaluates it to find the table's version.
        if (table.at_clause && table.at_clause->ExpressionMutable()) {
          ValuesOnly(*table.at_clause->ExpressionMutable(), "the version or time of a table");
        }
        return;
      }
      case Kind::SUBQUERY: {
        auto& subquery = ref.Cast<dd::SubqueryRef>().subquery;
        if (!subquery || !subquery->node) Unwalked("a derived table without a query");
        Query(*subquery->node);
        return;
      }
      case Kind::JOIN: {
        auto& join = ref.Cast<dd::JoinRef>();
        if (!join.left || !join.right) Unwalked("a join without a side");
        Ref(*join.left);
        Ref(*join.right);
        Planned(join.condition);
        Planned(join.ranking_expression);
        for (auto& expression : join.duplicate_eliminated_columns) Planned(expression);
        return;
      }
      case Kind::TABLE_FUNCTION: {
        auto& function = ref.Cast<dd::TableFunctionRef>();
        if (function.subquery) {
          if (!function.subquery->node) Unwalked("a table function over a query without a body");
          Query(*function.subquery->node);
        }
        if (!function.function || function.function->GetExpressionClass() != dd::ExpressionClass::FUNCTION) {
          Unwalked("a table function the server cannot name");
        }
        Seen(function.function.get());
        TableFunctionCall(function.function->Cast<dd::FunctionExpression>());
        return;
      }
      case Kind::EXPRESSION_LIST:
        for (auto& row : ref.Cast<dd::ExpressionListRef>().values) {
          for (auto& expression : row) Planned(expression);
        }
        return;
      case Kind::PIVOT: {
        auto& pivot = ref.Cast<dd::PivotRef>();
        if (!pivot.source) Unwalked("a PIVOT of nothing");
        Ref(*pivot.source);
        for (auto& expression : pivot.aggregates) Planned(expression);
        for (auto& column : pivot.pivots) {
          // What is pivoted on is an expression of the query DuckDB makes of the pivot.
          for (auto& expression : column.pivot_expressions) Planned(expression);
          for (auto& entry : column.entries) Planned(entry.expr);
          if (column.subquery) Query(*column.subquery);
        }
        return;
      }
      case Kind::SHOW_REF: {
        // SHOW x / DESCRIBE x: DuckDB binds the table or view of that name, or the query.
        auto& show = ref.Cast<dd::ShowRef>();
        const std::string name = Lower(Str(show.GetTableName()));
        if (!name.empty()) references_.relations.insert(name);
        if (show.query) Query(*show.query);
        return;
      }
      case Kind::EMPTY_FROM:   // no FROM clause
      case Kind::COLUMN_DATA:  // rows the parser read: nothing to bind
        return;
      case Kind::DELIM_GET:
      case Kind::CTE:
      case Kind::BOUND_TABLE_REF:
      case Kind::INVALID:
      default:
        Unwalked("a kind of table reference the server does not know");
    }
  }

  // A table function by its name, in a FROM clause or a CALL. (Whether it is bound at all is
  // decided where DuckDB binds it: BeforeTableFunctionBind. The bind step of one on no list never runs
  // (a function that binds or runs SQL the statement does not hold, one that hands SQL to
  // another system, one an extension added that nobody has looked at).
  void TableFunctionCall(dd::FunctionExpression& call) {
    const std::string name = Lower(Str(call.FunctionName()));
    references_.functions.insert(name);
    static const ReadOnlyPolicy::Names kTakesSql = {
        "query", "query_table", "json_execute_serialized_sql", "postgres_query", "postgres_execute", "mysql_query",
        "mysql_execute", "sqlite_query", "odbc_query", "quack_query", "quack_query_by_name", "duckdb_query"};
    if (kTakesSql.contains(name)) {
      throw ReadOnlyRefusal("the table function " + Upper(Shown(name)) + ", which takes SQL as its argument,");
    }
    const std::string listed = Upper(name);
    const bool known = screen_.policy != nullptr && (screen_.policy->table_functions.contains(listed) ||
                                                     screen_.policy->explained_table_functions.contains(listed));
    // On no list: a table macro of someone's, which DuckDB expands, or a function that the
    // guard refuses when DuckDB comes to bind it (BeforeTableFunctionBind), as it does for a
    // call that no statement shows: in a view, in a macro, in a view bound later. One that
    // DuckDB would install and load an extension for when it looks the name up is refused
    // here, before it does.
    if (!known && ExtensionTableFunction(name)) throw ReadOnlyNotListed(Upper(Shown(name)));
    for (auto& argument : call.ChildrenMutable()) {
      if (argument) ValuesOnly(*argument, "an argument of a table function");
    }
  }

  // An expression of the query itself: evaluated when the plan runs, which the guard decides.
  void Planned(dd::unique_ptr<dd::ParsedExpression>& expression) {
    if (!expression) return;
    Seen(expression.get());
    Expression(*expression);
  }

  void Expression(dd::ParsedExpression& expression) {
    using Class = dd::ExpressionClass;
    switch (expression.GetExpressionClass()) {
      case Class::FUNCTION: {
        auto& call = expression.Cast<dd::FunctionExpression>();
        const std::string name = Lower(Str(call.FunctionName()));
        references_.functions.insert(name);
        // Scalar functions that bind SQL they are given as text: what they bind is evaluated
        // like a statement's own, and no plan of it is ever shown to the guard.
        static const ReadOnlyPolicy::Names kBindsSql = {"json_serialize_plan"};
        if (kBindsSql.contains(name)) {
          throw ReadOnlyRefusal("the function " + Upper(Shown(name)) + ", which takes SQL as its argument,");
        }
        break;
      }
      case Class::WINDOW:
        references_.functions.insert(Lower(Str(expression.Cast<dd::WindowExpression>().FunctionName())));
        break;
      case Class::SUBQUERY: {
        auto& subquery = expression.Cast<dd::SubqueryExpression>().SubqueryMutable();
        if (!subquery || !subquery->node) Unwalked("a subquery without a query");
        Query(*subquery->node);
        break;
      }
      case Class::CONSTANT:
      case Class::PARAMETER:
      case Class::COLUMN_REF:
      case Class::DEFAULT:
      case Class::POSITIONAL_REFERENCE:
      case Class::LAMBDA_REF:
      case Class::CASE:
      case Class::CAST:
      case Class::COLLATE:
      case Class::COMPARISON:
      case Class::CONJUNCTION:
      case Class::OPERATOR:
      case Class::BETWEEN:
      case Class::STAR:
      case Class::LAMBDA:
      case Class::TYPE:
        break;
      default:
        Unwalked("a kind of expression the server does not know");
    }
    for (auto& child : expression.ChildrenMutable()) {
      if (child) Expression(*child);
    }
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
    if (top) {
      Seen(&expression);
      if (ComputesAString(expression)) references_.computed_names = true;
    }
    // A name there can be that of a table or a view, which DuckDB then binds: as written,
    // and its last part where it is qualified.
    auto names = [&](std::string name) {
      references_.named.insert(name);
      if (const auto dot = name.rfind('.'); dot != std::string::npos) name = name.substr(dot + 1);
      std::erase(name, '"');
      references_.named.insert(name);
    };
    if (auto text = StringConstant(expression)) names(Lower(*text));
    switch (expression.GetExpressionClass()) {
      case dd::ExpressionClass::COLUMN_REF:
      case dd::ExpressionClass::CONSTANT:
      case dd::ExpressionClass::PARAMETER:
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
    for (auto& child : expression.ChildrenMutable()) {
      if (child) ValuesOnly(*child, where, false);
    }
  }

  ReadOnlyReferences& references_;
  const ReadOnlyScreen& screen_;
  std::unordered_set<const void*> seen_;  // the expressions and table references the walk has judged
};

}  // namespace

ReadOnlyReferences ScreenBeforeBinding(duckdb_v2_sql_statement_handle statement, const ReadOnlyScreen& screen,
                                       ReadOnlyScreened what) {
  ReadOnlyReferences references;
  dd::SQLStatement* parsed = dd::capiv2::Convert(statement);
  if (parsed == nullptr) throw ReadOnlyRefusal("a statement the server cannot check before it is bound");
  BindScreen(references, screen)
      .Statement(*parsed, what == ReadOnlyScreened::kDefinition ? BindScreen::Where::kDefinition : BindScreen::Where::kClient);
  return references;
}

ConnectionTransaction TransactionOf(duckdb_v2_connection_handle connection) {
  auto& context = *dd::capiv2::Convert(connection)->context;
  ConnectionTransaction now;
  now.active = context.transaction.HasActiveTransaction();
  if (!now.active) return now;
  auto& transaction = context.transaction.ActiveTransaction();
  now.changes = transaction.ModifiedDatabase() != nullptr;
  for (const auto& opened : transaction.OpenedTransactions()) {
    dd::AttachedDatabase& database = opened.get();
    if (database.IsSystem()) continue;
    auto in_database = transaction.TryGetTransaction(database);
    if (!in_database) continue;
    // Told it will be written, or (DuckDB's own databases) changed already: also by a
    // statement that was only bound, which DuckDB is not told of.
    if (!in_database->IsReadOnly()) now.changes = true;
    if (in_database->IsDuckTransaction() && in_database->Cast<dd::DuckTransaction>().ChangesMade()) now.changes = true;
  }
  return now;
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
