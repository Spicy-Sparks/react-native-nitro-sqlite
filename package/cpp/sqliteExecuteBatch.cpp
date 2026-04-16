/**
 * SQL Batch execution implementation using default sqliteBridge implementation
 */
#include "sqliteExecuteBatch.hpp"
#include "NitroSQLiteException.hpp"
#include "logs.hpp"
#include "operations.hpp"
#include <utility>

namespace margelo::rnnitrosqlite {

std::vector<BatchQuery> batchParamsToCommands(const std::vector<BatchQueryCommand>& batchParams) {
  auto commands = std::vector<BatchQuery>();

  for (auto& command : batchParams) {
    if (command.params) {
      using ParamsVec = SQLiteQueryParams;
      using NestedParamsVec = std::vector<ParamsVec>;

      if (std::holds_alternative<NestedParamsVec>(*command.params)) {
        // This arguments is an array of arrays, like a batch update of a single sql command.
        const auto& nestedParams = std::get<NestedParamsVec>(*command.params);
        if (nestedParams.empty()) {
          LOGW("batchParamsToCommands: query decoded with empty nested params; treating as a single command with zero params. queryLength=%zu",
               command.query.size());
          commands.push_back(BatchQuery{command.query, ParamsVec()});
          continue;
        }

        for (const auto& params : nestedParams) {
          commands.push_back(BatchQuery{command.query, ParamsVec(params)});
        }
      } else {
        commands.push_back(BatchQuery{command.query, std::move(std::get<ParamsVec>(*command.params))});
      }
    } else {
      commands.push_back(BatchQuery{command.query, std::nullopt});
    }
  }

  return commands;
}

SQLiteOperationResult sqliteExecuteBatch(const std::string& dbName, const std::vector<BatchQuery>& commands, bool ignoreNull) {
  size_t commandCount = commands.size();
  LOGI("sqliteExecuteBatch: db=%s commandCount=%zu ignoreNull=%d", dbName.c_str(), commandCount, ignoreNull ? 1 : 0);
  for (size_t i = 0; i < commandCount; ++i) {
    const auto paramsCount = commands[i].params ? commands[i].params->size() : 0;
    LOGI("sqliteExecuteBatch[%zu]: paramsCount=%zu sql=%s", i, paramsCount, commands[i].sql.c_str());
  }

  if (commandCount <= 0) {
    LOGE("sqliteExecuteBatch: db=%s throwing NoBatchCommandsProvided because executable commandCount is zero", dbName.c_str());
    throw NitroSQLiteException(NitroSQLiteExceptionType::NoBatchCommandsProvided, "No SQL batch commands provided");
  }

  try {
    int rowsAffected = 0;
    sqliteExecuteLiteral(dbName, "BEGIN EXCLUSIVE TRANSACTION");
    for (int i = 0; i < commandCount; i++) {
      const auto command = commands.at(i);

      // We do not provide a data structure to receive query data because we don't need/want to handle this results in a batch execution
      auto result = sqliteExecute(dbName, command.sql, command.params, ignoreNull);
      rowsAffected += result->getRowsAffected();
    }
    sqliteExecuteLiteral(dbName, "COMMIT");
    return {
        .rowsAffected = rowsAffected,
        .commands = (int)commandCount,
    };
  } catch (NitroSQLiteException& e) {
    sqliteExecuteLiteral(dbName, "ROLLBACK");
    throw e;
  }
}

} // namespace margelo::rnnitrosqlite
