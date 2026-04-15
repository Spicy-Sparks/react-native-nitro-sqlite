#include "HybridNitroSQLite.hpp"
#include "HybridNativeQueryResult.hpp"
#include "NitroSQLiteException.hpp"
#include "importSqlFile.hpp"
#include "logs.hpp"
#include "macros.hpp"
#include "operations.hpp"
#include "sqliteExecuteBatch.hpp"
#include <algorithm>
#include <exception>
#include <iostream>
#include <map>
#include <string>
#include <variant>
#include <vector>

namespace margelo::nitro::rnnitrosqlite {

namespace {

constexpr size_t kMaxLoggedBatchCommands = 20;
constexpr size_t kMaxLoggedSqlLength = 300;

std::string previewSql(const std::string& sql) {
  if (sql.size() <= kMaxLoggedSqlLength) {
    return sql;
  }

  return sql.substr(0, kMaxLoggedSqlLength) + "...";
}

void logNativeBatchCommand(const char* context, const NativeBatchQueryCommand& command, size_t index) {
  const auto sqlPreview = previewSql(command.query);

  if (!command.params) {
    LOGI("%s[%zu]: queryLength=%zu params=none sql=%s", context, index, command.query.size(), sqlPreview.c_str());
    return;
  }

  using ParamsVec = margelo::rnnitrosqlite::SQLiteQueryParams;
  using NestedParamsVec = std::vector<ParamsVec>;

  if (std::holds_alternative<NestedParamsVec>(*command.params)) {
    const auto& nestedParams = std::get<NestedParamsVec>(*command.params);
    const auto firstParamSetSize = nestedParams.empty() ? 0 : nestedParams.front().size();
    LOGI("%s[%zu]: queryLength=%zu params=nested paramSets=%zu firstParamSetSize=%zu sql=%s", context, index, command.query.size(),
         nestedParams.size(), firstParamSetSize, sqlPreview.c_str());
    if (nestedParams.empty()) {
      LOGW("%s[%zu]: params decoded as an empty nested array; native conversion will treat this as one command with zero params", context, index);
    }
    return;
  }

  const auto& params = std::get<ParamsVec>(*command.params);
  LOGI("%s[%zu]: queryLength=%zu params=flat paramCount=%zu sql=%s", context, index, command.query.size(), params.size(), sqlPreview.c_str());
}

void logNativeBatchCommands(const char* context, const std::string& dbName, const std::vector<NativeBatchQueryCommand>& batchParams,
                            std::optional<bool> ignoreNull) {
  LOGI("%s: db=%s nativeCommandCount=%zu ignoreNull=%d", context, dbName.c_str(), batchParams.size(), ignoreNull.value_or(false) ? 1 : 0);

  const auto loggedCommandCount = std::min(batchParams.size(), kMaxLoggedBatchCommands);
  for (size_t i = 0; i < loggedCommandCount; ++i) {
    logNativeBatchCommand(context, batchParams[i], i);
  }

  if (batchParams.size() > loggedCommandCount) {
    LOGI("%s: skipped logging %zu additional native commands", context, batchParams.size() - loggedCommandCount);
  }
}

} // namespace

const std::string getDocPath(const std::optional<std::string>& location) {
  std::string tempDocPath = std::string(HybridNitroSQLite::docPath);
  if (location) {
    tempDocPath = tempDocPath + "/" + *location;
  }

  return tempDocPath;
}

void HybridNitroSQLite::open(const std::string& dbName, const std::optional<std::string>& location) {
  const auto docPath = getDocPath(location);
  sqliteOpenDb(dbName, docPath);
}

void HybridNitroSQLite::close(const std::string& dbName) {
  sqliteCloseDb(dbName);
};

void HybridNitroSQLite::drop(const std::string& dbName, const std::optional<std::string>& location) {
  const auto docPath = getDocPath(location);
  sqliteRemoveDb(dbName, docPath);
};

void HybridNitroSQLite::attach(const std::string& mainDbName, const std::string& dbNameToAttach, const std::string& alias,
                               const std::optional<std::string>& location) {
  std::string tempDocPath = std::string(docPath);
  if (location) {
    tempDocPath = tempDocPath + "/" + *location;
  }

  sqliteAttachDb(mainDbName, tempDocPath, dbNameToAttach, alias);
};

void HybridNitroSQLite::detach(const std::string& mainDbName, const std::string& alias) {
  sqliteDetachDb(mainDbName, alias);
};

using ExecuteQueryResult = std::shared_ptr<HybridNativeQueryResultSpec>;

ExecuteQueryResult HybridNitroSQLite::execute(const std::string& dbName, const std::string& query,
                                              const std::optional<SQLiteQueryParams>& params, std::optional<bool> ignoreNull) {
  SQLiteExecuteQueryResult result = sqliteExecute(dbName, query, params, ignoreNull.value_or(false));
  return std::make_shared<HybridNativeQueryResult>(std::move(result));
};

std::shared_ptr<Promise<std::shared_ptr<HybridNativeQueryResultSpec>>>
HybridNitroSQLite::executeAsync(const std::string& dbName, const std::string& query, const std::optional<SQLiteQueryParams>& params, std::optional<bool> ignoreNull) {
  return Promise<std::shared_ptr<HybridNativeQueryResultSpec>>::async([=, this]() -> std::shared_ptr<HybridNativeQueryResultSpec> {
    auto result = execute(dbName, query, params, ignoreNull);
    return result;
  });
};

BatchQueryResult HybridNitroSQLite::executeBatch(const std::string& dbName, const std::vector<NativeBatchQueryCommand>& batchParams, std::optional<bool> ignoreNull) {
  logNativeBatchCommands("HybridNitroSQLite::executeBatch", dbName, batchParams, ignoreNull);

  const auto commands = batchParamsToCommands(batchParams);
  LOGI("HybridNitroSQLite::executeBatch: db=%s executableCommandCount=%zu", dbName.c_str(), commands.size());
  if (batchParams.size() > 0 && commands.empty()) {
    LOGW("HybridNitroSQLite::executeBatch: nativeCommandCount=%zu converted to zero executable commands; check for empty nested params arrays",
         batchParams.size());
  }

  auto result = sqliteExecuteBatch(dbName, commands, ignoreNull.value_or(false));
  LOGI("HybridNitroSQLite::executeBatch: db=%s completed rowsAffected=%d", dbName.c_str(), result.rowsAffected);
  return BatchQueryResult(result.rowsAffected);
};

std::shared_ptr<Promise<BatchQueryResult>> HybridNitroSQLite::executeBatchAsync(const std::string& dbName,
                                                                                const std::vector<NativeBatchQueryCommand>& batchParams,
                                                                                std::optional<bool> ignoreNull) {
  logNativeBatchCommands("HybridNitroSQLite::executeBatchAsync scheduled", dbName, batchParams, ignoreNull);
  if (batchParams.empty()) {
    LOGW("HybridNitroSQLite::executeBatchAsync scheduled: db=%s received an empty native batch", dbName.c_str());
  }

  return Promise<BatchQueryResult>::async([=, this]() -> BatchQueryResult {
    LOGI("HybridNitroSQLite::executeBatchAsync running: db=%s nativeCommandCount=%zu", dbName.c_str(), batchParams.size());
    try {
      auto result = executeBatch(dbName, batchParams, ignoreNull);
      LOGI("HybridNitroSQLite::executeBatchAsync completed: db=%s", dbName.c_str());
      return result;
    } catch (const std::exception& error) {
      LOGE("HybridNitroSQLite::executeBatchAsync failed: db=%s nativeCommandCount=%zu error=%s", dbName.c_str(), batchParams.size(),
           error.what());
      throw;
    }
  });
};

FileLoadResult HybridNitroSQLite::loadFile(const std::string& dbName, const std::string& location) {
  const auto result = importSqlFile(dbName, location);
  return FileLoadResult(result.commands, result.rowsAffected);
};

std::shared_ptr<Promise<FileLoadResult>> HybridNitroSQLite::loadFileAsync(const std::string& dbName, const std::string& location) {
  return Promise<FileLoadResult>::async([=, this]() -> FileLoadResult {
    auto result = loadFile(dbName, location);
    return result;
  });
};

} // namespace margelo::nitro::rnnitrosqlite
