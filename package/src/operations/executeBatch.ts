import {
  isSimpleNullHandlingEnabled,
  replaceWithNativeNullValue,
} from '../nullHandling'
import { HybridNitroSQLite } from '../nitro'
import {
  queueOperationAsync,
  startOperationSync,
  throwIfDatabaseIsNotOpen,
} from '../DatabaseQueue'
import type {
  NativeSQLiteQueryParams,
  BatchQueryResult,
  BatchQueryCommand,
  NativeBatchQueryCommand,
  ExecuteOptions,
} from '../types'
import NitroSQLiteError from '../NitroSQLiteError'

export function executeBatch(
  dbName: string,
  commands: BatchQueryCommand[],
  options?: ExecuteOptions,
): BatchQueryResult {
  throwIfDatabaseIsNotOpen(dbName)

  const transformedCommands = isSimpleNullHandlingEnabled()
    ? toNativeBatchQueryCommands(commands)
    : (commands as NativeBatchQueryCommand[])

  try {
    return startOperationSync(dbName, () =>
      HybridNitroSQLite.executeBatch(
        dbName,
        transformedCommands,
        options?.ignoreNull,
      ),
    )
  } catch (error) {
    throw NitroSQLiteError.fromError(error)
  }
}

export async function executeBatchAsync(
  dbName: string,
  commands: BatchQueryCommand[],
  options?: ExecuteOptions,
): Promise<BatchQueryResult> {
  throwIfDatabaseIsNotOpen(dbName)

  const transformedCommands = isSimpleNullHandlingEnabled()
    ? toNativeBatchQueryCommands(commands)
    : (commands as NativeBatchQueryCommand[])

  return queueOperationAsync(dbName, async () => {
    try {
      return await HybridNitroSQLite.executeBatchAsync(
        dbName,
        transformedCommands,
        options?.ignoreNull,
      )
    } catch (error) {
      throw NitroSQLiteError.fromError(error)
    }
  })
}

function toNativeBatchQueryCommands(
  commands: BatchQueryCommand[],
): NativeBatchQueryCommand[] {
  return commands.map((command) => {
    const transformedParams = command.params?.map((param) => {
      if (Array.isArray(param)) {
        return param.map((p) => replaceWithNativeNullValue(p))
      }
      return replaceWithNativeNullValue(param)
    }) as NativeSQLiteQueryParams | NativeSQLiteQueryParams[]

    return {
      query: command.query,
      params: transformedParams,
    }
  })
}
