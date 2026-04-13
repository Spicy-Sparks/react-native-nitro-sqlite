import { HybridNitroSQLite } from '../nitro'
import {
  queueOperationAsync,
  startOperationSync,
  throwIfDatabaseIsNotOpen,
} from '../DatabaseQueue'
import NitroSQLiteError from '../NitroSQLiteError'
import type { BatchQueryCommand, BatchQueryResult, ExecuteOptions } from '../types'

export function executeBatch(
  dbName: string,
  commands: BatchQueryCommand[],
  options?: ExecuteOptions,
): BatchQueryResult {
  throwIfDatabaseIsNotOpen(dbName)

  try {
    return startOperationSync(dbName, () =>
      HybridNitroSQLite.executeBatch(dbName, commands, options?.ignoreNull),
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

  return queueOperationAsync(dbName, async () => {
    try {
      return await HybridNitroSQLite.executeBatchAsync(
        dbName,
        commands,
        options?.ignoreNull,
      )
    } catch (error) {
      throw NitroSQLiteError.fromError(error)
    }
  })
}
