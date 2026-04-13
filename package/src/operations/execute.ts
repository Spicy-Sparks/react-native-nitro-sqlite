import { HybridNitroSQLite } from '../nitro'
import type {
  ExecuteOptions,
  QueryResult,
  QueryResultRow,
  SQLiteQueryParams,
} from '../types'
import NitroSQLiteError from '../NitroSQLiteError'
import type { NitroSQLiteQueryResult } from '../specs/NitroSQLiteQueryResult.nitro'

export function execute<Row extends QueryResultRow = never>(
  dbName: string,
  query: string,
  params?: SQLiteQueryParams,
  options?: ExecuteOptions,
): QueryResult<Row> {
  try {
    const nativeResult = HybridNitroSQLite.execute(
      dbName,
      query,
      params,
      options?.ignoreNull,
    )
    return buildJSQueryResult<Row>(nativeResult)
  } catch (error) {
    throw NitroSQLiteError.fromError(error)
  }
}

export async function executeAsync<Row extends QueryResultRow = never>(
  dbName: string,
  query: string,
  params?: SQLiteQueryParams,
  options?: ExecuteOptions,
): Promise<QueryResult<Row>> {
  try {
    const nativeResult = await HybridNitroSQLite.executeAsync(
      dbName,
      query,
      params,
      options?.ignoreNull,
    )
    return buildJSQueryResult<Row>(nativeResult)
  } catch (error) {
    throw NitroSQLiteError.fromError(error)
  }
}

function buildJSQueryResult<Row extends QueryResultRow = never>(
  result: NitroSQLiteQueryResult,
): QueryResult<Row> {
  return Object.assign(result as QueryResult<Row>, {
    rows: {
      _array: result.results as Row[],
      length: result.results.length,
      item: (idx: number) => result.results[idx] as Row | undefined,
    },
  })
}
