import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { Pool, type PoolConfig } from "pg";
import type { MasterDataDbMode } from "@/lib/master-data-auth";

declare global {
  // eslint-disable-next-line no-var
  var masterDataPools: Partial<Record<MasterDataDbMode, Pool>> | undefined;
  // eslint-disable-next-line no-var
  var masterDataDbReadyPromises:
    | Partial<Record<MasterDataDbMode, Promise<void>>>
    | undefined;
}

function createMasterDataPoolConfig(dbMode: MasterDataDbMode): PoolConfig {
  if (dbMode === "postgresql") {
    if (process.env.DATABASE_URL_SUPABASE?.trim()) {
      return {
        connectionString: process.env.DATABASE_URL_SUPABASE,
      };
    }

    return {
      host: process.env.POSTGRES_HOST,
      port: Number(process.env.POSTGRES_PORT || 5432),
      user: process.env.POSTGRES_USER,
      password: process.env.POSTGRES_PASSWORD,
      database: process.env.POSTGRES_DB,
    };
  }

  return {
    connectionString: process.env.DATABASE_URL_NEON || process.env.DATABASE_URL,
  };
}

function getMasterDataPoolByMode(dbMode: MasterDataDbMode) {
  if (!global.masterDataPools) {
    global.masterDataPools = {};
  }

  if (!global.masterDataPools[dbMode]) {
    global.masterDataPools[dbMode] = new Pool(createMasterDataPoolConfig(dbMode));
  }

  return global.masterDataPools[dbMode]!;
}

async function ensureMasterDataLoginHistoryTable(targetPool: Pool) {
  await targetPool.query(`
    CREATE TABLE IF NOT EXISTS master_data_login_history (
      id BIGSERIAL PRIMARY KEY,
      user_id TEXT NOT NULL,
      user_name TEXT NOT NULL,
      user_role TEXT NOT NULL,
      ip_address TEXT NOT NULL,
      browser TEXT NOT NULL,
      logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_master_data_login_history_user_logged_at
      ON master_data_login_history (user_id, logged_at DESC);
  `);
}

function createMasterDataDbReadyPromise(dbMode: MasterDataDbMode) {
  const targetPool = getMasterDataPoolByMode(dbMode);

  return (async () => {
    const sqlFilePath = join(process.cwd(), "sql", "add_column.sql");
    const sql = await readFile(sqlFilePath, "utf8");
    const normalizedSql = sql.trim();

    if (normalizedSql) {
      await targetPool.query(normalizedSql);
    }

    await ensureMasterDataLoginHistoryTable(targetPool);
  })();
}

export function getMasterDataPool(dbMode: MasterDataDbMode) {
  return getMasterDataPoolByMode(dbMode);
}

export function getMasterDataDbReady(dbMode: MasterDataDbMode) {
  if (!global.masterDataDbReadyPromises) {
    global.masterDataDbReadyPromises = {};
  }

  if (!global.masterDataDbReadyPromises[dbMode]) {
    global.masterDataDbReadyPromises[dbMode] =
      createMasterDataDbReadyPromise(dbMode);
  }

  return global.masterDataDbReadyPromises[dbMode]!;
}

export const pool = getMasterDataPool("neon");
export const dbReady = getMasterDataDbReady("neon");