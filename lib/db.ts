import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { Pool } from "pg";

declare global {
  // eslint-disable-next-line no-var
  var pgPool: Pool | undefined;
  // eslint-disable-next-line no-var
  var masterDataColumnInitPromise: Promise<void> | undefined;
}

const poolConfig = process.env.DATABASE_URL
  ? {
      connectionString: process.env.DATABASE_URL,
    }
  : {
      host: process.env.POSTGRES_HOST,
      port: Number(process.env.POSTGRES_PORT || 5432),
      user: process.env.POSTGRES_USER,
      password: process.env.POSTGRES_PASSWORD,
      database: process.env.POSTGRES_DB,
    };

if (!global.pgPool) {
  global.pgPool = new Pool(poolConfig);
}

export const pool = global.pgPool;

async function ensureMasterDataLoginHistoryTable() {
  await pool.query(`
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

export const dbReady =
  global.masterDataColumnInitPromise ||
  (async () => {
    const sqlFilePath = join(process.cwd(), "sql", "add_column.sql");
    const sql = await readFile(sqlFilePath, "utf8");
    const normalizedSql = sql.trim();

    if (normalizedSql) {
      await pool.query(normalizedSql);
    }

    await ensureMasterDataLoginHistoryTable();
  })();

global.masterDataColumnInitPromise = dbReady;