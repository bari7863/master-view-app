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

export const pool = global.pgPool || new Pool(poolConfig);

export const dbReady =
  global.masterDataColumnInitPromise ||
  (async () => {
    const sqlFilePath = join(process.cwd(), "sql", "add_column.sql");
    const sql = await readFile(sqlFilePath, "utf8");
    const normalizedSql = sql.trim();

    if (!normalizedSql) return;

    await pool.query(normalizedSql);
  })();

global.masterDataColumnInitPromise = dbReady;

if (process.env.NODE_ENV !== "production") {
  global.pgPool = pool;
}