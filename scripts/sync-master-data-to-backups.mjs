#!/usr/bin/env node
// Neonのmaster_dataテーブル(スキーマ+データ全件)を、
// Supabase / ローカルPostgreSQLへ完全に複製するための手動実行スクリプト。
//
// 使い方: npm run sync:backup
//
// Neonを大元として、リスト追加・削除・精査・クローリングの変更があった都度、
// このコマンドを実行してSupabase(バックアップ)とローカルPostgreSQL(開発環境)を
// 最新のNeonの内容に揃える。

import { readFileSync, unlinkSync, existsSync } from "fs";
import { execFileSync } from "child_process";
import { tmpdir } from "os";
import { join } from "path";
import { Pool } from "pg";

const ENV_PATH = join(process.cwd(), ".env.local");

function getVar(name) {
  const envContent = readFileSync(ENV_PATH, "utf8");
  const m = envContent.match(new RegExp(`^${name}="([^"]*)"`, "m"));
  return m ? m[1] : null;
}

function withSslMode(connStr, mode) {
  const sep = connStr.includes("?") ? "&" : "?";
  return `${connStr}${sep}sslmode=${mode}`;
}

// このPCで見つかっているPostgreSQLインストール先。存在するものだけPATHに追加する。
const PG_BIN_CANDIDATES = [
  "/Library/PostgreSQL/18/bin",
  "/Library/PostgreSQL/16/bin",
  "/usr/local/opt/postgresql@16/bin",
];

function resolvedPath() {
  const extra = PG_BIN_CANDIDATES.filter((p) => existsSync(p)).join(":");
  return `${extra}:${process.env.PATH}`;
}

// psqlの標準エラー出力に、Neon固有ロールへのOWNER TO文由来のエラー以外が
// 含まれていないか確認する。それ以外のエラー(接続失敗・構文エラー等)が
// 混ざっていたら、無害と決めつけずに失敗として扱う。
function isOnlyBenignPsqlErrors(stderr) {
  const errorLines = stderr.split("\n").filter((line) => line.includes("ERROR:"));

  if (errorLines.length === 0) {
    return true;
  }

  return errorLines.every((line) => /role .* does not exist/.test(line));
}

async function countRows(connStr) {
  const pool = new Pool({
    connectionString: connStr,
    ssl: connStr.includes("neon.tech") ? { rejectUnauthorized: false } : undefined,
  });
  try {
    const res = await pool.query("SELECT COUNT(*) FROM public.master_data");
    return res.rows[0].count;
  } finally {
    await pool.end();
  }
}

async function main() {
  const neonUrl = getVar("DATABASE_URL_NEON");
  const supabaseUrl = getVar("DATABASE_URL_SUPABASE");
  const localUrl = getVar("DATABASE_URL_LOCAL");

  if (!neonUrl) {
    console.error("DATABASE_URL_NEON が .env.local に見つからない。処理を中止する。");
    process.exit(1);
  }

  const env = {
    ...process.env,
    PATH: resolvedPath(),
    PGSSLROOTCERT: "system",
  };

  const dumpPath = join(tmpdir(), `master_data_sync_${Date.now()}.sql`);

  console.log("[1/3] Neonから master_data をダンプ中...");
  execFileSync(
    "pg_dump",
    [neonUrl, "--table=public.master_data", "--clean", "--if-exists", "-f", dumpPath],
    { env }
  );

  const targets = [];
  if (supabaseUrl) targets.push(["Supabase", withSslMode(supabaseUrl, "require")]);
  if (localUrl) targets.push(["Local(PostgreSQL)", withSslMode(localUrl, "disable")]);

  if (targets.length === 0) {
    console.log("反映先(DATABASE_URL_SUPABASE / DATABASE_URL_LOCAL)が .env.local に見つからない。");
  }

  console.log("[2/3] 反映先へ書き込み中...");
  for (const [label, connStr] of targets) {
    try {
      execFileSync("psql", [connStr, "-v", "ON_ERROR_STOP=0", "-f", dumpPath], {
        env,
        stdio: ["ignore", "ignore", "pipe"],
      });
      console.log(`  - ${label}: 反映完了`);
    } catch (e) {
      const stderr = (e.stderr || "").toString();

      if (isOnlyBenignPsqlErrors(stderr)) {
        console.log(`  - ${label}: 反映完了(所有者変更関連の警告は無視して問題ない)`);
      } else {
        console.error(`  - ${label}: 反映失敗`);
        console.error(stderr.slice(0, 2000));
        throw new Error(`${label}への反映に失敗した。上記のエラー内容を確認してほしい。`);
      }
    }
  }

  unlinkSync(dumpPath);

  console.log("[3/3] 件数を確認中...");
  const neonCount = await countRows(neonUrl);
  console.log(`  - Neon: ${neonCount}件`);
  if (supabaseUrl) console.log(`  - Supabase: ${await countRows(supabaseUrl)}件`);
  if (localUrl) console.log(`  - Local(PostgreSQL): ${await countRows(localUrl)}件`);

  console.log("\n完了。");
}

main().catch((e) => {
  console.error("同期処理でエラーが発生した:", e.message);
  process.exit(1);
});
