#!/usr/bin/env node
// Neonのmaster_dataに問題があった場合、バックアップ(Supabase)の内容で
// Neonを復旧するための手動実行スクリプト。Neon本番への書き込みを伴うため、
// 実行時に確認プロンプトを挟む。
//
// 使い方: npm run sync:restore

import { readFileSync, unlinkSync, existsSync } from "fs";
import { execFileSync } from "child_process";
import { tmpdir } from "os";
import { join } from "path";
import { createInterface } from "readline/promises";
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

  if (!neonUrl || !supabaseUrl) {
    console.error("DATABASE_URL_NEON / DATABASE_URL_SUPABASE が .env.local に見つからない。処理を中止する。");
    process.exit(1);
  }

  const neonCount = await countRows(neonUrl);
  const supabaseCount = await countRows(supabaseUrl);

  console.log("");
  console.log("=== 警告: これはNeon(本番)への書き込みです ===");
  console.log(`現在のNeon: ${neonCount}件 → Supabaseの内容(${supabaseCount}件)で完全に上書きします。`);
  console.log("Neon側の現在のデータは失われます。");
  console.log("");

  const rl = createInterface({ input: process.stdin, output: process.stdout });
  const answer = await rl.question("本当に実行しますか？ 実行する場合は yes と入力してください: ");
  rl.close();

  if (answer.trim().toLowerCase() !== "yes") {
    console.log("中止した。");
    process.exit(0);
  }

  const env = {
    ...process.env,
    PATH: resolvedPath(),
    PGSSLROOTCERT: "system",
  };

  const dumpPath = join(tmpdir(), `master_data_restore_${Date.now()}.sql`);

  console.log("[1/2] Supabaseから master_data をダンプ中...");
  execFileSync(
    "pg_dump",
    [withSslMode(supabaseUrl, "require"), "--table=public.master_data", "--clean", "--if-exists", "-f", dumpPath],
    { env }
  );

  console.log("[2/2] Neonへ書き込み中...");
  try {
    execFileSync("psql", [neonUrl, "-v", "ON_ERROR_STOP=0", "-f", dumpPath], {
      env,
      stdio: ["ignore", "ignore", "pipe"],
    });
    console.log("  - Neon: 反映完了");
  } catch (e) {
    const stderr = (e.stderr || "").toString();

    if (isOnlyBenignPsqlErrors(stderr)) {
      console.log("  - Neon: 反映完了(所有者変更関連の警告は無視して問題ない)");
    } else {
      console.error("  - Neon: 反映失敗");
      console.error(stderr.slice(0, 2000));
      throw new Error("Neonへの反映に失敗した。上記のエラー内容を確認してほしい。");
    }
  }

  unlinkSync(dumpPath);

  console.log("\n--- 件数確認 ---");
  console.log(`  - Neon: ${await countRows(neonUrl)}件`);
  console.log(`  - Supabase: ${await countRows(supabaseUrl)}件`);
  console.log("\n完了。");
}

main().catch((e) => {
  console.error("復旧処理でエラーが発生した:", e.message);
  process.exit(1);
});
