import { getMasterDataDbReady, getMasterDataPool } from "@/lib/db";
import type { MasterDataDbMode } from "@/lib/master-data-auth";

declare global {
  // eslint-disable-next-line no-var
  var masterDataCrmTablesReadyPromises:
    | Partial<Record<MasterDataDbMode, Promise<void>>>
    | undefined;
}

type Queryable = {
  query: (sql: string, params?: unknown[]) => Promise<{ rows: any[] }>;
};

// SFA/CRM拡張(ロードマップPhase1以降)向けの土台テーブル。
// master_data(企業マスタ)本体とはFK制約を貼らず、既存のクローリング系テーブル
// (master_data_crawl_preview等)と同じく row_id(bigint) でアプリ側から紐付ける。

export type MasterDataContact = {
  id: number;
  row_id: number;
  name: string | null;
  department: string | null;
  position: string | null;
  phone: string | null;
  email: string | null;
  memo: string | null;
  created_at: string;
  updated_at: string;
};

export type MasterDataActivity = {
  id: number;
  row_id: number;
  contact_id: number | null;
  activity_type: string | null;
  activity_at: string | null;
  content: string | null;
  created_by: string | null;
  created_at: string;
  updated_at: string;
};

export type MasterDataDeal = {
  id: number;
  row_id: number;
  deal_name: string | null;
  status: string | null;
  amount: string | null;
  probability: string | null;
  owner: string | null;
  expected_close_date: string | null;
  memo: string | null;
  created_at: string;
  updated_at: string;
};

export async function ensureMasterDataContactsTable(
  dbMode: MasterDataDbMode = "neon",
  client?: Queryable
) {
  const targetPool = client ?? getMasterDataPool(dbMode);

  await targetPool.query(`
    CREATE TABLE IF NOT EXISTS public.master_data_contacts (
      id BIGSERIAL PRIMARY KEY,
      row_id bigint NOT NULL,
      name text,
      department text,
      position text,
      phone text,
      email text,
      memo text,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await targetPool.query(`
    CREATE INDEX IF NOT EXISTS master_data_contacts_row_id_idx
    ON public.master_data_contacts (row_id)
  `);
}

export async function ensureMasterDataActivitiesTable(
  dbMode: MasterDataDbMode = "neon",
  client?: Queryable
) {
  const targetPool = client ?? getMasterDataPool(dbMode);

  await targetPool.query(`
    CREATE TABLE IF NOT EXISTS public.master_data_activities (
      id BIGSERIAL PRIMARY KEY,
      row_id bigint NOT NULL,
      contact_id bigint,
      activity_type text,
      activity_at timestamptz,
      content text,
      created_by text,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await targetPool.query(`
    CREATE INDEX IF NOT EXISTS master_data_activities_row_id_idx
    ON public.master_data_activities (row_id)
  `);

  await targetPool.query(`
    CREATE INDEX IF NOT EXISTS master_data_activities_contact_id_idx
    ON public.master_data_activities (contact_id)
  `);
}

export async function ensureMasterDataDealsTable(
  dbMode: MasterDataDbMode = "neon",
  client?: Queryable
) {
  const targetPool = client ?? getMasterDataPool(dbMode);

  await targetPool.query(`
    CREATE TABLE IF NOT EXISTS public.master_data_deals (
      id BIGSERIAL PRIMARY KEY,
      row_id bigint NOT NULL,
      deal_name text,
      status text,
      amount text,
      probability text,
      owner text,
      expected_close_date text,
      memo text,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await targetPool.query(`
    CREATE INDEX IF NOT EXISTS master_data_deals_row_id_idx
    ON public.master_data_deals (row_id)
  `);
}

export function ensureMasterDataCrmTables(dbMode: MasterDataDbMode = "neon") {
  if (!global.masterDataCrmTablesReadyPromises) {
    global.masterDataCrmTablesReadyPromises = {};
  }

  if (!global.masterDataCrmTablesReadyPromises[dbMode]) {
    global.masterDataCrmTablesReadyPromises[dbMode] = (async () => {
      await getMasterDataDbReady(dbMode);

      const targetPool = getMasterDataPool(dbMode);

      await ensureMasterDataContactsTable(dbMode, targetPool);
      await ensureMasterDataActivitiesTable(dbMode, targetPool);
      await ensureMasterDataDealsTable(dbMode, targetPool);
    })().catch((error) => {
      // 失敗した結果をキャッシュし続けると、以降このdbModeへの全リクエストが
      // プロセス再起動まで恒久的に失敗し続けるため、次回呼び出しで再試行できる
      // ようキャッシュを消してから再スローする。
      delete global.masterDataCrmTablesReadyPromises?.[dbMode];
      throw error;
    });
  }

  return global.masterDataCrmTablesReadyPromises[dbMode]!;
}
