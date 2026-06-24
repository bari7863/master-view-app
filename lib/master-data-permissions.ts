import { NextRequest, NextResponse } from "next/server";
import { getMasterDataDbReady, getMasterDataPool } from "@/lib/db";
import {
  requireMasterDataUser,
  type MasterDataAuthUser,
  type MasterDataDbMode,
} from "@/lib/master-data-auth";

export const MASTER_DATA_PERMISSION_KEYS = [
  "search.companyName",
  "search.prefecture",
  "search.industry",
  "search.established",
  "search.capital",
  "search.employeeCount",
  "search.tag",
  "search.columnFilters",

  "list.add",
  "list.delete",
  "list.itemDelete",
  "list.dedupe",

  "csv.import",
  "csv.export",
  "csv.template",

  "inspection.crawl",
  "inspection.itemInspection",

  "inspection.crawlField.company",
  "inspection.crawlField.zipcode",
  "inspection.crawlField.address",
  "inspection.crawlField.website_url",
  "inspection.crawlField.form_url",
  "inspection.crawlField.phone",
  "inspection.crawlField.fax",
  "inspection.crawlField.email",
  "inspection.crawlField.established_date",
  "inspection.crawlField.representative_name",
  "inspection.crawlField.capital",
  "inspection.crawlField.employee_count",
  "inspection.crawlField.business_content",
  "inspection.crawlField.worker_dispatch_license",
  "inspection.crawlField.paid_job_placement_license",

  "inspection.itemInspectionField.company",
  "inspection.itemInspectionField.zipcode",
  "inspection.itemInspectionField.address",
  "inspection.itemInspectionField.big_industry",
  "inspection.itemInspectionField.small_industry",
  "inspection.itemInspectionField.company_kana",
  "inspection.itemInspectionField.summary",
  "inspection.itemInspectionField.website_url",
  "inspection.itemInspectionField.form_url",
  "inspection.itemInspectionField.phone",
  "inspection.itemInspectionField.fax",
  "inspection.itemInspectionField.email",
  "inspection.itemInspectionField.established_date",
  "inspection.itemInspectionField.representative_name",
  "inspection.itemInspectionField.representative_title",
  "inspection.itemInspectionField.capital",
  "inspection.itemInspectionField.employee_count",
  "inspection.itemInspectionField.employee_count_year",
  "inspection.itemInspectionField.previous_sales",
  "inspection.itemInspectionField.latest_sales",
  "inspection.itemInspectionField.closing_month",
  "inspection.itemInspectionField.office_count",
  "inspection.itemInspectionField.tag",
  "inspection.itemInspectionField.business_type",
  "inspection.itemInspectionField.business_content",
  "inspection.itemInspectionField.industry_category",
  "inspection.itemInspectionField.permit_number",
  "inspection.itemInspectionField.memo",
] as const;

export type MasterDataPermissionKey =
  (typeof MASTER_DATA_PERMISSION_KEYS)[number];

export type MasterDataPermissions = Record<MasterDataPermissionKey, boolean>;

export type MasterDataPermissionSettings = {
  permissions: MasterDataPermissions;
  allowedFilters: Record<string, unknown>;
};

type Queryable = {
  query: (sql: string, params?: unknown[]) => Promise<{ rows: any[] }>;
};

function createDefaultMasterDataPermissions(): MasterDataPermissions {
  return MASTER_DATA_PERMISSION_KEYS.reduce((acc, key) => {
    acc[key] = true;
    return acc;
  }, {} as MasterDataPermissions);
}

function toPlainObject(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  return value as Record<string, unknown>;
}

export function normalizeMasterDataPermissions(
  value: unknown
): MasterDataPermissions {
  const defaults = createDefaultMasterDataPermissions();
  const raw = toPlainObject(value);

  MASTER_DATA_PERMISSION_KEYS.forEach((key) => {
    if (typeof raw[key] === "boolean") {
      defaults[key] = raw[key] as boolean;
    }
  });

  return defaults;
}

export function normalizeMasterDataAllowedFilters(
  value: unknown
): Record<string, unknown> {
  return toPlainObject(value);
}

export async function ensureMasterDataPermissionTables(
  dbMode: MasterDataDbMode = "neon",
  client?: Queryable
) {
  const targetPool = client ?? getMasterDataPool(dbMode);

  await getMasterDataDbReady(dbMode);

  await targetPool.query(`
    CREATE TABLE IF NOT EXISTS public.master_data_user_permissions (
      user_id text,
      organization text NOT NULL DEFAULT '',
      permissions jsonb NOT NULL DEFAULT '{}'::jsonb,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await targetPool.query(`
    ALTER TABLE public.master_data_user_permissions
    ADD COLUMN IF NOT EXISTS user_id text
  `);

  await targetPool.query(`
    ALTER TABLE public.master_data_user_permissions
    ADD COLUMN IF NOT EXISTS organization text NOT NULL DEFAULT ''
  `);

  await targetPool.query(`
    ALTER TABLE public.master_data_user_permissions
    ADD COLUMN IF NOT EXISTS permissions jsonb NOT NULL DEFAULT '{}'::jsonb
  `);

  await targetPool.query(`
    ALTER TABLE public.master_data_user_permissions
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now()
  `);

  await targetPool.query(`
    ALTER TABLE public.master_data_user_permissions
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now()
  `);

  await targetPool.query(`
    DELETE FROM public.master_data_user_permissions
    WHERE user_id IS NULL OR btrim(user_id) = ''
  `);

  await targetPool.query(`
    WITH ranked_permissions AS (
      SELECT
        ctid,
        ROW_NUMBER() OVER (
          PARTITION BY user_id
          ORDER BY updated_at DESC NULLS LAST, ctid DESC
        ) AS row_number
      FROM public.master_data_user_permissions
    )
    DELETE FROM public.master_data_user_permissions
    WHERE ctid IN (
      SELECT ctid
      FROM ranked_permissions
      WHERE row_number > 1
    )
  `);

  await targetPool.query(`
    DO $$
    DECLARE
      primary_key_name text;
    BEGIN
      SELECT conname
      INTO primary_key_name
      FROM pg_constraint
      WHERE conrelid = 'public.master_data_user_permissions'::regclass
        AND contype = 'p'
      LIMIT 1;

      IF primary_key_name IS NOT NULL THEN
        EXECUTE format(
          'ALTER TABLE public.master_data_user_permissions DROP CONSTRAINT %I',
          primary_key_name
        );
      END IF;
    END $$;
  `);

  await targetPool.query(`
    ALTER TABLE public.master_data_user_permissions
    ALTER COLUMN user_id SET NOT NULL
  `);

  await targetPool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS master_data_user_permissions_user_id_idx
    ON public.master_data_user_permissions (user_id)
  `);

  await targetPool.query(`
    CREATE TABLE IF NOT EXISTS public.master_data_user_scopes (
      user_id text,
      organization text NOT NULL DEFAULT '',
      allowed_filters jsonb NOT NULL DEFAULT '{}'::jsonb,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await targetPool.query(`
    ALTER TABLE public.master_data_user_scopes
    ADD COLUMN IF NOT EXISTS user_id text
  `);

  await targetPool.query(`
    ALTER TABLE public.master_data_user_scopes
    ADD COLUMN IF NOT EXISTS organization text NOT NULL DEFAULT ''
  `);

  await targetPool.query(`
    ALTER TABLE public.master_data_user_scopes
    ADD COLUMN IF NOT EXISTS allowed_filters jsonb NOT NULL DEFAULT '{}'::jsonb
  `);

  await targetPool.query(`
    ALTER TABLE public.master_data_user_scopes
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now()
  `);

  await targetPool.query(`
    ALTER TABLE public.master_data_user_scopes
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now()
  `);

  await targetPool.query(`
    DELETE FROM public.master_data_user_scopes
    WHERE user_id IS NULL OR btrim(user_id) = ''
  `);

  await targetPool.query(`
    WITH ranked_scopes AS (
      SELECT
        ctid,
        ROW_NUMBER() OVER (
          PARTITION BY user_id
          ORDER BY updated_at DESC NULLS LAST, ctid DESC
        ) AS row_number
      FROM public.master_data_user_scopes
    )
    DELETE FROM public.master_data_user_scopes
    WHERE ctid IN (
      SELECT ctid
      FROM ranked_scopes
      WHERE row_number > 1
    )
  `);

  await targetPool.query(`
    DO $$
    DECLARE
      primary_key_name text;
    BEGIN
      SELECT conname
      INTO primary_key_name
      FROM pg_constraint
      WHERE conrelid = 'public.master_data_user_scopes'::regclass
        AND contype = 'p'
      LIMIT 1;

      IF primary_key_name IS NOT NULL THEN
        EXECUTE format(
          'ALTER TABLE public.master_data_user_scopes DROP CONSTRAINT %I',
          primary_key_name
        );
      END IF;
    END $$;
  `);

  await targetPool.query(`
    ALTER TABLE public.master_data_user_scopes
    ALTER COLUMN user_id SET NOT NULL
  `);

  await targetPool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS master_data_user_scopes_user_id_idx
    ON public.master_data_user_scopes (user_id)
  `);
}

export async function getMasterDataUserPermissionSettings(
  userId: string,
  organization: string,
  dbMode: MasterDataDbMode = "neon"
): Promise<MasterDataPermissionSettings> {
  await ensureMasterDataPermissionTables(dbMode);

  const targetPool = getMasterDataPool(dbMode);

  const permissionResult = await targetPool.query(
    `
      SELECT permissions
      FROM public.master_data_user_permissions
      WHERE user_id = $1
      ORDER BY
        CASE WHEN organization = $2 THEN 0 ELSE 1 END,
        updated_at DESC
      LIMIT 1
    `,
    [userId, organization]
  );

  const scopeResult = await targetPool.query(
    `
      SELECT allowed_filters
      FROM public.master_data_user_scopes
      WHERE user_id = $1
      ORDER BY
        CASE WHEN organization = $2 THEN 0 ELSE 1 END,
        updated_at DESC
      LIMIT 1
    `,
    [userId, organization]
  );

  return {
    permissions: normalizeMasterDataPermissions(
      permissionResult.rows[0]?.permissions
    ),
    allowedFilters: normalizeMasterDataAllowedFilters(
      scopeResult.rows[0]?.allowed_filters
    ),
  };
}

export async function getMasterDataUsersPermissionSettings(
  users: { id: string; organization: string }[],
  dbMode: MasterDataDbMode = "neon"
): Promise<Record<string, MasterDataPermissionSettings>> {
  const normalizedUsers = users
    .map((user) => ({
      id: String(user.id ?? "").trim(),
      organization: String(user.organization ?? "").trim(),
    }))
    .filter((user) => user.id !== "");

  const userIds = Array.from(new Set(normalizedUsers.map((user) => user.id)));

  if (userIds.length === 0) {
    return {};
  }

  await ensureMasterDataPermissionTables(dbMode);

  const targetPool = getMasterDataPool(dbMode);

  const permissionResult = await targetPool.query(
    `
      SELECT
        user_id,
        permissions
      FROM public.master_data_user_permissions
      WHERE user_id = ANY($1::text[])
    `,
    [userIds]
  );

  const scopeResult = await targetPool.query(
    `
      SELECT
        user_id,
        allowed_filters
      FROM public.master_data_user_scopes
      WHERE user_id = ANY($1::text[])
    `,
    [userIds]
  );

  const permissionMap = new Map<string, unknown>();
  const scopeMap = new Map<string, unknown>();

  permissionResult.rows.forEach((row) => {
    permissionMap.set(String(row.user_id ?? ""), row.permissions);
  });

  scopeResult.rows.forEach((row) => {
    scopeMap.set(String(row.user_id ?? ""), row.allowed_filters);
  });

  return normalizedUsers.reduce((acc, user) => {
    acc[user.id] = {
      permissions: normalizeMasterDataPermissions(permissionMap.get(user.id)),
      allowedFilters: normalizeMasterDataAllowedFilters(scopeMap.get(user.id)),
    };

    return acc;
  }, {} as Record<string, MasterDataPermissionSettings>);
}

export async function upsertMasterDataUserPermissionSettings({
  userId,
  organization,
  permissions,
  allowedFilters,
  dbMode = "neon",
}: {
  userId: string;
  organization: string;
  permissions?: unknown;
  allowedFilters?: unknown;
  dbMode?: MasterDataDbMode;
}) {
  await ensureMasterDataPermissionTables(dbMode);

  const targetPool = getMasterDataPool(dbMode);

  if (permissions !== undefined) {
    const normalizedPermissions = normalizeMasterDataPermissions(permissions);

    await targetPool.query(
      `
        INSERT INTO public.master_data_user_permissions (
          user_id,
          organization,
          permissions,
          updated_at
        )
        VALUES ($1, $2, $3::jsonb, now())
        ON CONFLICT (user_id)
        DO UPDATE SET
          organization = EXCLUDED.organization,
          permissions = EXCLUDED.permissions,
          updated_at = now()
      `,
      [userId, organization, JSON.stringify(normalizedPermissions)]
    );
  }

  if (allowedFilters !== undefined) {
    const normalizedAllowedFilters =
      normalizeMasterDataAllowedFilters(allowedFilters);

    await targetPool.query(
      `
        INSERT INTO public.master_data_user_scopes (
          user_id,
          organization,
          allowed_filters,
          updated_at
        )
        VALUES ($1, $2, $3::jsonb, now())
        ON CONFLICT (user_id)
        DO UPDATE SET
          organization = EXCLUDED.organization,
          allowed_filters = EXCLUDED.allowed_filters,
          updated_at = now()
      `,
      [userId, organization, JSON.stringify(normalizedAllowedFilters)]
    );
  }

  return getMasterDataUserPermissionSettings(userId, organization, dbMode);
}

export async function requireMasterDataPermission(
  req: NextRequest,
  permissionKey: MasterDataPermissionKey
): Promise<{
  user: MasterDataAuthUser | null;
  settings: MasterDataPermissionSettings | null;
  errorResponse: NextResponse | null;
}> {
  const result = requireMasterDataUser(req);

  if (result.errorResponse || !result.user) {
    return {
      user: null,
      settings: null,
      errorResponse: result.errorResponse,
    };
  }

  if (result.user.role === "スーパー管理者") {
    return {
      user: result.user,
      settings: {
        permissions: createDefaultMasterDataPermissions(),
        allowedFilters: {},
      },
      errorResponse: null,
    };
  }

  const settings = await getMasterDataUserPermissionSettings(
    result.user.id,
    result.user.organization,
    result.user.dbMode ?? "neon"
  );

  if (!settings.permissions[permissionKey]) {
    return {
      user: result.user,
      settings,
      errorResponse: NextResponse.json(
        { ok: false, error: "この操作を行う権限がありません" },
        { status: 403 }
      ),
    };
  }

  return {
    user: result.user,
    settings,
    errorResponse: null,
  };
}