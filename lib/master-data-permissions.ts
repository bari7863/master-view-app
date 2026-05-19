import { NextRequest, NextResponse } from "next/server";
import { dbReady, pool } from "@/lib/db";
import {
  MasterDataAuthUser,
  requireMasterDataUser,
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
  client: Queryable = pool
) {
  await dbReady;

  await client.query(`
    CREATE TABLE IF NOT EXISTS public.master_data_user_permissions (
      user_id text PRIMARY KEY,
      organization text NOT NULL,
      permissions jsonb NOT NULL DEFAULT '{}'::jsonb,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS public.master_data_user_scopes (
      user_id text PRIMARY KEY,
      organization text NOT NULL,
      allowed_filters jsonb NOT NULL DEFAULT '{}'::jsonb,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);
}

export async function getMasterDataUserPermissionSettings(
  userId: string,
  organization: string
): Promise<MasterDataPermissionSettings> {
  await ensureMasterDataPermissionTables();

  const permissionResult = await pool.query(
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

  const scopeResult = await pool.query(
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

export async function upsertMasterDataUserPermissionSettings({
  userId,
  organization,
  permissions,
  allowedFilters,
}: {
  userId: string;
  organization: string;
  permissions?: unknown;
  allowedFilters?: unknown;
}) {
  await ensureMasterDataPermissionTables();

  if (permissions !== undefined) {
    const normalizedPermissions = normalizeMasterDataPermissions(permissions);

    await pool.query(
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

    await pool.query(
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

  return getMasterDataUserPermissionSettings(userId, organization);
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

  if (result.user.role === "管理者") {
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
    result.user.organization
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