import { NextRequest, NextResponse } from "next/server";
export const dynamic = "force-dynamic";
export const fetchCache = "force-no-store";
export const runtime = "nodejs";
import { pool } from "@/lib/db";
import type { PoolClient } from "pg";
import {
  crawlCompanyWebsite,
  type CrawlExtractedFields,
  type CrawlExtractedOffice,
} from "@/lib/master-data-crawler";
import {
  refreshMasterDataAuthCookie,
  requireMasterDataAuth,
  requireMasterDataUser,
} from "@/lib/master-data-auth";
import {
  getMasterDataUserPermissionSettings,
  requireMasterDataPermission,
  type MasterDataPermissionKey,
} from "@/lib/master-data-permissions";

const FILTER_COLUMN_MAP = {
  company: `"企業名"`,
  zipcode: `"郵便番号"`,
  address: `"住所"`,
  big_industry: `"大業種名"`,
  small_industry: `"小業種名"`,
  company_kana: `"企業名（かな）"`,
  summary: `"企業概要"`,
  website_url: `"企業サイトURL"`,
  form_url: `"問い合わせフォームURL"`,
  phone: `"電話番号"`,
  fax: `"FAX番号"`,
  email: `"メールアドレス"`,
  established_date: `"設立年月"`,
  representative_name: `"代表者名"`,
  representative_title: `"代表者役職"`,
  capital: `"資本金"`,
  employee_count: `"従業員数"`,
  employee_count_year: `"従業員数年度"`,
  previous_sales: `"前年売上高"`,
  latest_sales: `"直近売上高"`,
  closing_month: `"決算月"`,
  office_count: `"事業所数"`,
  tag: `"新規登録タグ"`,
  business_type: `"業種"`,
  business_content: `"事業内容"`,
  industry_category: `"業界"`,
  permit_number: `"許可番号"`,
  memo: `"メモ"`,
} as const;

type FilterKey = keyof typeof FILTER_COLUMN_MAP;
type SortDirection = "asc" | "desc";
type ConditionType =
  | ""
  | "contains"
  | "not_contains"
  | "equals"
  | "not_equals"
  | "starts_with"
  | "ends_with"
  | "is_empty"
  | "is_not_empty";

type FilterModel = {
  sortDirection?: "" | SortDirection;
  conditionType?: ConditionType;
  conditionValue?: string;
  valueFilterEnabled?: boolean;
  selectedValues?: string[];
};

type AdvancedPrefectureFilters = {
  regions?: string[];
  prefectures?: string[];
  cities?: string[];
};

type AdvancedIndustryFilters = {
  bigIndustries?: string[];
  smallIndustries?: string[];
};

type AdvancedEstablishedFilters = {
  years?: string[];
  yearMonths?: string[];
  from?: string;
  to?: string;
};

type AdvancedRangeFilters = {
  min?: string | number;
  max?: string | number;
};

type AdvancedTagFilters = {
  parents?: string[];
  tags?: string[];
};

type AdvancedCompanyNameFilter = {
  keyword?: string;
};

type AdvancedFilters = {
  companyName?: AdvancedCompanyNameFilter;
  prefectures?: AdvancedPrefectureFilters;
  industries?: AdvancedIndustryFilters;
  established?: AdvancedEstablishedFilters;
  capital?: AdvancedRangeFilters;
  employeeCount?: AdvancedRangeFilters;
  tags?: AdvancedTagFilters;
};

type CrawlPayload = {
  company: string | null;
  website_url: string | null;
  form_url: string | null;
  phone: string | null;
  fax: string | null;
  email: string | null;
  zipcode: string | null;
  address: string | null;
  established_date: string | null;
  representative_name: string | null;
  representative_name_raw: string | null;
  representative_name_reason: string | null;
  representative_title: string | null;
  capital: string | null;
  employee_count: string | null;
  business_content: string | null;
  permit_number: string | null;
};

type CrawlSelectableFieldKey =
  | "company"
  | "zipcode"
  | "address"
  | "website_url"
  | "form_url"
  | "phone"
  | "fax"
  | "email"
  | "established_date"
  | "representative_name"
  | "capital"
  | "employee_count"
  | "business_content"
  | "worker_dispatch_license"
  | "paid_job_placement_license";

type CrawlPayloadCandidateKey = Extract<
  CrawlSelectableFieldKey,
  "phone" | "fax" | "email" | "zipcode" | "address"
>;

type CrawlPreviewSelectableKey =
  | Exclude<
      CrawlSelectableFieldKey,
      "worker_dispatch_license" | "paid_job_placement_license"
    >
  | "permit_number";

type CrawlPreviewChange = {
  key: CrawlPreviewSelectableKey;
  label: string;
  before: string | null;
  after: string | null;
  candidates: string[];
};

type PreviewSourceRow = {
  company: string | null;
  zipcode: string | null;
  address: string | null;
  big_industry: string | null;
  small_industry: string | null;
  company_kana: string | null;
  summary: string | null;
  website_url: string | null;
  form_url: string | null;
  phone: string | null;
  fax: string | null;
  email: string | null;
  established_date: string | null;
  representative_name: string | null;
  representative_title: string | null;
  capital: string | null;
  employee_count: string | null;
  employee_count_year: string | null;
  previous_sales: string | null;
  latest_sales: string | null;
  closing_month: string | null;
  office_count: string | null;
  tag: string | null;
  business_type: string | null;
  business_content: string | null;
  industry_category: string | null;
  permit_number: string | null;
  memo: string | null;
};

type CrawlPreviewRow = {
  row_id: string;
  preview_row_id: string;
  company: string | null;
  website_url: string | null;
  source_row: PreviewSourceRow | null;
  changes: CrawlPreviewChange[];
};

type SelectedCrawlChanges = Partial<
  Record<
    string,
    Partial<Record<CrawlPreviewSelectableKey, string | null>>
  >
>;

type WorkerCrawlTarget = {
  targetIndex: number;
  rowId: string;
  company: string | null;
  address: string | null;
  websiteUrl: string | null;
  selectedFields: CrawlSelectableFieldKey[];
};

type WorkerTargetResult = {
  targetIndex: number;
  targetStatus: "done" | "skipped" | "failed";
  statusReason?: string | null;
  extracted?: CrawlExtractedFields | null;
  targetStartedAt?: string | null;
  targetFinishedAt?: string | null;
};

type PermissionListScopeFilters = {
  filterModels?: Partial<Record<FilterKey, FilterModel>>;
  advancedFilters?: AdvancedFilters;
};

type CrawlRequestBody = {
  action?:
    | "start_preview_job"
    | "get_job_status"
    | "pause_job"
    | "resume_job"
    | "cancel_job"
    | "save_partial"
    | "worker_register"
    | "worker_heartbeat"
    | "worker_claim_job"
    | "worker_claim_target"
    | "worker_claim_targets"
    | "worker_report_target"
    | "worker_report_targets";
  jobId?: string | null;
  workerId?: string | null;
  workerName?: string | null;
  assignedWorkerId?: string | null;
  targetIndex?: number | null;
  targetLimit?: number | null;
  targetStatus?: "done" | "skipped" | "failed";
  statusReason?: string | null;
  extracted?: CrawlExtractedFields | null;
  targetStartedAt?: string | null;
  targetFinishedAt?: string | null;
  targetResults?: WorkerTargetResult[];
  filterModels?: Partial<Record<FilterKey, FilterModel>>;
  advancedFilters?: AdvancedFilters;
  sortKey?: FilterKey | null;
  sortDirection?: SortDirection | "" | null;
  previewOnly?: boolean;
  selectedChanges?: SelectedCrawlChanges;
  selectedFields?: CrawlSelectableFieldKey[];
  previewPage?: number;
  previewPageSize?: number;
  previewTab?: "candidate" | "multiple" | "excluded";
  deferPersist?: boolean;
};

type DbClient = PoolClient;

async function ensureMasterDataIdColumn(client: DbClient) {
  await client.query(`
    ALTER TABLE public.master_data
    ADD COLUMN IF NOT EXISTS id BIGSERIAL
  `);

  await client.query(`
    ALTER TABLE public.master_data
    ADD COLUMN IF NOT EXISTS "許可番号" text
  `);

  await client.query(`
    UPDATE public.master_data
    SET id = DEFAULT
    WHERE id IS NULL
  `);

  await client.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS master_data_id_idx
    ON public.master_data (id)
  `);
}

const PREFECTURE_TO_REGION = {
  北海道: "北海道",
  青森県: "東北",
  岩手県: "東北",
  宮城県: "東北",
  秋田県: "東北",
  山形県: "東北",
  福島県: "東北",
  茨城県: "関東",
  栃木県: "関東",
  群馬県: "関東",
  埼玉県: "関東",
  千葉県: "関東",
  東京都: "関東",
  神奈川県: "関東",
  新潟県: "中部",
  富山県: "中部",
  石川県: "中部",
  福井県: "中部",
  山梨県: "中部",
  長野県: "中部",
  岐阜県: "中部",
  静岡県: "中部",
  愛知県: "中部",
  三重県: "中部",
  滋賀県: "近畿",
  京都府: "近畿",
  大阪府: "近畿",
  兵庫県: "近畿",
  奈良県: "近畿",
  和歌山県: "近畿",
  鳥取県: "中国",
  島根県: "中国",
  岡山県: "中国",
  広島県: "中国",
  山口県: "中国",
  徳島県: "四国",
  香川県: "四国",
  愛媛県: "四国",
  高知県: "四国",
  福岡県: "九州",
  佐賀県: "九州",
  長崎県: "九州",
  熊本県: "九州",
  大分県: "九州",
  宮崎県: "九州",
  鹿児島県: "九州",
  沖縄県: "沖縄",
} as const;

const PREFECTURE_NAMES = Object.keys(PREFECTURE_TO_REGION) as Array<
  keyof typeof PREFECTURE_TO_REGION
>;

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

const PREFECTURE_REGEX = PREFECTURE_NAMES.map((name) =>
  escapeRegExp(name)
).join("|");

function createPrefectureExpression(addressText: string) {
  return `NULLIF((regexp_match(BTRIM(${addressText}), '(${PREFECTURE_REGEX})'))[1], '')`;
}

function createCityExpression(addressText: string) {
  return `NULLIF((regexp_match(BTRIM(${addressText}), '(?:${PREFECTURE_REGEX})([^0-9０-９ 　－-]+?(?:市|区|町|村))'))[1], '')`;
}

function createEstablishedYmExpression(textColumn: string) {
  const digitsOnly = `regexp_replace(${textColumn}, '[^0-9]', '', 'g')`;
  const yearPart = `substring(${textColumn} from '([12][0-9]{3})')`;
  const monthPart = `substring(${textColumn} from '[12][0-9]{3}[^0-9]*([0-9]{1,2})')`;

  return `CASE
    WHEN NULLIF(BTRIM(${textColumn}), '') IS NULL THEN NULL
    WHEN ${digitsOnly} ~ '^[0-9]{6,}$' THEN substring(${digitsOnly} from '^[0-9]{6}')
    WHEN ${yearPart} IS NOT NULL AND ${monthPart} IS NOT NULL
      THEN ${yearPart} || LPAD(${monthPart}, 2, '0')
    ELSE NULL
  END`;
}

function createCapitalNumericExpression(textColumn: string) {
  const trimmed = `BTRIM(${textColumn})`;
  const digitsOnly = `NULLIF(regexp_replace(${textColumn}, '[^0-9]', '', 'g'), '')`;

  return `CASE
    WHEN NULLIF(${trimmed}, '') IS NULL THEN NULL
    WHEN ${trimmed} ~ '^[+-]?(?:[0-9]+(?:[.][0-9]+)?|[.][0-9]+)(?:[eE][+-]?[0-9]+)?$'
      THEN (${trimmed})::numeric
    WHEN ${digitsOnly} IS NOT NULL
      THEN ${digitsOnly}::numeric
    ELSE NULL
  END`;
}

function createEmployeeNumericExpression(textColumn: string) {
  const noComma = `regexp_replace(${textColumn}, '[,，]', '', 'g')`;

  const consolidatedBefore = `NULLIF((regexp_match(${noComma}, '(?:連結|consolidated|CONSOLIDATED)[^0-9]{0,12}([0-9]+)[[:space:]]*(?:名|人)'))[1], '')`;
  const consolidatedAfter = `NULLIF((regexp_match(${noComma}, '([0-9]+)[[:space:]]*(?:名|人)[^0-9]{0,12}(?:連結|consolidated|CONSOLIDATED)'))[1], '')`;

  const standaloneBefore = `NULLIF((regexp_match(${noComma}, '(?:単体|単独|個別|individual|INDIVIDUAL|non-consolidated|NON-CONSOLIDATED|nonconsolidated|NONCONSOLIDATED)[^0-9]{0,12}([0-9]+)[[:space:]]*(?:名|人)'))[1], '')`;
  const standaloneAfter = `NULLIF((regexp_match(${noComma}, '([0-9]+)[[:space:]]*(?:名|人)[^0-9]{0,12}(?:単体|単独|個別|individual|INDIVIDUAL|non-consolidated|NON-CONSOLIDATED|nonconsolidated|NONCONSOLIDATED)'))[1], '')`;

  const employmentRegex = `(?:正社員|正職員|社員|職員|パート|アルバイト|契約社員|契約職員|派遣社員|派遣スタッフ|嘱託|嘱託社員|臨時社員|臨時職員|常勤|非常勤|フルタイム|短時間|再雇用|有期雇用|無期雇用|役員)[^0-9]{0,12}([0-9]+)[[:space:]]*(?:名|人)`;

  const employmentCount = `(SELECT COUNT(*) FROM regexp_matches(${noComma}, '${employmentRegex}', 'g') AS m)`;
  const employmentSum = `(SELECT SUM((m)[1]::numeric) FROM regexp_matches(${noComma}, '${employmentRegex}', 'g') AS m)`;

  const personMax = `(SELECT MAX((m)[1]::numeric) FROM regexp_matches(${noComma}, '([0-9]+)[[:space:]]*(?:名|人)', 'g') AS m)`;

  const pureDigits = `CASE WHEN ${noComma} ~ '^[0-9]+$' THEN ${noComma}::numeric ELSE NULL END`;

  return `CASE
    WHEN NULLIF(BTRIM(${textColumn}), '') IS NULL THEN NULL
    WHEN ${consolidatedBefore} IS NOT NULL THEN ${consolidatedBefore}::numeric
    WHEN ${consolidatedAfter} IS NOT NULL THEN ${consolidatedAfter}::numeric
    WHEN ${employmentCount} >= 2 THEN ${employmentSum}
    WHEN ${standaloneBefore} IS NOT NULL THEN ${standaloneBefore}::numeric
    WHEN ${standaloneAfter} IS NOT NULL THEN ${standaloneAfter}::numeric
    WHEN ${personMax} IS NOT NULL THEN ${personMax}
    ELSE ${pureDigits}
  END`;
}

function createTagParentCase(tagExpression: string) {
  const trimmed = `BTRIM(${tagExpression})`;

  return `CASE
    WHEN ${trimmed} ~ '(マイナビ|リクナビ|doda|DODA|Indeed|indeed|エン転職|en転職|type|Wantedly|求人ボックス|スタンバイ|バイトル|ジョブドラフト|キャリタス|あさがく|ワンキャリア|ONE CAREER|新卒|転職)'
      THEN '採用媒体'
    WHEN ${trimmed} ~ '(ハローワーク|高校|大学|専門学校|学校|自治体|市役所|県庁)'
      THEN '教育・行政'
    WHEN ${trimmed} ~ '(製造|建設|物流|運送|介護|福祉|IT|営業|事務|サービス)'
      THEN '業種・職種'
    WHEN ${trimmed} ~ '(北海道|東北|関東|中部|近畿|中国|四国|九州|沖縄|都|道|府|県)'
      THEN '地域'
    ELSE 'その他'
  END`;
}

const ADDRESS_TEXT = `COALESCE("住所"::text, '')`;
const PREFECTURE_EXPR = createPrefectureExpression(ADDRESS_TEXT);
const CITY_EXPR = createCityExpression(ADDRESS_TEXT);

const BIG_INDUSTRY_TEXT = `COALESCE("大業種名"::text, '')`;
const SMALL_INDUSTRY_TEXT = `COALESCE("小業種名"::text, '')`;

const ESTABLISHED_TEXT = `COALESCE("設立年月"::text, '')`;
const ESTABLISHED_YM_EXPR = createEstablishedYmExpression(ESTABLISHED_TEXT);

const CAPITAL_TEXT = `COALESCE("資本金"::text, '')`;
const CAPITAL_NUMERIC_EXPR = createCapitalNumericExpression(CAPITAL_TEXT);

const EMPLOYEE_TEXT = `COALESCE("従業員数"::text, '')`;
const EMPLOYEE_NUMERIC_EXPR = createEmployeeNumericExpression(EMPLOYEE_TEXT);

const TAG_PARENT_CASE_FROM_SPLIT = createTagParentCase("tag_value");

function toStringArray(value: unknown) {
  if (!Array.isArray(value)) return [];

  return Array.from(
    new Set(
      value
        .map((item) => String(item ?? "").trim())
        .filter((item) => item !== "")
    )
  );
}

function toNumberOrNull(value: unknown) {
  if (value == null) return null;

  const text = String(value).replace(/,/g, "").trim();
  if (text === "") return null;

  const num = Number(text);
  return Number.isFinite(num) ? num : null;
}

function buildInClause(
  params: unknown[],
  expression: string,
  values: string[]
) {
  const normalized = toStringArray(values);
  if (normalized.length === 0) return "";

  params.push(normalized);
  return `${expression} = ANY($${params.length}::text[])`;
}

function addNumericRangeClause(
  where: string[],
  params: unknown[],
  expression: string,
  range?: AdvancedRangeFilters
) {
  const min = toNumberOrNull(range?.min);
  const max = toNumberOrNull(range?.max);

  if (min != null) {
    params.push(min);
    where.push(`${expression} >= $${params.length}`);
  }

  if (max != null) {
    params.push(max);
    where.push(`${expression} <= $${params.length}`);
  }
}

function getPrefecturesFromRegions(regions: string[]) {
  const regionSet = new Set(toStringArray(regions));

  return PREFECTURE_NAMES.filter((prefecture) =>
    regionSet.has(PREFECTURE_TO_REGION[prefecture])
  ) as string[];
}

function addAdvancedFilterClauses(
  where: string[],
  params: unknown[],
  filters: AdvancedFilters
) {
  const companyKeyword = String(filters.companyName?.keyword ?? "").trim();
  if (companyKeyword !== "") {
    params.push(`%${companyKeyword}%`);
    where.push(`COALESCE("企業名"::text, '') ILIKE $${params.length}`);
  }

  const locationPieces = [
    buildInClause(
      params,
      PREFECTURE_EXPR,
      getPrefecturesFromRegions(filters.prefectures?.regions ?? [])
    ),
    buildInClause(
      params,
      PREFECTURE_EXPR,
      toStringArray(filters.prefectures?.prefectures ?? [])
    ),
    buildInClause(
      params,
      CITY_EXPR,
      toStringArray(filters.prefectures?.cities ?? [])
    ),
  ].filter((piece) => piece !== "");

  if (locationPieces.length > 0) {
    where.push(`(${locationPieces.join(" OR ")})`);
  }

  const industryPieces = [
    buildInClause(
      params,
      BIG_INDUSTRY_TEXT,
      toStringArray(filters.industries?.bigIndustries ?? [])
    ),
    buildInClause(
      params,
      SMALL_INDUSTRY_TEXT,
      toStringArray(filters.industries?.smallIndustries ?? [])
    ),
  ].filter((piece) => piece !== "");

  if (industryPieces.length > 0) {
    where.push(`(${industryPieces.join(" OR ")})`);
  }

  const establishedPieces = [
    buildInClause(
      params,
      `substring(${ESTABLISHED_YM_EXPR} from 1 for 4)`,
      toStringArray(filters.established?.years ?? [])
    ),
    buildInClause(
      params,
      ESTABLISHED_YM_EXPR,
      toStringArray(filters.established?.yearMonths ?? [])
    ),
  ].filter((piece) => piece !== "");

  if (establishedPieces.length > 0) {
    where.push(`(${establishedPieces.join(" OR ")})`);
  }

  const establishedFrom = String(filters.established?.from ?? "").trim();
  const establishedTo = String(filters.established?.to ?? "").trim();

  if (establishedFrom !== "") {
    params.push(establishedFrom);
    where.push(`${ESTABLISHED_YM_EXPR} >= $${params.length}`);
  }

  if (establishedTo !== "") {
    params.push(establishedTo);
    where.push(`${ESTABLISHED_YM_EXPR} <= $${params.length}`);
  }

  addNumericRangeClause(where, params, CAPITAL_NUMERIC_EXPR, filters.capital);
  addNumericRangeClause(where, params, EMPLOYEE_NUMERIC_EXPR, filters.employeeCount);

  const selectedTagParents = toStringArray(filters.tags?.parents ?? []);
  const selectedTags = toStringArray(filters.tags?.tags ?? []);

  if (selectedTagParents.length > 0 || selectedTags.length > 0) {
    const tagConditions: string[] = [];

    if (selectedTags.length > 0) {
      params.push(selectedTags);
      tagConditions.push(`BTRIM(tag_value) = ANY($${params.length}::text[])`);
    }

    if (selectedTagParents.length > 0) {
      params.push(selectedTagParents);
      tagConditions.push(
        `${TAG_PARENT_CASE_FROM_SPLIT} = ANY($${params.length}::text[])`
      );
    }

    where.push(`EXISTS (
      SELECT 1
      FROM regexp_split_to_table(COALESCE("新規登録タグ"::text, ''), E'\\s*;\\s*') AS tag_value
      WHERE NULLIF(BTRIM(tag_value), '') IS NOT NULL
        AND (${tagConditions.join(" OR ")})
    )`);
  }
}

function addConditionClause(
  where: string[],
  params: unknown[],
  column: string,
  model?: FilterModel
) {
  if (!model) return;

  const type = model.conditionType || "";
  const value = (model.conditionValue || "").trim();
  const textColumn = `COALESCE(${column}::text, '')`;

  if (type === "") return;

  if (type === "is_empty") {
    where.push(`NULLIF(BTRIM(${textColumn}), '') IS NULL`);
    return;
  }

  if (type === "is_not_empty") {
    where.push(`NULLIF(BTRIM(${textColumn}), '') IS NOT NULL`);
    return;
  }

  if (value === "") return;

  if (type === "contains") {
    params.push(`%${value}%`);
    where.push(`${textColumn} ILIKE $${params.length}`);
    return;
  }

  if (type === "not_contains") {
    params.push(`%${value}%`);
    where.push(`${textColumn} NOT ILIKE $${params.length}`);
    return;
  }

  if (type === "equals") {
    params.push(value);
    where.push(`${textColumn} = $${params.length}`);
    return;
  }

  if (type === "not_equals") {
    params.push(value);
    where.push(`${textColumn} <> $${params.length}`);
    return;
  }

  if (type === "starts_with") {
    params.push(`${value}%`);
    where.push(`${textColumn} ILIKE $${params.length}`);
    return;
  }

  if (type === "ends_with") {
    params.push(`%${value}`);
    where.push(`${textColumn} ILIKE $${params.length}`);
  }
}

function addValueFilterClause(
  where: string[],
  params: unknown[],
  column: string,
  model?: FilterModel
) {
  if (!model?.valueFilterEnabled) return;

  const selectedValues = Array.isArray(model.selectedValues)
    ? model.selectedValues.map((value) => String(value ?? ""))
    : [];

  if (selectedValues.length === 0) {
    where.push("1 = 0");
    return;
  }

  const textColumn = `COALESCE(${column}::text, '')`;
  const normalValues = Array.from(
    new Set(selectedValues.filter((value) => value !== ""))
  );
  const includeEmpty = selectedValues.includes("");

  const pieces: string[] = [];

  if (normalValues.length > 0) {
    params.push(normalValues);
    pieces.push(`${textColumn} = ANY($${params.length}::text[])`);
  }

  if (includeEmpty) {
    pieces.push(`NULLIF(BTRIM(${textColumn}), '') IS NULL`);
  }

  if (pieces.length > 0) {
    where.push(`(${pieces.join(" OR ")})`);
  }
}

function buildWhereClause(
  filterModels: Partial<Record<FilterKey, FilterModel>>,
  advancedFilters: AdvancedFilters
) {
  const where: string[] = [];
  const params: unknown[] = [];

  (Object.entries(FILTER_COLUMN_MAP) as [FilterKey, string][]).forEach(
    ([key, column]) => {
      const model = filterModels[key];
      addConditionClause(where, params, column, model);
      addValueFilterClause(where, params, column, model);
    }
  );

  addAdvancedFilterClauses(where, params, advancedFilters);

  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  return { whereSql, params };
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function parsePermissionListScopeFilters(
  allowedFilters: Record<string, unknown> | undefined
): PermissionListScopeFilters | null {
  const listScopeFilters = allowedFilters?.listScopeFilters;

  if (!isObjectRecord(listScopeFilters)) {
    return null;
  }

  return {
    filterModels: isObjectRecord(listScopeFilters.filterModels)
      ? (listScopeFilters.filterModels as Partial<Record<FilterKey, FilterModel>>)
      : {},
    advancedFilters: isObjectRecord(listScopeFilters.advancedFilters)
      ? (listScopeFilters.advancedFilters as AdvancedFilters)
      : {},
  };
}

function shiftSqlParams(sql: string, offset: number) {
  if (offset <= 0) return sql;
  return sql.replace(/\$(\d+)/g, (_, index) => `$${Number(index) + offset}`);
}

function mergeWhereClauses(
  baseWhereSql: string,
  baseParams: unknown[],
  scopeWhereSql: string,
  scopeParams: unknown[]
) {
  if (!scopeWhereSql) {
    return {
      whereSql: baseWhereSql,
      params: baseParams,
    };
  }

  const scopeCondition = shiftSqlParams(scopeWhereSql, baseParams.length)
    .replace(/^WHERE\s+/i, "")
    .trim();

  return {
    whereSql: baseWhereSql
      ? `${baseWhereSql} AND (${scopeCondition})`
      : `WHERE (${scopeCondition})`,
    params: [...baseParams, ...scopeParams],
  };
}

function buildWhereClauseWithListScope(
  filterModels: Partial<Record<FilterKey, FilterModel>>,
  advancedFilters: AdvancedFilters,
  listScopeFilters: PermissionListScopeFilters | null
) {
  const base = buildWhereClause(filterModels, advancedFilters);

  if (!listScopeFilters) {
    return base;
  }

  const scope = buildWhereClause(
    listScopeFilters.filterModels ?? {},
    listScopeFilters.advancedFilters ?? {}
  );

  return mergeWhereClauses(
    base.whereSql,
    base.params,
    scope.whereSql,
    scope.params
  );
}

async function getListScopeFiltersForRequest(req: NextRequest) {
  const { user } = requireMasterDataUser(req);

  if (!user || user.role === "管理者") {
    return null;
  }

  const settings = await getMasterDataUserPermissionSettings(
    user.id,
    user.organization
  );

  return parsePermissionListScopeFilters(settings.allowedFilters);
}

function buildOrderBy(
  sortKey?: FilterKey | null,
  sortDirection?: SortDirection | "" | null
) {
  if (
    sortKey &&
    sortDirection &&
    FILTER_COLUMN_MAP[sortKey] &&
    (sortDirection === "asc" || sortDirection === "desc")
  ) {
    const sortColumnText = `COALESCE(${FILTER_COLUMN_MAP[sortKey]}::text, '')`;
    return `ORDER BY ${sortColumnText} ${sortDirection.toUpperCase()}, COALESCE("企業名"::text, ''), COALESCE("住所"::text, '')`;
  }

  return `ORDER BY COALESCE("企業名"::text, ''), COALESCE("住所"::text, '')`;
}

const MASTER_DATA_COLUMNS = [
  "企業名",
  "郵便番号",
  "住所",
  "大業種名",
  "小業種名",
  "企業名（かな）",
  "企業概要",
  "事業内容",
  "企業サイトURL",
  "問い合わせフォームURL",
  "電話番号",
  "FAX番号",
  "メールアドレス",
  "設立年月",
  "代表者名",
  "代表者役職",
  "資本金",
  "従業員数",
  "従業員数年度",
  "前年売上高",
  "直近売上高",
  "決算月",
  "事業所数",
  "新規登録タグ",
  "削除タグ",
  "削除フラグ",
  "強制フラグ",
  "業種",
  "業界",
  "許可番号",
  "メモ",
] as const;

const CRAWL_CANDIDATE_FIELDS: Array<{
  key: CrawlPayloadCandidateKey;
  label: string;
}> = [
  { key: "phone", label: "電話番号" },
  { key: "fax", label: "FAX番号" },
  { key: "email", label: "メールアドレス" },
  { key: "zipcode", label: "郵便番号" },
  { key: "address", label: "住所" },
];

const CRAWL_SINGLE_VALUE_FIELDS: Array<{
  key: Exclude<CrawlPreviewSelectableKey, CrawlPayloadCandidateKey>;
  label: string;
}> = [
  { key: "company", label: "企業名" },
  { key: "website_url", label: "企業URL" },
  { key: "form_url", label: "お問い合わせフォームURL" },
  { key: "established_date", label: "設立年月" },
  { key: "representative_name", label: "代表者名" },
  { key: "capital", label: "資本金" },
  { key: "employee_count", label: "従業員数" },
  { key: "business_content", label: "事業内容" },
  { key: "permit_number", label: "許可番号" },
];

const CRAWL_SELECTABLE_FIELDS: CrawlSelectableFieldKey[] = [
  "company",
  "zipcode",
  "address",
  "website_url",
  "form_url",
  "phone",
  "fax",
  "email",
  "established_date",
  "representative_name",
  "capital",
  "employee_count",
  "business_content",
  "worker_dispatch_license",
  "paid_job_placement_license",
];

function normalizeSelectedFields(selectedFields?: CrawlSelectableFieldKey[]) {
  const requested = Array.isArray(selectedFields)
    ? selectedFields.filter((field): field is CrawlSelectableFieldKey =>
        CRAWL_SELECTABLE_FIELDS.includes(field)
      )
    : [];

  const normalizedFields =
    requested.length > 0 ? requested : CRAWL_SELECTABLE_FIELDS;

  return new Set<CrawlSelectableFieldKey>(normalizedFields);
}

type CrawlPayloadBundle = {
  payload: CrawlPayload;
  candidates: Record<CrawlPayloadCandidateKey, string[]>;
  officeLabelMap?: Partial<Record<CrawlPayloadCandidateKey, Record<string, string>>>;
  forceCompanyUpdate: boolean;
};

function normalizeNullableText(value: unknown) {
  if (value == null) return null;
  const text = String(value).trim();
  return text === "" ? null : text;
}

function normalizeDateIsoText(value: unknown) {
  if (value == null) return null;

  if (value instanceof Date) {
    return value.toISOString();
  }

  const text = normalizeNullableText(value);
  if (!text) return null;

  const timestamp = Date.parse(text);
  if (!Number.isFinite(timestamp)) return null;

  return new Date(timestamp).toISOString();
}

function isLikelyCompanyName(value: string | null) {
  if (!value) return false;

  return !/^(top|home|ホーム|トップページ|latest news|最新情報|news|recruit|contact)$/i.test(
    value
  );
}

function uniqueTextValues(values: Array<string | null | undefined>) {
  const seen = new Set<string>();
  const result: string[] = [];

  for (const value of values) {
    const normalized = normalizeNullableText(value);
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    result.push(normalized);
  }

  return result;
}

function splitCrawlContactNumberCandidates(value: string | null | undefined) {
  const text = normalizeNullableText(value);
  if (!text) return [];

  const separatedValues = text
    .split(/\r?\n|[;；]/)
    .map((item) => item.trim())
    .filter((item) => item !== "");

  if (separatedValues.length > 1) {
    return uniqueTextValues(separatedValues);
  }

  const numberMatches =
    text.match(
      /(?:[^。\n\r;；、,]*?[：:]\s*)?(?:\+?81[-－ー―\s]?)?0[0-9０-９]{1,4}[-－ー―\s]?[0-9０-９]{1,4}[-－ー―\s]?[0-9０-９]{3,4}/g
    ) ?? [];

  if (numberMatches.length > 1) {
    return uniqueTextValues(numberMatches);
  }

  return uniqueTextValues([text]);
}

function getExtractedContactCandidateList(
  extracted: CrawlExtractedFields,
  key: "phone" | "fax"
) {
  const extended = extracted as CrawlExtractedFields & {
    phone_candidates?: unknown;
    fax_candidates?: unknown;
  };

  const directCandidates =
    key === "phone" && Array.isArray(extended.phone_candidates)
      ? extended.phone_candidates
      : key === "fax" && Array.isArray(extended.fax_candidates)
      ? extended.fax_candidates
      : [];

  const fallbackCandidates = splitCrawlContactNumberCandidates(
    key === "phone" ? extracted.phone : extracted.fax
  );

  return uniqueTextValues([
    ...directCandidates.map((value) => String(value ?? "")),
    ...fallbackCandidates,
  ]);
}

function normalizeContactNumberForCompare(value: string | null | undefined) {
  const text = normalizeNullableText(value);
  if (!text) return "";

  const digits = text.normalize("NFKC").replace(/[^0-9]/g, "");

  if (digits.startsWith("81") && digits.length >= 11) {
    return `0${digits.slice(2)}`;
  }

  return digits;
}

function hasSameContactNumberCandidate(values: string[], target: string) {
  const normalizedTarget = normalizeContactNumberForCompare(target);
  if (!normalizedTarget) return false;

  return values.some(
    (value) => normalizeContactNumberForCompare(value) === normalizedTarget
  );
}

function normalizeOfficeMatchText(value: string | null | undefined) {
  return normalizeNullableText(value)
    ?.normalize("NFKC")
    .replace(/\s+/g, "")
    .replace(/[‐-‒–—―ー－-]/g, "")
    .replace(/[（）()[\]【】「」『』]/g, "")
    .toLowerCase() ?? "";
}

function removeCorporateWordsForOfficeMatch(value: string) {
  return value
    .replace(/株式会社|有限会社|合同会社|合資会社|合名会社|㈱|（株）|\(株\)/g, "")
    .trim();
}

function scoreOfficeSourceMatch(
  office: CrawlExtractedOffice,
  sourceCompany: string | null,
  sourceAddress: string | null
) {
  const sourceCompanyText = removeCorporateWordsForOfficeMatch(
    normalizeOfficeMatchText(sourceCompany)
  );
  const sourceAddressText = normalizeOfficeMatchText(sourceAddress);

  const officeNameText = normalizeOfficeMatchText(office.office_name);
  const officeCompanyText = removeCorporateWordsForOfficeMatch(
    normalizeOfficeMatchText(office.company)
  );

  const officeAddressTexts = uniqueTextValues(office.address_candidates).map(
    (address) => normalizeOfficeMatchText(address)
  );

  let score = 0;

  if (officeNameText && sourceCompanyText.includes(officeNameText)) {
    score += 1000;
  }

  if (officeNameText && sourceAddressText.includes(officeNameText)) {
    score += 400;
  }

  if (
    officeCompanyText &&
    sourceCompanyText &&
    (sourceCompanyText.includes(officeCompanyText) ||
      officeCompanyText.includes(sourceCompanyText))
  ) {
    score += 120;
  }

  for (const officeAddressText of officeAddressTexts) {
    if (!officeAddressText || !sourceAddressText) continue;

    if (
      sourceAddressText === officeAddressText ||
      sourceAddressText.includes(officeAddressText) ||
      officeAddressText.includes(sourceAddressText)
    ) {
      score += 2000;
      continue;
    }

    const commonLength = Math.min(sourceAddressText.length, officeAddressText.length);
    let matchedLength = 0;

    for (let i = 0; i < commonLength; i += 1) {
      if (sourceAddressText[i] !== officeAddressText[i]) break;
      matchedLength += 1;
    }

    if (matchedLength >= 8) {
      score += matchedLength * 10;
    }
  }

  return score;
}

function getOfficeDisplayLabel(office: CrawlExtractedOffice) {
  return (
    normalizeNullableText(office.office_name) ??
    normalizeNullableText(office.address_candidates[0]) ??
    normalizeNullableText(office.company)
  );
}

function buildOfficeLabeledCandidates(
  offices: CrawlExtractedOffice[],
  key: "phone_candidates" | "fax_candidates"
) {
  const values: string[] = [];
  const labelMap: Record<string, string> = {};
  const seenNumbers = new Set<string>();

  for (const office of offices) {
    const officeLabel = getOfficeDisplayLabel(office);
    const candidates = uniqueTextValues(office[key]);

    for (const candidate of candidates) {
      const normalizedNumber = normalizeContactNumberForCompare(candidate);

      if (normalizedNumber && seenNumbers.has(normalizedNumber)) {
        continue;
      }

      if (normalizedNumber) {
        seenNumbers.add(normalizedNumber);
      }

      const displayValue = officeLabel ? `${officeLabel}：${candidate}` : candidate;

      if (values.includes(displayValue)) {
        continue;
      }

      values.push(displayValue);
      labelMap[displayValue] = candidate;
    }
  }

  return {
    values,
    labelMap,
  };
}

function resolveOfficeLabeledCandidateValue(
  value: string | null | undefined,
  labelMap?: Record<string, string>
) {
  const normalized = normalizeNullableText(value);
  if (!normalized) return null;

  return normalizeNullableText(labelMap?.[normalized]) ?? normalized;
}

function getResolvedValue(current: unknown, next: string | null) {
  const currentValue = normalizeNullableText(current);
  const nextValue = normalizeNullableText(next);
  return nextValue ?? currentValue;
}

function hasPermitSelection(selectedFieldSet: Set<CrawlSelectableFieldKey>) {
  return (
    selectedFieldSet.has("worker_dispatch_license") ||
    selectedFieldSet.has("paid_job_placement_license")
  );
}

function buildCrawlPayloadBundles(
  extracted: CrawlExtractedFields,
  sourceCompany: string | null,
  sourceAddress: string | null
): CrawlPayloadBundle[] {
  const extractedCompany = normalizeNullableText(extracted.company);
  const fallbackCompany = normalizeNullableText(sourceCompany);
  const baseCompany =
    extractedCompany && isLikelyCompanyName(extractedCompany)
      ? extractedCompany
      : fallbackCompany;

  const fallbackPhoneCandidates = getExtractedContactCandidateList(
    extracted,
    "phone"
  );
  const fallbackFaxCandidates = getExtractedContactCandidateList(
    extracted,
    "fax"
  );

  const officeSources: CrawlExtractedOffice[] =
    Array.isArray(extracted.offices) && extracted.offices.length > 0
      ? extracted.offices
      : [
          {
            office_name: null,
            company: baseCompany,
            phone_candidates: fallbackPhoneCandidates,
            fax_candidates: fallbackFaxCandidates,
            email_candidates: extracted.email ? [extracted.email] : [],
            zipcode_candidates: extracted.zipcode ? [extracted.zipcode] : [],
            address_candidates: extracted.address ? [extracted.address] : [],
          },
        ];
  
  const existingPhoneCandidates = uniqueTextValues(
    officeSources.flatMap((office) => office.phone_candidates)
  );

  const existingFaxCandidates = uniqueTextValues(
    officeSources.flatMap((office) => office.fax_candidates)
  );

  const missingPhoneCandidates = fallbackPhoneCandidates.filter(
    (candidate) => !hasSameContactNumberCandidate(existingPhoneCandidates, candidate)
  );

  const missingFaxCandidates = fallbackFaxCandidates.filter(
    (candidate) => !hasSameContactNumberCandidate(existingFaxCandidates, candidate)
  );

  if (missingPhoneCandidates.length > 0 || missingFaxCandidates.length > 0) {
    officeSources.push({
      office_name: "電話番号候補",
      company: baseCompany,
      phone_candidates: missingPhoneCandidates,
      fax_candidates: missingFaxCandidates,
      email_candidates: [],
      zipcode_candidates: [],
      address_candidates: [],
    });
  }

  const sortedOfficeSources = [...officeSources].sort((a, b) => {
    const scoreA = scoreOfficeSourceMatch(a, sourceCompany, sourceAddress);
    const scoreB = scoreOfficeSourceMatch(b, sourceCompany, sourceAddress);
    return scoreB - scoreA;
  });

  const primaryOffice = sortedOfficeSources[0] ?? null;

  const phoneCandidates = buildOfficeLabeledCandidates(
    sortedOfficeSources,
    "phone_candidates"
  );

  const faxCandidates = buildOfficeLabeledCandidates(
    sortedOfficeSources,
    "fax_candidates"
  );

  const firstPhoneValue =
    resolveOfficeLabeledCandidateValue(
      phoneCandidates.values[0],
      phoneCandidates.labelMap
    ) ?? normalizeNullableText(extracted.phone);

  const firstFaxValue =
    resolveOfficeLabeledCandidateValue(
      faxCandidates.values[0],
      faxCandidates.labelMap
    ) ?? normalizeNullableText(extracted.fax);

  const officeName = normalizeNullableText(primaryOffice?.office_name);
  const officeCompanyBase =
    normalizeNullableText(primaryOffice?.company) ?? baseCompany ?? fallbackCompany;

  const company =
    officeName && officeCompanyBase && !officeCompanyBase.includes(officeName)
      ? `${officeCompanyBase} ${officeName}`
      : officeCompanyBase;

  const payload: CrawlPayload = {
    company: company && isLikelyCompanyName(company) ? company : fallbackCompany,
    website_url: normalizeNullableText(extracted.website_url),
    form_url: normalizeNullableText(extracted.form_url),
    phone: firstPhoneValue,
    fax: firstFaxValue,
    email: normalizeNullableText(extracted.email),
    zipcode: normalizeNullableText(extracted.zipcode),
    address: normalizeNullableText(extracted.address),
    established_date: normalizeNullableText(extracted.established_date),
    representative_name: normalizeNullableText(extracted.representative_name),
    representative_name_raw: normalizeNullableText(
      extracted.representative_name_raw
    ),
    representative_name_reason: normalizeNullableText(
      extracted.representative_name_reason
    ),
    representative_title: normalizeNullableText(extracted.representative_title),
    capital: normalizeNullableText(extracted.capital),
    employee_count: normalizeNullableText(extracted.employee_count),
    business_content: normalizeNullableText(extracted.business_content),
    permit_number: normalizeNullableText(extracted.permit_number),
  };

  return [
    {
      payload,
      candidates: {
        phone: phoneCandidates.values.length > 0
          ? phoneCandidates.values
          : fallbackPhoneCandidates,
        fax: faxCandidates.values.length > 0
          ? faxCandidates.values
          : fallbackFaxCandidates,
        email: uniqueTextValues(
          sortedOfficeSources.flatMap((office) => office.email_candidates)
        ),
        zipcode: uniqueTextValues(
          sortedOfficeSources.flatMap((office) => office.zipcode_candidates)
        ),
        address: uniqueTextValues(
          sortedOfficeSources.flatMap((office) => office.address_candidates)
        ),
      },
      officeLabelMap: {
        phone: phoneCandidates.labelMap,
        fax: faxCandidates.labelMap,
      },
      forceCompanyUpdate: false,
    },
  ];
}

function buildPreviewChanges(
  row: Record<string, unknown>,
  bundle: CrawlPayloadBundle,
  selectedFieldSet: Set<CrawlSelectableFieldKey>
): CrawlPreviewChange[] {
  const changes: CrawlPreviewChange[] = [];

  for (const field of CRAWL_CANDIDATE_FIELDS) {
    if (!selectedFieldSet.has(field.key)) continue;

    const before = normalizeNullableText(row[field.key]);
    const candidates = uniqueTextValues(bundle.candidates[field.key]);
    const after = candidates[0] ?? null;

    if (candidates.length === 0) continue;
    if (candidates.length === 1 && before === after) continue;

    changes.push({
      key: field.key,
      label: field.label,
      before,
      after,
      candidates,
    });
  }

  for (const field of CRAWL_SINGLE_VALUE_FIELDS) {
    if (field.key === "permit_number") {
      if (!hasPermitSelection(selectedFieldSet)) continue;
    } else {
      if (!selectedFieldSet.has(field.key as CrawlSelectableFieldKey)) continue;
    }

    const before = normalizeNullableText(row[field.key]);
    const extractedAfter = normalizeNullableText(bundle.payload[field.key]);
    const after = getResolvedValue(row[field.key], bundle.payload[field.key]);

    const shouldKeepSameRepresentativePreview =
      field.key === "representative_name" &&
      extractedAfter != null &&
      before != null &&
      before === extractedAfter;

    const shouldKeepSameEmployeeCountPreview =
      field.key === "employee_count" &&
      extractedAfter != null &&
      before != null &&
      before === extractedAfter;

    if (
      before === after &&
      !shouldKeepSameRepresentativePreview &&
      !shouldKeepSameEmployeeCountPreview
    ) {
      continue;
    }

    if (after == null) continue;

    changes.push({
      key: field.key,
      label: field.label,
      before,
      after,
      candidates: after ? [after] : [],
    });
  }

  return changes;
}

function getDefaultSelectedValue(change: CrawlPreviewChange) {
  return (
    normalizeNullableText(change.after) ??
    normalizeNullableText(change.candidates[0]) ??
    null
  );
}

function buildEffectiveSelectedMap(
  item: CrawlJobPreviewItem,
  selectedChanges: SelectedCrawlChanges | undefined,
  selectedFields: CrawlSelectableFieldKey[]
) {
  const selected = selectedChanges?.[item.preview_row_id] ?? {};
  const result: Partial<Record<CrawlPreviewSelectableKey, string>> = {};
  const changes = buildPreviewChangesFromItem(item, selectedFields);

  for (const change of changes) {
    const hasExplicitValue = Object.prototype.hasOwnProperty.call(
      selected,
      change.key
    );

    const explicitValue = hasExplicitValue
      ? normalizeNullableText(selected[change.key])
      : undefined;

    const effectiveValue =
      explicitValue !== undefined
        ? explicitValue
        : getDefaultSelectedValue(change);

    if (effectiveValue !== null) {
      result[change.key] = effectiveValue;
    }
  }

  return result;
}

function buildSelectedPayload(
  bundle: CrawlPayloadBundle,
  item: CrawlJobPreviewItem,
  selectedChanges: SelectedCrawlChanges | undefined,
  selectedFields: CrawlSelectableFieldKey[]
): CrawlPayload {
  const selected = buildEffectiveSelectedMap(
    item,
    selectedChanges,
    selectedFields
  );

  const selectedPhone = resolveOfficeLabeledCandidateValue(
    selected.phone,
    bundle.officeLabelMap?.phone
  );

  const selectedFax = resolveOfficeLabeledCandidateValue(
    selected.fax,
    bundle.officeLabelMap?.fax
  );

  return {
    ...bundle.payload,
    company: selected.company ?? bundle.payload.company,
    website_url: selected.website_url ?? bundle.payload.website_url,
    phone: selectedPhone ?? bundle.payload.phone,
    fax: selectedFax ?? bundle.payload.fax,
    email: selected.email ?? bundle.payload.email,
    zipcode: selected.zipcode ?? bundle.payload.zipcode,
    address: selected.address ?? bundle.payload.address,
    form_url: selected.form_url ?? bundle.payload.form_url,
    established_date:
      selected.established_date ?? bundle.payload.established_date,
    representative_name:
      selected.representative_name ?? bundle.payload.representative_name,
    representative_title: bundle.payload.representative_title,
    capital: selected.capital ?? bundle.payload.capital,
    employee_count: selected.employee_count ?? bundle.payload.employee_count,
    business_content:
      selected.business_content ?? bundle.payload.business_content,
    permit_number:
      selected.permit_number ?? bundle.payload.permit_number,
  };
}

function getSelectedChangeKeys(
  item: CrawlJobPreviewItem,
  selectedChanges: SelectedCrawlChanges | undefined,
  selectedFields: CrawlSelectableFieldKey[]
) {
  return Object.keys(
    buildEffectiveSelectedMap(item, selectedChanges, selectedFields)
  ) as CrawlPreviewSelectableKey[];
}

function hasAnySelectedCandidate(
  item: CrawlJobPreviewItem,
  selectedChanges: SelectedCrawlChanges | undefined,
  selectedFields: CrawlSelectableFieldKey[]
) {
  return getSelectedChangeKeys(item, selectedChanges, selectedFields).length > 0;
}

function buildInsertRow(
  sourceRow: Record<string, unknown>,
  bundle: CrawlPayloadBundle,
  payload: CrawlPayload,
  selectedKeys: Set<CrawlPreviewSelectableKey>
): Record<string, unknown> {
  return {
    ...sourceRow,
    企業名: selectedKeys.has("company") ? payload.company : sourceRow["企業名"],
    企業サイトURL: selectedKeys.has("website_url")
      ? payload.website_url
      : sourceRow["企業サイトURL"],
    問い合わせフォームURL: selectedKeys.has("form_url")
      ? payload.form_url
      : sourceRow["問い合わせフォームURL"],
    電話番号: selectedKeys.has("phone") ? payload.phone : sourceRow["電話番号"],
    FAX番号: selectedKeys.has("fax") ? payload.fax : sourceRow["FAX番号"],
    メールアドレス: selectedKeys.has("email")
      ? payload.email
      : sourceRow["メールアドレス"],
    郵便番号: selectedKeys.has("zipcode")
      ? payload.zipcode
      : sourceRow["郵便番号"],
    住所: selectedKeys.has("address") ? payload.address : sourceRow["住所"],
    設立年月: selectedKeys.has("established_date")
      ? payload.established_date
      : sourceRow["設立年月"],
    代表者名: selectedKeys.has("representative_name")
      ? payload.representative_name
      : sourceRow["代表者名"],
    代表者役職: sourceRow["代表者役職"],
    資本金: selectedKeys.has("capital") ? payload.capital : sourceRow["資本金"],
    従業員数: selectedKeys.has("employee_count")
      ? payload.employee_count
      : sourceRow["従業員数"],
    事業内容: selectedKeys.has("business_content")
      ? payload.business_content
      : sourceRow["事業内容"],
    許可番号: selectedKeys.has("permit_number")
      ? payload.permit_number
      : sourceRow["許可番号"],
  };
}

function buildPreviewSourceRow(
  row: Record<string, unknown>
): PreviewSourceRow {
  return {
    company: normalizeNullableText(row.company),
    zipcode: normalizeNullableText(row.zipcode),
    address: normalizeNullableText(row.address),
    big_industry: normalizeNullableText(row.big_industry),
    small_industry: normalizeNullableText(row.small_industry),
    company_kana: normalizeNullableText(row.company_kana),
    summary: normalizeNullableText(row.summary),
    website_url: normalizeNullableText(row.website_url),
    form_url: normalizeNullableText(row.form_url),
    phone: normalizeNullableText(row.phone),
    fax: normalizeNullableText(row.fax),
    email: normalizeNullableText(row.email),
    established_date: normalizeNullableText(row.established_date),
    representative_name: normalizeNullableText(row.representative_name),
    representative_title: normalizeNullableText(row.representative_title),
    capital: normalizeNullableText(row.capital),
    employee_count: normalizeNullableText(row.employee_count),
    employee_count_year: normalizeNullableText(row.employee_count_year),
    previous_sales: normalizeNullableText(row.previous_sales),
    latest_sales: normalizeNullableText(row.latest_sales),
    closing_month: normalizeNullableText(row.closing_month),
    office_count: normalizeNullableText(row.office_count),
    tag: normalizeNullableText(row.tag),
    business_type: normalizeNullableText(row.business_type),
    business_content: normalizeNullableText(row.business_content),
    industry_category: normalizeNullableText(row.industry_category),
    permit_number: normalizeNullableText(row.permit_number),
    memo: normalizeNullableText(row.memo),
  };
}

async function fetchSourceRowForInsert(
  client: DbClient,
  rowId: string
): Promise<Record<string, unknown> | null> {
  const res = await client.query(
    `
      SELECT to_jsonb(md) AS source_row
      FROM public.master_data AS md
      WHERE md.id = $1::bigint
      LIMIT 1
    `,
    [rowId]
  );

  const sourceRow = res.rows[0]?.source_row;

  return sourceRow && typeof sourceRow === "object"
    ? (sourceRow as Record<string, unknown>)
    : null;
}

async function fetchSourceRowForPreview(
  client: DbClient,
  rowId: string
): Promise<Record<string, unknown> | null> {
  const res = await client.query(
    `
      SELECT
        md.id::text AS row_id,
        md."企業名" AS company,
        md."郵便番号" AS zipcode,
        md."住所" AS address,
        md."大業種名" AS big_industry,
        md."小業種名" AS small_industry,
        md."企業名（かな）" AS company_kana,
        md."企業概要" AS summary,
        md."企業サイトURL" AS website_url,
        md."問い合わせフォームURL" AS form_url,
        md."電話番号" AS phone,
        md."FAX番号" AS fax,
        md."メールアドレス" AS email,
        md."設立年月" AS established_date,
        md."代表者名" AS representative_name,
        md."代表者役職" AS representative_title,
        md."資本金" AS capital,
        md."従業員数" AS employee_count,
        md."従業員数年度" AS employee_count_year,
        md."前年売上高" AS previous_sales,
        md."直近売上高" AS latest_sales,
        md."決算月" AS closing_month,
        md."事業所数" AS office_count,
        md."新規登録タグ" AS tag,
        md."業種" AS business_type,
        md."事業内容" AS business_content,
        md."業界" AS industry_category,
        md."許可番号" AS permit_number,
        md."メモ" AS memo
      FROM public.master_data AS md
      WHERE md.id = $1::bigint
      LIMIT 1
    `,
    [rowId]
  );

  return (res.rows[0] as Record<string, unknown> | undefined) ?? null;
}

type CrawlJobPreviewItem = {
  row_id: string;
  preview_row_id: string;
  officeIndex: number;
  company: string | null;
  website_url: string | null;
  source_row: PreviewSourceRow | null;
  bundle: CrawlPayloadBundle;
};

function buildPreviewChangesFromItem(
  item: CrawlJobPreviewItem,
  selectedFields: CrawlSelectableFieldKey[]
) {
  if (!item.source_row) {
    return [] as CrawlPreviewChange[];
  }

  return buildPreviewChanges(
    item.source_row as unknown as Record<string, unknown>,
    item.bundle,
    new Set(selectedFields)
  );
}

function normalizePreviewCandidateForCompare(
  change: CrawlPreviewChange,
  bundle: CrawlPayloadBundle,
  candidate: string
) {
  const resolvedCandidate =
    change.key === "phone"
      ? resolveOfficeLabeledCandidateValue(candidate, bundle.officeLabelMap?.phone)
      : change.key === "fax"
      ? resolveOfficeLabeledCandidateValue(candidate, bundle.officeLabelMap?.fax)
      : normalizeNullableText(candidate);

  if (!resolvedCandidate) return "";

  if (change.key === "phone" || change.key === "fax") {
    return normalizeContactNumberForCompare(resolvedCandidate);
  }

  return resolvedCandidate
    .normalize("NFKC")
    .replace(/\s+/g, "")
    .toLowerCase();
}

function hasMultipleEffectiveCandidates(
  item: CrawlJobPreviewItem,
  change: CrawlPreviewChange
) {
  const uniqueCandidates = new Set<string>();

  for (const candidate of change.candidates) {
    const normalized = normalizePreviewCandidateForCompare(
      change,
      item.bundle,
      candidate
    );

    if (normalized) {
      uniqueCandidates.add(normalized);
    }
  }

  return uniqueCandidates.size > 1;
}

type CrawlJobState = {
  jobId: string;
  selectedFields: CrawlSelectableFieldKey[];
  selectedFieldLabels: string[];
  nextIndex: number;
  total: number;
  processed: number;
  updated: number;
  skipped: number;
  failed: number;
  currentCompany: string | null;
  currentWebsiteUrl: string | null;
  running: boolean;
  paused: boolean;
  pauseRequested: boolean;
  completed: boolean;
  error: string | null;
  elapsedMs: number;
  lastStartedAt: string | null;
  previewItems: CrawlJobPreviewItem[];
  excludedPreviewRows: CrawlPreviewRow[];
  savedPreviewRowIds: Set<string>;
  sortKey?: FilterKey | null;
  sortDirection?: SortDirection | "" | null;
};

const CRAWL_FIELD_LABEL_MAP: Record<CrawlSelectableFieldKey, string> = {
  company: "企業名",
  zipcode: "郵便番号",
  address: "住所",
  website_url: "企業URL",
  form_url: "お問い合わせフォームURL",
  phone: "電話番号",
  fax: "FAX番号",
  email: "メールアドレス",
  established_date: "設立年月",
  representative_name: "代表者名",
  capital: "資本金",
  employee_count: "従業員数",
  business_content: "事業内容",
  worker_dispatch_license: "労働者派遣",
  paid_job_placement_license: "有料職業紹介",
};

const globalForCrawlJobs = globalThis as typeof globalThis & {
  __masterDataCrawlJobs?: Map<string, CrawlJobState>;
  __masterDataCrawlJobRuns?: Set<string>;
};

const crawlJobs =
  globalForCrawlJobs.__masterDataCrawlJobs ??
  new Map<string, CrawlJobState>();

if (!globalForCrawlJobs.__masterDataCrawlJobs) {
  globalForCrawlJobs.__masterDataCrawlJobs = crawlJobs;
}

const runningCrawlJobIds =
  globalForCrawlJobs.__masterDataCrawlJobRuns ?? new Set<string>();

if (!globalForCrawlJobs.__masterDataCrawlJobRuns) {
  globalForCrawlJobs.__masterDataCrawlJobRuns = runningCrawlJobIds;
}

const CRAWL_PAUSED_ERROR_MESSAGE = "__MASTER_DATA_CRAWL_PAUSED__";

const WORKER_TOKEN_HEADER = "x-master-crawl-worker-token";

function getRequiredWorkerToken() {
  return (
    process.env.MASTER_CRAWL_WORKER_TOKEN ||
    process.env.WORKER_TOKEN ||
    ""
  ).trim();
}

function assertWorkerAuthorized(req: NextRequest) {
  const requiredToken = getRequiredWorkerToken();

  if (!requiredToken) {
    throw new Error("MASTER_CRAWL_WORKER_TOKEN が未設定です");
  }

  const actualToken = req.headers.get(WORKER_TOKEN_HEADER)?.trim() || "";

  if (actualToken !== requiredToken) {
    throw new Error("worker認証に失敗しました");
  }
}

function normalizeWorkerId(value: unknown) {
  const text = String(value ?? "").trim();
  return text === "" ? null : text.slice(0, 200);
}

function normalizeWorkerName(value: unknown) {
  const text = String(value ?? "").trim();
  return text === "" ? null : text.slice(0, 200);
}

function isLocalDevelopmentRequest(req: NextRequest) {
  const host = req.headers.get("host") || "";

  return (
    host.startsWith("localhost:") ||
    host.startsWith("127.0.0.1:")
  );
}

type SerializedCrawlJobPreviewItem = {
  row_id: string;
  preview_row_id: string;
  officeIndex: number;
  company: string | null;
  website_url: string | null;
  source_row: PreviewSourceRow | null;
  bundle: CrawlPayloadBundle;
};

type CrawlPersistCheckpoint = {
  lastPersistedProcessed: number;
  lastPersistedAt: number;
};

const CRAWL_JOB_PROGRESS_SAVE_EVERY = 1;
const CRAWL_JOB_PROGRESS_SAVE_INTERVAL_MS = 1000;

// 5万件を1回で走り切らず、一定件数ごとに区切って自動再開する
const CRAWL_JOB_MAX_ROWS_PER_RUN = 30;
const CRAWL_JOB_MAX_RUN_MS = 3 * 60 * 1000;
const CRAWL_JOB_RESTART_DELAY_MS = 1000;

// 1社の処理が長時間終わらない場合は、その会社だけ失敗扱いにして次へ進む
const CRAWL_COMPANY_TIMEOUT_MS = 90 * 1000;
const CRAWL_COMPANY_TIMEOUT_ERROR_MESSAGE =
  "__MASTER_DATA_CRAWL_COMPANY_TIMEOUT__";

function startCrawlJobRunner(jobId: string) {
  if (runningCrawlJobIds.has(jobId)) return;

  runningCrawlJobIds.add(jobId);

  void runCrawlJob(jobId)
    .catch(() => {
      // runCrawlJob内で状態保存するため、ここでは落とさない
    })
    .finally(() => {
      runningCrawlJobIds.delete(jobId);

      const job = crawlJobs.get(jobId);

      if (!job || job.completed || job.pauseRequested) {
        return;
      }

      setTimeout(() => {
        startCrawlJobRunner(jobId);
      }, CRAWL_JOB_RESTART_DELAY_MS);
    });
}

async function ensureCrawlPreviewTable(client: DbClient) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.master_data_crawl_preview (
      job_id text NOT NULL,
      preview_row_id text NOT NULL,
      row_id bigint NOT NULL,
      office_index integer NOT NULL,
      company text,
      website_url text,
      source_row jsonb,
      bundle jsonb NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (job_id, preview_row_id)
    )
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS master_data_crawl_preview_job_idx
    ON public.master_data_crawl_preview (job_id, row_id, office_index)
  `);
}

async function ensureCrawlJobTables(client: DbClient) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS public.master_data_crawl_jobs (
      job_id text PRIMARY KEY,
      selected_fields jsonb NOT NULL,
      selected_field_labels jsonb NOT NULL,
      next_index integer NOT NULL DEFAULT 0,
      total integer NOT NULL DEFAULT 0,
      processed integer NOT NULL DEFAULT 0,
      updated integer NOT NULL DEFAULT 0,
      skipped integer NOT NULL DEFAULT 0,
      failed integer NOT NULL DEFAULT 0,
      current_company text,
      current_website_url text,
      running boolean NOT NULL DEFAULT false,
      paused boolean NOT NULL DEFAULT false,
      pause_requested boolean NOT NULL DEFAULT false,
      completed boolean NOT NULL DEFAULT false,
      error text,
      elapsed_ms bigint NOT NULL DEFAULT 0,
      last_started_at timestamptz,
      sort_key text,
      sort_direction text,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await client.query(`
    ALTER TABLE public.master_data_crawl_jobs
      ADD COLUMN IF NOT EXISTS elapsed_ms bigint NOT NULL DEFAULT 0,
      ADD COLUMN IF NOT EXISTS last_started_at timestamptz,
      ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'queued',
      ADD COLUMN IF NOT EXISTS assigned_worker_id text,
      ADD COLUMN IF NOT EXISTS worker_id text,
      ADD COLUMN IF NOT EXISTS worker_heartbeat_at timestamptz,
      ADD COLUMN IF NOT EXISTS worker_locked_until timestamptz,
      ADD COLUMN IF NOT EXISTS worker_message text
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS public.master_data_crawl_targets (
      job_id text NOT NULL,
      target_index integer NOT NULL,
      row_id bigint NOT NULL,
      company text,
      address text,
      website_url text,
      status text NOT NULL DEFAULT 'pending',
      skip_reason text,
      error_message text,
      started_at timestamptz,
      finished_at timestamptz,
      PRIMARY KEY (job_id, target_index)
    )
  `);

  await client.query(`
    ALTER TABLE public.master_data_crawl_targets
      ADD COLUMN IF NOT EXISTS locked_by text,
      ADD COLUMN IF NOT EXISTS locked_until timestamptz,
      ADD COLUMN IF NOT EXISTS attempt_count integer NOT NULL DEFAULT 0
  `);

  await client.query(`
    CREATE TABLE IF NOT EXISTS public.master_data_crawl_workers (
      worker_id text PRIMARY KEY,
      worker_name text,
      last_seen_at timestamptz NOT NULL DEFAULT now(),
      last_message text,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS master_data_crawl_jobs_worker_idx
    ON public.master_data_crawl_jobs (assigned_worker_id, status, completed, created_at)
  `);

  await client.query(`
    CREATE INDEX IF NOT EXISTS master_data_crawl_targets_job_status_idx
    ON public.master_data_crawl_targets (job_id, status, target_index)
  `);
}

function serializeCrawlPreviewItem(
  item: CrawlJobPreviewItem
): SerializedCrawlJobPreviewItem {
  return {
    row_id: item.row_id,
    preview_row_id: item.preview_row_id,
    officeIndex: item.officeIndex,
    company: item.company,
    website_url: item.website_url,
    source_row: item.source_row,
    bundle: item.bundle,
  };
}

function deserializeCrawlPreviewItem(row: {
  row_id: string;
  preview_row_id: string;
  office_index: number;
  company: string | null;
  website_url: string | null;
  source_row: unknown;
  bundle: unknown;
}): CrawlJobPreviewItem {
  const sourceRow =
    typeof row.source_row === "string"
      ? (JSON.parse(row.source_row) as PreviewSourceRow | null)
      : ((row.source_row ?? null) as PreviewSourceRow | null);

  const bundle =
    typeof row.bundle === "string"
      ? (JSON.parse(row.bundle) as CrawlPayloadBundle)
      : (row.bundle as CrawlPayloadBundle);

  return {
    row_id: String(row.row_id ?? ""),
    preview_row_id: String(row.preview_row_id ?? ""),
    officeIndex: Number(row.office_index ?? 0),
    company: normalizeNullableText(row.company),
    website_url: normalizeNullableText(row.website_url),
    source_row: sourceRow,
    bundle,
  };
}

async function insertCrawlPreviewItems(
  client: DbClient,
  jobId: string,
  items: CrawlJobPreviewItem[]
) {
  if (items.length === 0) return;

  await ensureCrawlPreviewTable(client);

  for (const item of items) {
    const serialized = serializeCrawlPreviewItem(item);

    await client.query(
      `
        INSERT INTO public.master_data_crawl_preview (
          job_id,
          preview_row_id,
          row_id,
          office_index,
          company,
          website_url,
          source_row,
          bundle
        )
        VALUES ($1, $2, $3::bigint, $4::integer, $5, $6, $7::jsonb, $8::jsonb)
        ON CONFLICT (job_id, preview_row_id)
        DO UPDATE SET
          company = EXCLUDED.company,
          website_url = EXCLUDED.website_url,
          source_row = EXCLUDED.source_row,
          bundle = EXCLUDED.bundle
      `,
      [
        jobId,
        serialized.preview_row_id,
        serialized.row_id,
        serialized.officeIndex,
        serialized.company,
        serialized.website_url,
        JSON.stringify(serialized.source_row),
        JSON.stringify(serialized.bundle),
      ]
    );
  }
}

async function fetchPagedCrawlPreviewItems(
  client: DbClient,
  jobId: string,
  page: number,
  pageSize: number
) {
  await ensureCrawlPreviewTable(client);

  const totalRes = await client.query(
    `
      SELECT COUNT(*)::int AS total
      FROM public.master_data_crawl_preview
      WHERE job_id = $1
    `,
    [jobId]
  );

  const total = Number(totalRes.rows[0]?.total ?? 0);
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const safePage = Math.min(Math.max(page, 1), totalPages);
  const offset = (safePage - 1) * pageSize;

  const rowsRes = await client.query(
    `
      SELECT
        preview_row_id,
        row_id::text,
        office_index,
        company,
        website_url,
        source_row,
        bundle
      FROM public.master_data_crawl_preview
      WHERE job_id = $1
      ORDER BY row_id::bigint ASC, office_index ASC
      LIMIT $2 OFFSET $3
    `,
    [jobId, pageSize, offset]
  );

  return {
    items: rowsRes.rows.map((row) =>
      deserializeCrawlPreviewItem(row as {
        row_id: string;
        preview_row_id: string;
        office_index: number;
        company: string | null;
        website_url: string | null;
        source_row: unknown;
        bundle: unknown;
      })
    ),
    total,
    page: safePage,
    pageSize,
  };
}

async function fetchAllCrawlPreviewItems(client: DbClient, jobId: string) {
  await ensureCrawlPreviewTable(client);

  const rowsRes = await client.query(
    `
      SELECT
        preview_row_id,
        row_id::text,
        office_index,
        company,
        website_url,
        source_row,
        bundle
      FROM public.master_data_crawl_preview
      WHERE job_id = $1
      ORDER BY row_id::bigint ASC, office_index ASC
    `,
    [jobId]
  );

  return rowsRes.rows.map((row) =>
    deserializeCrawlPreviewItem(row as {
      row_id: string;
      preview_row_id: string;
      office_index: number;
      company: string | null;
      website_url: string | null;
      source_row: unknown;
      bundle: unknown;
    })
  );
}

async function deleteCrawlPreviewItemsByIds(
  client: DbClient,
  jobId: string,
  previewRowIds: string[]
) {
  if (previewRowIds.length === 0) return;

  await ensureCrawlPreviewTable(client);

  await client.query(
    `
      DELETE FROM public.master_data_crawl_preview
      WHERE job_id = $1
        AND preview_row_id = ANY($2::text[])
    `,
    [jobId, previewRowIds]
  );
}

async function deleteAllCrawlPreviewItems(client: DbClient, jobId: string) {
  await ensureCrawlPreviewTable(client);

  await client.query(
    `
      DELETE FROM public.master_data_crawl_preview
      WHERE job_id = $1
    `,
    [jobId]
  );
}

function createCrawlPersistCheckpoint(job: CrawlJobState): CrawlPersistCheckpoint {
  return {
    lastPersistedProcessed: job.processed,
    lastPersistedAt: Date.now(),
  };
}

async function persistCrawlJobStateIfNeeded(
  job: CrawlJobState,
  checkpoint: CrawlPersistCheckpoint,
  force = false
) {
  const now = Date.now();
  const processedDiff = job.processed - checkpoint.lastPersistedProcessed;
  const elapsedMs = now - checkpoint.lastPersistedAt;

  if (
    !force &&
    processedDiff < CRAWL_JOB_PROGRESS_SAVE_EVERY &&
    elapsedMs < CRAWL_JOB_PROGRESS_SAVE_INTERVAL_MS
  ) {
    return;
  }

  await persistCrawlJobState(job);

  checkpoint.lastPersistedProcessed = job.processed;
  checkpoint.lastPersistedAt = now;
}

async function persistCrawlJobState(job: CrawlJobState) {
  snapshotCrawlRunTimer(job);

  const client = await pool.connect();

  try {
    await ensureCrawlJobTables(client);

    await client.query(
      `
        INSERT INTO public.master_data_crawl_jobs (
          job_id,
          selected_fields,
          selected_field_labels,
          next_index,
          total,
          processed,
          updated,
          skipped,
          failed,
          current_company,
          current_website_url,
          running,
          paused,
          pause_requested,
          completed,
          error,
          elapsed_ms,
          last_started_at,
          sort_key,
          sort_direction,
          updated_at
        )
        VALUES (
          $1, $2::jsonb, $3::jsonb, $4, $5, $6, $7, $8, $9,
          $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, now()
        )
        ON CONFLICT (job_id)
        DO UPDATE SET
          selected_fields = EXCLUDED.selected_fields,
          selected_field_labels = EXCLUDED.selected_field_labels,
          next_index = EXCLUDED.next_index,
          total = EXCLUDED.total,
          processed = EXCLUDED.processed,
          updated = EXCLUDED.updated,
          skipped = EXCLUDED.skipped,
          failed = EXCLUDED.failed,
          current_company = EXCLUDED.current_company,
          current_website_url = EXCLUDED.current_website_url,
          running = EXCLUDED.running,
          paused = EXCLUDED.paused,
          pause_requested = EXCLUDED.pause_requested,
          completed = EXCLUDED.completed,
          error = EXCLUDED.error,
          elapsed_ms = EXCLUDED.elapsed_ms,
          last_started_at = EXCLUDED.last_started_at,
          sort_key = EXCLUDED.sort_key,
          sort_direction = EXCLUDED.sort_direction,
          updated_at = now()
      `,
      [
        job.jobId,
        JSON.stringify(job.selectedFields),
        JSON.stringify(job.selectedFieldLabels),
        job.nextIndex,
        job.total,
        job.processed,
        job.updated,
        job.skipped,
        job.failed,
        job.currentCompany,
        job.currentWebsiteUrl,
        job.running,
        job.paused,
        job.pauseRequested,
        job.completed,
        job.error,
        Math.max(Math.floor(job.elapsedMs), 0),
        job.lastStartedAt,
        job.sortKey ?? null,
        job.sortDirection ?? null,
      ]
    );
  } finally {
    client.release();
  }
}

async function deletePersistedCrawlJobState(jobId: string) {
  const client = await pool.connect();

  try {
    await ensureCrawlJobTables(client);

    await client.query(
      `DELETE FROM public.master_data_crawl_targets WHERE job_id = $1`,
      [jobId]
    );

    await client.query(
      `DELETE FROM public.master_data_crawl_jobs WHERE job_id = $1`,
      [jobId]
    );
  } finally {
    client.release();
  }
}

async function loadPersistedCrawlJobState(jobId: string) {
  const existing = crawlJobs.get(jobId);
  if (existing) {
    return existing;
  }

  const client = await pool.connect();

  try {
    await ensureCrawlJobTables(client);

    const res = await client.query(
      `
        SELECT
          job_id,
          selected_fields,
          selected_field_labels,
          next_index,
          total,
          processed,
          updated,
          skipped,
          failed,
          current_company,
          current_website_url,
          running,
          paused,
          pause_requested,
          completed,
          error,
          elapsed_ms,
          last_started_at,
          sort_key,
          sort_direction
        FROM public.master_data_crawl_jobs
        WHERE job_id = $1
        LIMIT 1
      `,
      [jobId]
    );

    const row = res.rows[0];

    if (!row) {
      return null;
    }

    const selectedFields = Array.isArray(row.selected_fields)
      ? row.selected_fields
      : JSON.parse(String(row.selected_fields ?? "[]"));

    const normalizedSelectedFields = Array.from(
      normalizeSelectedFields(selectedFields)
    );

    const persistedElapsedMs = Number(row.elapsed_ms ?? 0);
    const persistedLastStartedAt = normalizeDateIsoText(row.last_started_at);
    const persistedLastStartedAtMs = persistedLastStartedAt
      ? Date.parse(persistedLastStartedAt)
      : NaN;

    const persistedError = normalizeNullableText(row.error);
    const persistedCompleted = !!row.completed;
    const persistedPaused = !!row.paused;
    const persistedPauseRequested = !!row.pause_requested;
    const persistedRunning =
      !!row.running &&
      !persistedCompleted &&
      !persistedPaused &&
      !persistedPauseRequested &&
      !persistedError;

    const recoveredElapsedMs =
      persistedRunning && Number.isFinite(persistedLastStartedAtMs)
        ? persistedElapsedMs + Math.max(Date.now() - persistedLastStartedAtMs, 0)
        : persistedElapsedMs;

    const job: CrawlJobState = {
      jobId: String(row.job_id),
      selectedFields: normalizedSelectedFields,
      selectedFieldLabels: buildSelectedFieldLabels(normalizedSelectedFields),
      nextIndex: Number(row.next_index ?? 0),
      total: Number(row.total ?? 0),
      processed: Number(row.processed ?? 0),
      updated: Number(row.updated ?? 0),
      skipped: Number(row.skipped ?? 0),
      failed: Number(row.failed ?? 0),
      currentCompany: normalizeNullableText(row.current_company),
      currentWebsiteUrl: normalizeNullableText(row.current_website_url),
      running: persistedRunning,
      paused: persistedPaused,
      pauseRequested: persistedPauseRequested,
      completed: persistedCompleted,
      error: persistedError,
      elapsedMs: Number.isFinite(recoveredElapsedMs)
        ? Math.max(Math.floor(recoveredElapsedMs), 0)
        : 0,
      lastStartedAt: null,
      previewItems: [],
      excludedPreviewRows: [],
      savedPreviewRowIds: new Set<string>(),
      sortKey: row.sort_key as FilterKey | null,
      sortDirection: row.sort_direction as SortDirection | "" | null,
    };

    crawlJobs.set(job.jobId, job);
    return job;
  } finally {
    client.release();
  }
}

async function getCrawlJobFromMemoryOrFile(jobId?: string | null) {
  if (!jobId) {
    return null;
  }

  return await loadPersistedCrawlJobState(jobId);
}

function isCrawlPausedError(error: unknown) {
  return (
    error instanceof Error &&
    error.message === CRAWL_PAUSED_ERROR_MESSAGE
  );
}

function isRetryableCrawlJobError(message: string) {
  return /(Connection terminated|ECONNRESET|ETIMEDOUT|EPIPE|ENOTFOUND|ECONNREFUSED|fetch failed|timeout|terminating connection|server closed the connection unexpectedly|Client has encountered a connection error|Connection lost|ConnectionError|read ECONNRESET|socket hang up)/i.test(
    message
  );
}

function isCrawlCompanyTimeoutError(error: unknown) {
  return (
    error instanceof Error &&
    error.message === CRAWL_COMPANY_TIMEOUT_ERROR_MESSAGE
  );
}

async function runWithCrawlCompanyTimeout<T>(
  task: Promise<T>,
  onTimeout: () => void
): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;

  try {
    return await Promise.race([
      task,
      new Promise<T>((_, reject) => {
        timeoutId = setTimeout(() => {
          onTimeout();
          reject(new Error(CRAWL_COMPANY_TIMEOUT_ERROR_MESSAGE));
        }, CRAWL_COMPANY_TIMEOUT_MS);
      }),
    ]);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

function markCrawlJobPaused(job: CrawlJobState) {
  stopCrawlRunTimer(job);

  job.running = false;
  job.paused = true;
  job.pauseRequested = true;
  job.currentCompany = null;
  job.currentWebsiteUrl = null;
}

function getCrawlElapsedMs(job: CrawlJobState) {
  const baseElapsedMs = Number.isFinite(job.elapsedMs)
    ? Math.max(Math.floor(job.elapsedMs), 0)
    : 0;

  if (!job.running || !job.lastStartedAt) {
    return baseElapsedMs;
  }

  const startedAtMs = Date.parse(job.lastStartedAt);
  if (!Number.isFinite(startedAtMs)) {
    return baseElapsedMs;
  }

  return baseElapsedMs + Math.max(Date.now() - startedAtMs, 0);
}

function startCrawlRunTimer(job: CrawlJobState) {
  if (!job.lastStartedAt) {
    job.lastStartedAt = new Date().toISOString();
  }
}

function stopCrawlRunTimer(job: CrawlJobState) {
  job.elapsedMs = getCrawlElapsedMs(job);
  job.lastStartedAt = null;
}

function snapshotCrawlRunTimer(job: CrawlJobState) {
  if (!job.running || !job.lastStartedAt) {
    return;
  }

  job.elapsedMs = getCrawlElapsedMs(job);
  job.lastStartedAt = new Date().toISOString();
}

function buildSelectedFieldLabels(selectedFields: CrawlSelectableFieldKey[]) {
  return selectedFields.map((field) => CRAWL_FIELD_LABEL_MAP[field]);
}

const DEFAULT_CRAWL_PREVIEW_PAGE_SIZE = 20;
const MAX_CRAWL_PREVIEW_PAGE_SIZE = 100;

function normalizePreviewPage(value: unknown) {
  const num = Number(value);
  return Number.isFinite(num) && num > 0 ? Math.floor(num) : 1;
}

function normalizePreviewPageSize(value: unknown) {
  const num = Number(value);

  if (!Number.isFinite(num) || num <= 0) {
    return DEFAULT_CRAWL_PREVIEW_PAGE_SIZE;
  }

  return Math.min(Math.floor(num), MAX_CRAWL_PREVIEW_PAGE_SIZE);
}

async function buildPublicPreviewRows(
  client: DbClient,
  job: CrawlJobState,
  page: number,
  pageSize: number,
  previewTab: "candidate" | "multiple" | "excluded" = "candidate"
) {
  const previewPage = normalizePreviewPage(page);
  const previewPageSize = normalizePreviewPageSize(pageSize);

  if (previewTab === "candidate") {
    const previewResult = await fetchPagedCrawlPreviewItems(
      client,
      job.jobId,
      previewPage,
      previewPageSize
    );

    const rows = previewResult.items.map((item) => ({
      row_id: item.row_id,
      preview_row_id: item.preview_row_id,
      company: item.company,
      website_url: item.website_url,
      source_row: item.source_row,
      changes: buildPreviewChangesFromItem(item, job.selectedFields),
    }));

    return {
      rows,
      total: previewResult.total,
      page: previewResult.page,
      pageSize: previewResult.pageSize,
    };
  }

  if (previewTab === "multiple") {
    const allItems = await fetchAllCrawlPreviewItems(client, job.jobId);

    const multipleRows = allItems
      .map((item) => {
        const changes = buildPreviewChangesFromItem(
          item,
          job.selectedFields
        ).filter((change) => hasMultipleEffectiveCandidates(item, change));

        return {
          row_id: item.row_id,
          preview_row_id: item.preview_row_id,
          company: item.company,
          website_url: item.website_url,
          source_row: item.source_row,
          changes,
        };
      })
      .filter((row) => row.changes.length > 0);

    const total = multipleRows.length;
    const totalPages = Math.max(1, Math.ceil(total / previewPageSize));
    const safePage = Math.min(Math.max(previewPage, 1), totalPages);
    const start = (safePage - 1) * previewPageSize;

    return {
      rows: multipleRows.slice(start, start + previewPageSize),
      total,
      page: safePage,
      pageSize: previewPageSize,
    };
  }

  const countRes = await client.query(
    `
      SELECT COUNT(*)::int AS total
      FROM public.master_data_crawl_targets AS t
      WHERE t.job_id = $1
        AND t.status IN ('done', 'skipped', 'failed')
        AND NOT EXISTS (
          SELECT 1
          FROM public.master_data_crawl_preview AS p
          WHERE p.job_id = t.job_id
            AND p.row_id = t.row_id
        )
    `,
    [job.jobId]
  );

  const total = Number(countRes.rows[0]?.total ?? 0);
  const totalPages = Math.max(1, Math.ceil(total / previewPageSize));
  const safePage = Math.min(Math.max(previewPage, 1), totalPages);
  const start = (safePage - 1) * previewPageSize;

  const excludedRes = await client.query(
    `
      SELECT
        t.row_id::text,
        t.company,
        COALESCE(t.address, md."住所") AS address,
        t.website_url,
        t.skip_reason,
        t.error_message
      FROM public.master_data_crawl_targets AS t
      LEFT JOIN public.master_data AS md
        ON md.id = t.row_id
      WHERE t.job_id = $1
        AND t.status IN ('done', 'skipped', 'failed')
        AND NOT EXISTS (
          SELECT 1
          FROM public.master_data_crawl_preview AS p
          WHERE p.job_id = t.job_id
            AND p.row_id = t.row_id
        )
      ORDER BY t.target_index ASC
      LIMIT $2 OFFSET $3
    `,
    [job.jobId, previewPageSize, start]
  );

  const rows: CrawlPreviewRow[] = excludedRes.rows.map((row) => {
    const rowId = String(row.row_id ?? "");
    const reason =
      normalizeNullableText(row.skip_reason) ??
      normalizeNullableText(row.error_message);

    return {
      row_id: rowId,
      preview_row_id: `${rowId}__excluded`,
      company: normalizeNullableText(row.company),
      website_url: normalizeNullableText(row.website_url),
      source_row: {
        company: normalizeNullableText(row.company),
        zipcode: null,
        address: normalizeNullableText(row.address),
        big_industry: null,
        small_industry: null,
        company_kana: null,
        summary: reason,
        website_url: normalizeNullableText(row.website_url),
        form_url: null,
        phone: null,
        fax: null,
        email: null,
        established_date: null,
        representative_name: null,
        representative_title: null,
        capital: null,
        employee_count: null,
        employee_count_year: null,
        previous_sales: null,
        latest_sales: null,
        closing_month: null,
        office_count: null,
        tag: null,
        business_type: null,
        business_content: null,
        industry_category: null,
        permit_number: null,
        memo: reason,
      },
      changes: [],
    };
  });

  return {
    rows,
    total,
    page: safePage,
    pageSize: previewPageSize,
  };
}

function buildJobResponse(job: CrawlJobState) {
  const elapsedMs = getCrawlElapsedMs(job);
  const averageSecondsPerItem =
    job.processed > 0
      ? Number((elapsedMs / 1000 / job.processed).toFixed(1))
      : 0;

  return {
    ok: true,
    jobId: job.jobId,
    jobStatus: job.error
      ? "error"
      : job.completed
      ? "completed"
      : job.paused
      ? "paused"
      : "running",
    totalTargets: job.total,
    processed: job.processed,
    updated: Math.min(job.updated, job.processed),
    skipped: job.skipped,
    failed: job.failed,
    currentCompany: job.currentCompany,
    currentWebsiteUrl: job.currentWebsiteUrl,
    currentFields: job.selectedFieldLabels,
    progressPercent:
      job.total === 0 ? 0 : Math.round((job.processed / job.total) * 100),
    elapsedMs: Math.round(elapsedMs),
    elapsedSeconds: Math.floor(elapsedMs / 1000),
    averageSecondsPerItem,
    previewRows: undefined,
    previewTotal: 0,
    previewPage: 1,
    previewPageSize: DEFAULT_CRAWL_PREVIEW_PAGE_SIZE,
    remainingCount: Math.max(job.total - job.nextIndex, 0),
    paused: job.paused,
    completed: job.completed,
    error: job.error,
  };
}

function shouldRefreshCrawlProgressAuthCookie(job: CrawlJobState) {
  return (
    job.running &&
    !job.completed &&
    !job.paused &&
    !job.pauseRequested &&
    !job.error
  );
}

async function createCrawlTargets(
  client: DbClient,
  jobId: string,
  body: CrawlRequestBody,
  listScopeFilters: PermissionListScopeFilters | null
) {
  await ensureMasterDataIdColumn(client);
  await ensureCrawlJobTables(client);

  const filterModels = body.filterModels ?? {};
  const advancedFilters = body.advancedFilters ?? {};
  const { whereSql, params } = buildWhereClauseWithListScope(
    filterModels,
    advancedFilters,
    listScopeFilters
  );
  const orderBySql = buildOrderBy(body.sortKey, body.sortDirection);

  await client.query(
    `
      INSERT INTO public.master_data_crawl_targets (
        job_id,
        target_index,
        row_id,
        company,
        address,
        website_url
      )
      SELECT
        $${params.length + 1} AS job_id,
        ROW_NUMBER() OVER (${orderBySql})::integer - 1 AS target_index,
        md.id::bigint AS row_id,
        md."企業名" AS company,
        md."住所" AS address,
        md."企業サイトURL" AS website_url
      FROM public.master_data AS md
      ${whereSql}
      ${orderBySql}
    `,
    [...params, jobId]
  );

  const countRes = await client.query(
    `
      SELECT COUNT(*)::int AS total
      FROM public.master_data_crawl_targets
      WHERE job_id = $1
    `,
    [jobId]
  );

  return Number(countRes.rows[0]?.total ?? 0);
}

async function fetchCrawlTargetByIndex(
  client: DbClient,
  jobId: string,
  targetIndex: number
) {
  const res = await client.query(
    `
      SELECT
        row_id::text,
        company,
        address,
        website_url
      FROM public.master_data_crawl_targets
      WHERE job_id = $1
        AND target_index = $2
      LIMIT 1
    `,
    [jobId, targetIndex]
  );

  return (res.rows[0] as Record<string, unknown> | undefined) ?? null;
}

async function fetchCrawlTargetsByRange(
  client: DbClient,
  jobId: string,
  startIndex: number,
  limit: number
) {
  const safeLimit = Math.min(Math.max(Math.floor(limit), 1), 10);

  const res = await client.query(
    `
      SELECT
        target_index,
        row_id::text,
        company,
        address,
        website_url
      FROM public.master_data_crawl_targets
      WHERE job_id = $1
        AND target_index >= $2
        AND target_index < $2 + $3
      ORDER BY target_index ASC
    `,
    [jobId, startIndex, safeLimit]
  );

  return res.rows as Array<Record<string, unknown>>;
}

async function markCrawlTargetStatus(
  client: DbClient,
  jobId: string,
  targetIndex: number,
  status: "processing" | "done" | "skipped" | "failed",
  reason?: string | null,
  timing?: {
    startedAt?: string | null;
    finishedAt?: string | null;
  }
) {
  await client.query(
    `
      UPDATE public.master_data_crawl_targets
      SET
        status = $3,
        skip_reason = CASE WHEN $3 = 'skipped' THEN $4 ELSE skip_reason END,
        error_message = CASE WHEN $3 = 'failed' THEN $4 ELSE error_message END,
        started_at = CASE
          WHEN $5::timestamptz IS NOT NULL THEN $5::timestamptz
          WHEN $3 = 'processing' THEN COALESCE(started_at, now())
          WHEN $3 IN ('done', 'skipped', 'failed') THEN COALESCE(started_at, now())
          ELSE started_at
        END,
        finished_at = CASE
          WHEN $6::timestamptz IS NOT NULL THEN $6::timestamptz
          WHEN $3 IN ('done', 'skipped', 'failed') THEN now()
          ELSE finished_at
        END
      WHERE job_id = $1
        AND target_index = $2
    `,
    [
      jobId,
      targetIndex,
      status,
      reason ?? null,
      timing?.startedAt ?? null,
      timing?.finishedAt ?? null,
    ]
  );
}

async function markCrawlTargetsProcessing(
  client: DbClient,
  jobId: string,
  targetIndexes: number[]
) {
  if (targetIndexes.length === 0) return;

  await client.query(
    `
      UPDATE public.master_data_crawl_targets
      SET
        status = 'processing'
      WHERE job_id = $1
        AND target_index = ANY($2::int[])
    `,
    [jobId, targetIndexes]
  );
}

async function withCrawlDbClient<T>(
  callback: (client: DbClient) => Promise<T>
): Promise<T> {
  const client = await pool.connect();

  try {
    return await callback(client);
  } finally {
    client.release();
  }
}

async function registerWorker(
  client: DbClient,
  workerId: string,
  workerName: string | null,
  message?: string | null
) {
  await ensureCrawlJobTables(client);

  await client.query(
    `
      INSERT INTO public.master_data_crawl_workers (
        worker_id,
        worker_name,
        last_seen_at,
        last_message,
        updated_at
      )
      VALUES ($1, $2, now(), $3, now())
      ON CONFLICT (worker_id)
      DO UPDATE SET
        worker_name = EXCLUDED.worker_name,
        last_seen_at = now(),
        last_message = EXCLUDED.last_message,
        updated_at = now()
    `,
    [workerId, workerName, message ?? null]
  );
}

async function updateJobWorkerColumns(
  client: DbClient,
  jobId: string,
  params: {
    status?: string;
    assignedWorkerId?: string | null;
    workerId?: string | null;
    message?: string | null;
  }
) {
  await client.query(
    `
      UPDATE public.master_data_crawl_jobs
      SET
        status = COALESCE($2, status),
        assigned_worker_id = COALESCE($3, assigned_worker_id),
        worker_id = COALESCE($4, worker_id),
        worker_heartbeat_at = now(),
        worker_message = COALESCE($5, worker_message),
        updated_at = now()
      WHERE job_id = $1
    `,
    [
      jobId,
      params.status ?? null,
      params.assignedWorkerId ?? null,
      params.workerId ?? null,
      params.message ?? null,
    ]
  );
}

async function claimWorkerJob(
  client: DbClient,
  workerId: string,
  workerName: string | null
) {
  await ensureCrawlJobTables(client);
  await registerWorker(client, workerId, workerName, "ジョブ確認中");

  await client.query("BEGIN");

  try {
    const res = await client.query(
      `
        SELECT job_id
        FROM public.master_data_crawl_jobs
        WHERE completed = false
          AND pause_requested = false
          AND COALESCE(status, 'queued') IN ('queued', 'running')
          AND (
            assigned_worker_id = $1
            OR COALESCE(status, 'queued') = 'queued'
            OR worker_heartbeat_at IS NULL
            OR worker_heartbeat_at < now() - interval '2 minutes'
          )
        ORDER BY
          CASE
            WHEN assigned_worker_id = $1 THEN 0
            WHEN COALESCE(status, 'queued') = 'queued' THEN 1
            ELSE 2
          END,
          created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      `,
      [workerId]
    );

    const jobId = res.rows[0]?.job_id ? String(res.rows[0].job_id) : null;

    if (!jobId) {
      await client.query("COMMIT");
      return null;
    }

    await client.query(
      `
        UPDATE public.master_data_crawl_jobs
        SET
          status = 'running',
          running = true,
          paused = false,
          pause_requested = false,
          completed = false,
          error = NULL,
          assigned_worker_id = $2,
          worker_id = $2,
          worker_heartbeat_at = now(),
          worker_message = 'ジョブ処理中',
          last_started_at = COALESCE(last_started_at, now()),
          updated_at = now()
        WHERE job_id = $1
      `,
      [jobId, workerId]
    );

    await registerWorker(client, workerId, workerName, `ジョブ処理中: ${jobId}`);

    await client.query("COMMIT");
    return jobId;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}

async function claimWorkerTarget(
  client: DbClient,
  job: CrawlJobState,
  workerId: string
) {
  await ensureCrawlJobTables(client);

  if (job.pauseRequested) {
    markCrawlJobPaused(job);
    await persistCrawlJobState(job);
    await updateJobWorkerColumns(client, job.jobId, {
      status: "paused",
      workerId,
      message: "一時停止中",
    });

    return { paused: true, completed: false, target: null };
  }

  if (job.completed || job.nextIndex >= job.total) {
    stopCrawlRunTimer(job);

    job.completed = true;
    job.running = false;
    job.paused = false;
    job.currentCompany = null;
    job.currentWebsiteUrl = null;

    await persistCrawlJobState(job);
    await updateJobWorkerColumns(client, job.jobId, {
      status: "completed",
      workerId,
      message: "完了",
    });

    return { paused: false, completed: true, target: null };
  }

  const row = await fetchCrawlTargetByIndex(client, job.jobId, job.nextIndex);

  if (!row) {
    job.failed += 1;
    job.processed += 1;
    job.nextIndex += 1;
    await persistCrawlJobState(job);

    return { paused: false, completed: false, target: null };
  }

  job.running = true;
  job.paused = false;
  job.currentCompany = normalizeNullableText(row.company);
  job.currentWebsiteUrl = normalizeNullableText(row.website_url);
  startCrawlRunTimer(job);

  await markCrawlTargetStatus(client, job.jobId, job.nextIndex, "processing");
  await persistCrawlJobState(job);
  await updateJobWorkerColumns(client, job.jobId, {
    status: "running",
    workerId,
    message: `取得中: ${job.currentCompany ?? job.currentWebsiteUrl ?? ""}`,
  });

  return {
    paused: false,
    completed: false,
    target: {
      targetIndex: job.nextIndex,
      rowId: String(row.row_id ?? ""),
      company: normalizeNullableText(row.company),
      address: normalizeNullableText(row.address),
      websiteUrl: normalizeNullableText(row.website_url),
      selectedFields: job.selectedFields,
    },
  };
}

async function claimWorkerTargets(
  client: DbClient,
  job: CrawlJobState,
  workerId: string,
  limit: number
) {
  await ensureCrawlJobTables(client);

  const safeLimit = Math.min(Math.max(Math.floor(limit), 1), 10);

  await updateJobWorkerColumns(client, job.jobId, {
    status: "running",
    assignedWorkerId: workerId,
    workerId,
    message: "対象取得中",
  });

  if (job.pauseRequested) {
    markCrawlJobPaused(job);
    await persistCrawlJobState(job);
    await updateJobWorkerColumns(client, job.jobId, {
      status: "paused",
      workerId,
      message: "一時停止中",
    });

    return { paused: true, completed: false, targets: [] as WorkerCrawlTarget[] };
  }

  if (job.completed || job.nextIndex >= job.total) {
    stopCrawlRunTimer(job);

    job.completed = true;
    job.running = false;
    job.paused = false;
    job.currentCompany = null;
    job.currentWebsiteUrl = null;

    await persistCrawlJobState(job);
    await updateJobWorkerColumns(client, job.jobId, {
      status: "completed",
      workerId,
      message: "完了",
    });

    return { paused: false, completed: true, targets: [] as WorkerCrawlTarget[] };
  }

  const rows = await fetchCrawlTargetsByRange(
    client,
    job.jobId,
    job.nextIndex,
    safeLimit
  );

  if (rows.length === 0) {
    job.failed += 1;
    job.processed += 1;
    job.nextIndex += 1;
    await persistCrawlJobState(job);

    return { paused: false, completed: false, targets: [] as WorkerCrawlTarget[] };
  }

  const targets: WorkerCrawlTarget[] = rows.map((row) => ({
    targetIndex: Number(row.target_index ?? 0),
    rowId: String(row.row_id ?? ""),
    company: normalizeNullableText(row.company),
    address: normalizeNullableText(row.address),
    websiteUrl: normalizeNullableText(row.website_url),
    selectedFields: job.selectedFields,
  }));

  const firstTarget = targets[0];

  job.running = true;
  job.paused = false;
  job.currentCompany = firstTarget.company;
  job.currentWebsiteUrl = firstTarget.websiteUrl;
  startCrawlRunTimer(job);

  await markCrawlTargetsProcessing(
    client,
    job.jobId,
    targets.map((target) => target.targetIndex)
  );

  await persistCrawlJobState(job);
  await updateJobWorkerColumns(client, job.jobId, {
    status: "running",
    workerId,
    message: `取得中: ${job.currentCompany ?? job.currentWebsiteUrl ?? ""}`,
  });

  return {
    paused: false,
    completed: false,
    targets,
  };
}

async function reportWorkerTargetResult(
  client: DbClient,
  job: CrawlJobState,
  workerId: string,
  body: CrawlRequestBody
) {
  const targetIndex = Number(body.targetIndex);

  if (!Number.isFinite(targetIndex) || targetIndex < 0) {
    throw new Error("targetIndex が不正です");
  }

  if (job.paused || job.pauseRequested) {
    markCrawlJobPaused(job);
    await persistCrawlJobState(job);

    await updateJobWorkerColumns(client, job.jobId, {
      status: "paused",
      workerId,
      message: "一時停止中",
    });

    return;
  }

  if (targetIndex < job.nextIndex) {
    return;
  }

  if (targetIndex > job.nextIndex) {
    throw new Error("targetIndex が現在の進捗と一致しません");
  }

  const row = await fetchCrawlTargetByIndex(client, job.jobId, targetIndex);

  if (!row) {
    job.failed += 1;
    job.processed += 1;
    job.nextIndex = targetIndex + 1;

    await markCrawlTargetStatus(
      client,
      job.jobId,
      targetIndex,
      "failed",
      "DB上の対象行が見つかりません"
    );

    if (!body.deferPersist) {
      await persistCrawlJobState(job);
    }

    return;
  }

  let targetStatus = body.targetStatus ?? "done";
  let statusReason = normalizeNullableText(body.statusReason);
  const extracted = body.extracted ?? null;
  const targetStartedAt = normalizeDateIsoText(body.targetStartedAt);
  const targetFinishedAt = normalizeDateIsoText(body.targetFinishedAt);

  try {
    if (targetStatus === "done") {
      if (!extracted) {
        targetStatus = "failed";
        statusReason = "workerから取得結果が返されませんでした";
        job.failed += 1;
      } else {
        const currentRowData = await fetchSourceRowForPreview(
          client,
          String(row.row_id ?? "")
        );

        if (!currentRowData) {
          targetStatus = "failed";
          statusReason = "DB上の元データが見つかりません";
          job.failed += 1;
        } else {
          const selectedFieldSet = new Set(job.selectedFields);
          const bundles = buildCrawlPayloadBundles(
            extracted,
            normalizeNullableText(row.company),
            normalizeNullableText(row.address)
          );

          const previewSourceRow = buildPreviewSourceRow(currentRowData);
          const rowPreviewItems: CrawlJobPreviewItem[] = [];

          bundles.forEach((bundle, officeIndex) => {
            const changes = buildPreviewChanges(
              previewSourceRow as unknown as Record<string, unknown>,
              bundle,
              selectedFieldSet
            );

            if (changes.length === 0) {
              return;
            }

            rowPreviewItems.push({
              row_id: String(row.row_id ?? ""),
              preview_row_id: `${String(row.row_id ?? "")}__${officeIndex}`,
              officeIndex,
              company: normalizeNullableText(row.company),
              website_url: normalizeNullableText(row.website_url),
              source_row: previewSourceRow,
              bundle,
            });
          });

          if (rowPreviewItems.length > 0) {
            await insertCrawlPreviewItems(client, job.jobId, rowPreviewItems);
            job.updated += 1;
            targetStatus = "done";
          } else {
            targetStatus = "skipped";
            statusReason = "取得候補なし、または既存値と同じため更新候補なし";
            job.skipped += 1;
          }
        }
      }
    } else if (targetStatus === "skipped") {
      job.skipped += 1;
    } else {
      job.failed += 1;
    }

    await markCrawlTargetStatus(
      client,
      job.jobId,
      targetIndex,
      targetStatus,
      statusReason,
      {
        startedAt: targetStartedAt,
        finishedAt: targetFinishedAt,
      }
    );

    job.processed += 1;
    job.nextIndex = targetIndex + 1;
    job.currentCompany = null;
    job.currentWebsiteUrl = null;

    if (job.nextIndex >= job.total) {
      stopCrawlRunTimer(job);
      job.completed = true;
      job.running = false;
      job.paused = false;
    }

    if (!body.deferPersist) {
      await persistCrawlJobState(job);

      await updateJobWorkerColumns(client, job.jobId, {
        status: job.completed ? "completed" : "running",
        workerId,
        message: job.completed ? "完了" : "処理継続中",
      });
    }
  } catch (error) {
    await markCrawlTargetStatus(
      client,
      job.jobId,
      targetIndex,
      "failed",
      error instanceof Error ? error.message : "不明なエラー",
      {
        startedAt: targetStartedAt,
        finishedAt: targetFinishedAt,
      }
    );

    job.failed += 1;
    job.processed += 1;
    job.nextIndex = targetIndex + 1;
    job.currentCompany = null;
    job.currentWebsiteUrl = null;

    if (!body.deferPersist) {
      await persistCrawlJobState(job);
    }
  }
}

async function reportWorkerTargetResults(
  client: DbClient,
  job: CrawlJobState,
  workerId: string,
  results: WorkerTargetResult[]
) {
  const sortedResults = [...results].sort(
    (a, b) => Number(a.targetIndex) - Number(b.targetIndex)
  );

  for (const result of sortedResults) {
    await reportWorkerTargetResult(client, job, workerId, {
      action: "worker_report_target",
      jobId: job.jobId,
      targetIndex: result.targetIndex,
      targetStatus: result.targetStatus,
      statusReason: result.statusReason ?? null,
      extracted: result.extracted ?? null,
      targetStartedAt: result.targetStartedAt ?? null,
      targetFinishedAt: result.targetFinishedAt ?? null,
      deferPersist: true,
    });
  }

  await persistCrawlJobState(job);

  await updateJobWorkerColumns(client, job.jobId, {
    status: job.completed ? "completed" : "running",
    workerId,
    message: job.completed ? "完了" : "処理継続中",
  });
}

async function fetchCrawlElapsedMsFromTargets(
  client: DbClient,
  jobId: string
) {
  await ensureCrawlJobTables(client);

  const res = await client.query(
    `
      SELECT
        COALESCE(
          SUM(
            GREATEST(
              EXTRACT(
                EPOCH FROM (
                  (
                    CASE
                      WHEN finished_at IS NOT NULL THEN finished_at
                      WHEN status = 'processing' THEN now()
                      ELSE started_at
                    END
                  ) - started_at
                )
              ) * 1000,
              0
            )
          ),
          0
        )::bigint AS elapsed_ms
      FROM public.master_data_crawl_targets
      WHERE job_id = $1
        AND started_at IS NOT NULL
    `,
    [jobId]
  );

  return Number(res.rows[0]?.elapsed_ms ?? 0);
}

async function syncCrawlElapsedMsFromTargets(job: CrawlJobState) {
  const targetElapsedMs = await withCrawlDbClient((client) =>
    fetchCrawlElapsedMsFromTargets(client, job.jobId)
  );

  if (!Number.isFinite(targetElapsedMs)) {
    return;
  }

  const isRunningJob =
    job.running && !job.completed && !job.paused && !job.pauseRequested;

  const currentElapsedMs = getCrawlElapsedMs(job);
  const nextElapsedMs = Math.max(
    Math.floor(currentElapsedMs),
    Math.floor(targetElapsedMs),
    Math.floor(job.elapsedMs),
    0
  );

  if (
    nextElapsedMs === job.elapsedMs &&
    (!isRunningJob || job.lastStartedAt !== null)
  ) {
    return;
  }

  job.elapsedMs = nextElapsedMs;
  job.lastStartedAt = isRunningJob ? new Date().toISOString() : null;

  await persistCrawlJobState(job);
}

async function runCrawlJob(jobId: string) {
  const firstJob = await getCrawlJobFromMemoryOrFile(jobId);
  if (!firstJob || firstJob.completed) return;

  firstJob.running = true;
  firstJob.paused = false;
  firstJob.pauseRequested = false;
  firstJob.error = null;
  startCrawlRunTimer(firstJob);

  await persistCrawlJobState(firstJob);

  const persistCheckpoint = createCrawlPersistCheckpoint(firstJob);
  const runStartedAt = Date.now();
  const runStartedProcessed = firstJob.processed;
  let companyTimeoutTriggered = false;

  const shouldStop = () => {
    const currentJob = crawlJobs.get(jobId);
    return (
      !currentJob ||
      currentJob.pauseRequested ||
      companyTimeoutTriggered
    );
  };

  try {
    await withCrawlDbClient(async (client) => {
      await ensureCrawlPreviewTable(client);
      await ensureCrawlJobTables(client);
    });

    for (let index = firstJob.nextIndex; index < firstJob.total; index += 1) {
      const job = crawlJobs.get(jobId);
      if (!job) return;

      if (job.pauseRequested) {
        markCrawlJobPaused(job);
        await persistCrawlJobStateIfNeeded(job, persistCheckpoint, true);
        return;
      }

      if (
        index > firstJob.nextIndex &&
        (
          Date.now() - runStartedAt >= CRAWL_JOB_MAX_RUN_MS ||
          job.processed - runStartedProcessed >= CRAWL_JOB_MAX_ROWS_PER_RUN
        )
      ) {
        break;
      }

      const row = await withCrawlDbClient((client) =>
        fetchCrawlTargetByIndex(client, job.jobId, index)
      );

      if (!row) {
        job.failed += 1;
        job.processed += 1;
        job.nextIndex = index + 1;
        await persistCrawlJobStateIfNeeded(job, persistCheckpoint, true);
        continue;
      }

      job.currentCompany = normalizeNullableText(row.company);
      job.currentWebsiteUrl = normalizeNullableText(row.website_url);

      await withCrawlDbClient((client) =>
        markCrawlTargetStatus(client, job.jobId, index, "processing")
      );

      let pauseTriggered = false;
      let targetStatus: "done" | "skipped" | "failed" = "done";
      let statusReason: string | null = null;

      companyTimeoutTriggered = false;

      try {
        const selectedFieldSet = new Set(job.selectedFields);
        const websiteUrl = normalizeNullableText(row.website_url);

        if (!websiteUrl) {
          job.skipped += 1;
          targetStatus = "skipped";
          statusReason = "企業サイトURLが空です";
        } else {
          const currentRowData = await withCrawlDbClient((client) =>
            fetchSourceRowForPreview(client, String(row.row_id ?? ""))
          );

          if (!currentRowData) {
            job.failed += 1;
            targetStatus = "failed";
            statusReason = "DB上の元データが見つかりません";
          } else {
            const extracted = await runWithCrawlCompanyTimeout(
              crawlCompanyWebsite(
                websiteUrl,
                Array.from(selectedFieldSet),
                {
                  company: normalizeNullableText(row.company),
                  address: normalizeNullableText(row.address),
                },
                {
                  shouldStop,
                }
              ),
              () => {
                companyTimeoutTriggered = true;
              }
            );

            if (shouldStop()) {
              pauseTriggered = true;
            } else {
              const bundles = buildCrawlPayloadBundles(
                extracted,
                normalizeNullableText(row.company),
                normalizeNullableText(row.address)
              );

              const previewSourceRow = buildPreviewSourceRow(currentRowData);
              const rowPreviewItems: CrawlJobPreviewItem[] = [];

              bundles.forEach((bundle, officeIndex) => {
                const changes = buildPreviewChanges(
                  previewSourceRow as unknown as Record<string, unknown>,
                  bundle,
                  selectedFieldSet
                );

                if (changes.length === 0) {
                  return;
                }

                rowPreviewItems.push({
                  row_id: String(row.row_id ?? ""),
                  preview_row_id: `${String(row.row_id ?? "")}__${officeIndex}`,
                  officeIndex,
                  company: normalizeNullableText(row.company),
                  website_url: normalizeNullableText(row.website_url),
                  source_row: previewSourceRow,
                  bundle,
                });
              });

              if (rowPreviewItems.length > 0) {
                await withCrawlDbClient((client) =>
                  insertCrawlPreviewItems(client, job.jobId, rowPreviewItems)
                );

                job.updated += 1;
                targetStatus = "done";
              } else {
                job.skipped += 1;
                targetStatus = "skipped";
                statusReason = "取得候補なし、または既存値と同じため更新候補なし";
              }
            }
          }
        }
      } catch (error) {
        if (
          isCrawlCompanyTimeoutError(error) ||
          (companyTimeoutTriggered && !job.pauseRequested)
        ) {
          job.failed += 1;
          targetStatus = "failed";
          statusReason = `1社あたりの最大処理時間（${Math.round(
            CRAWL_COMPANY_TIMEOUT_MS / 1000
          )}秒）を超えたため、失敗扱いにして次の会社へ進みました`;
        } else if (isCrawlPausedError(error) || shouldStop()) {
          pauseTriggered = true;
        } else {
          job.failed += 1;
          targetStatus = "failed";
          statusReason =
            error instanceof Error
              ? error.message
              : "クローリング中に不明なエラーが発生しました";
        }
      } finally {
        if (!pauseTriggered) {
          await withCrawlDbClient((client) =>
            markCrawlTargetStatus(
              client,
              job.jobId,
              index,
              targetStatus,
              statusReason
            )
          );

          job.processed += 1;
          job.nextIndex = index + 1;
          await persistCrawlJobStateIfNeeded(job, persistCheckpoint, false);
        }
      }

      if (pauseTriggered) {
        const currentJob = crawlJobs.get(jobId);
        if (!currentJob) return;

        markCrawlJobPaused(currentJob);
        await persistCrawlJobStateIfNeeded(currentJob, persistCheckpoint, true);
        return;
      }
    }

    const job = crawlJobs.get(jobId);
    if (!job) return;

    stopCrawlRunTimer(job);

    if (job.nextIndex >= job.total) {
      job.completed = true;
      job.running = false;
      job.paused = false;
      job.pauseRequested = false;
      job.currentCompany = null;
      job.currentWebsiteUrl = null;
    } else {
      job.running = false;
      job.paused = false;
      job.pauseRequested = false;
      job.currentCompany = null;
      job.currentWebsiteUrl = null;
    }

    await persistCrawlJobStateIfNeeded(job, persistCheckpoint, true);
  } catch (error) {
    const job = crawlJobs.get(jobId);
    if (!job) return;

    if (isCrawlPausedError(error) || job.pauseRequested) {
      markCrawlJobPaused(job);
      await persistCrawlJobState(job);
      return;
    }

    const errorMessage =
      error instanceof Error
        ? error.message
        : "クローリング中にエラーが発生しました";

    if (isRetryableCrawlJobError(errorMessage)) {
      stopCrawlRunTimer(job);

      job.running = false;
      job.paused = false;
      job.pauseRequested = false;
      job.completed = false;
      job.currentCompany = null;
      job.currentWebsiteUrl = null;
      job.error = null;

      await persistCrawlJobState(job);
      return;
    }

    stopCrawlRunTimer(job);

    job.running = false;
    job.paused = false;
    job.pauseRequested = false;
    job.completed = false;
    job.currentCompany = null;
    job.currentWebsiteUrl = null;
    job.error = null;

    await persistCrawlJobState(job);
  }
}

async function savePreviewItems(
  client: DbClient,
  job: CrawlJobState,
  selectedChanges: SelectedCrawlChanges | undefined
) {
  await ensureMasterDataIdColumn(client);
  await ensureCrawlPreviewTable(client);

  let processed = 0;
  let updated = 0;
  let skipped = 0;
  let failed = 0;

  const sourceRowCache = new Map<string, Record<string, unknown> | null>();
  const items = await fetchAllCrawlPreviewItems(client, job.jobId);
  const processedPreviewRowIds: string[] = [];

  for (const item of items) {
    processed += 1;

    if (!sourceRowCache.has(item.row_id)) {
      sourceRowCache.set(
        item.row_id,
        await fetchSourceRowForInsert(client, item.row_id)
      );
    }

    try {
      if (!hasAnySelectedCandidate(item, selectedChanges, job.selectedFields)) {
        processedPreviewRowIds.push(item.preview_row_id);
        skipped += 1;
        continue;
      }

      const selectedKeys = new Set(
        getSelectedChangeKeys(item, selectedChanges, job.selectedFields)
      );

      const payload = buildSelectedPayload(
        item.bundle,
        item,
        selectedChanges,
        job.selectedFields
      );

      if (item.officeIndex === 0) {
        const updateSql = `
          UPDATE public.master_data
          SET
            "企業名" = CASE
              WHEN $15::boolean THEN COALESCE($1, "企業名")
              WHEN NULLIF(BTRIM(COALESCE("企業名"::text, '')), '') IS NULL
                THEN COALESCE($1, "企業名")
              ELSE "企業名"
            END,
            "企業サイトURL" = CASE
              WHEN NULLIF(BTRIM(COALESCE("企業サイトURL"::text, '')), '') IS NULL
                THEN COALESCE($2, "企業サイトURL")
              ELSE "企業サイトURL"
            END,
            "問い合わせフォームURL" = COALESCE($3, "問い合わせフォームURL"),
            "電話番号" = COALESCE($4, "電話番号"),
            "FAX番号" = COALESCE($5, "FAX番号"),
            "メールアドレス" = COALESCE($6, "メールアドレス"),
            "郵便番号" = COALESCE($7, "郵便番号"),
            "住所" = COALESCE($8, "住所"),
            "設立年月" = COALESCE($9, "設立年月"),
            "代表者名" = COALESCE($10, "代表者名"),
            "資本金" = COALESCE($11, "資本金"),
            "従業員数" = COALESCE($12, "従業員数"),
            "事業内容" = COALESCE($13, "事業内容"),
            "許可番号" = COALESCE($14, "許可番号")
          WHERE id = $16::bigint
        `;

        const updateValues = [
          selectedKeys.has("company") ? payload.company : null,
          selectedKeys.has("website_url") ? payload.website_url : null,
          selectedKeys.has("form_url") ? payload.form_url : null,
          selectedKeys.has("phone") ? payload.phone : null,
          selectedKeys.has("fax") ? payload.fax : null,
          selectedKeys.has("email") ? payload.email : null,
          selectedKeys.has("zipcode") ? payload.zipcode : null,
          selectedKeys.has("address") ? payload.address : null,
          selectedKeys.has("established_date") ? payload.established_date : null,
          selectedKeys.has("representative_name")
            ? payload.representative_name
            : null,
          selectedKeys.has("capital") ? payload.capital : null,
          selectedKeys.has("employee_count") ? payload.employee_count : null,
          selectedKeys.has("business_content") ? payload.business_content : null,
          selectedKeys.has("permit_number") ? payload.permit_number : null,
          selectedKeys.has("company") && item.bundle.forceCompanyUpdate,
          item.row_id,
        ];

        const updateRes = await client.query(updateSql, updateValues);

        if ((updateRes.rowCount ?? 0) > 0) {
          updated += 1;
          processedPreviewRowIds.push(item.preview_row_id);
        } else {
          skipped += 1;
          processedPreviewRowIds.push(item.preview_row_id);
        }

        continue;
      }

      const sourceRow = sourceRowCache.get(item.row_id);

      if (!sourceRow) {
        failed += 1;
        continue;
      }

      const insertRow = buildInsertRow(
        sourceRow,
        item.bundle,
        payload,
        selectedKeys
      );

      const insertColumns = MASTER_DATA_COLUMNS.map(
        (column) => `"${column}"`
      ).join(", ");
      const insertPlaceholders = MASTER_DATA_COLUMNS.map(
        (_, index) => `$${index + 1}`
      ).join(", ");

      const insertSql = `
        INSERT INTO public.master_data (${insertColumns})
        VALUES (${insertPlaceholders})
      `;

      const insertValues = MASTER_DATA_COLUMNS.map(
        (column) => insertRow[column] ?? null
      );

      await client.query(insertSql, insertValues);
      updated += 1;
      processedPreviewRowIds.push(item.preview_row_id);
    } catch {
      failed += 1;
    }
  }

  await deleteCrawlPreviewItemsByIds(client, job.jobId, processedPreviewRowIds);

  return { processed, updated, skipped, failed };
}

export async function POST(req: NextRequest) {
  let client: DbClient | null = null;

  try {
    const body = (await req.json()) as CrawlRequestBody;
    const action = body.action;

    const isWorkerAction =
      action === "worker_register" ||
      action === "worker_heartbeat" ||
      action === "worker_claim_job" ||
      action === "worker_claim_target" ||
      action === "worker_claim_targets" ||
      action === "worker_report_target" ||
      action === "worker_report_targets";

    if (!isWorkerAction) {
      const requiredPermission =
        action === "start_preview_job" ||
        action === "save_partial" ||
        action === "pause_job" ||
        action === "resume_job" ||
        action === "cancel_job"
          ? "inspection.crawl"
          : null;

      if (requiredPermission) {
        const permission = await requireMasterDataPermission(
          req,
          requiredPermission
        );

        if (permission.errorResponse) {
          return permission.errorResponse;
        }
      } else {
        const authError = requireMasterDataAuth(req);
        if (authError) return authError;
      }
    }

    if (
      action === "worker_register" ||
      action === "worker_heartbeat" ||
      action === "worker_claim_job" ||
      action === "worker_claim_target" ||
      action === "worker_claim_targets" ||
      action === "worker_report_target" ||
      action === "worker_report_targets"
    ) {
      assertWorkerAuthorized(req);

      const workerId = normalizeWorkerId(body.workerId);
      const workerName = normalizeWorkerName(body.workerName);

      if (!workerId) {
        return NextResponse.json(
          { ok: false, error: "workerId が空です" },
          { status: 400 }
        );
      }

      client = await pool.connect();

      try {
        await ensureCrawlJobTables(client);

        if (action === "worker_register" || action === "worker_heartbeat") {
          await registerWorker(
            client,
            workerId,
            workerName,
            action === "worker_register" ? "worker登録" : "heartbeat"
          );

          await client.query(
            `
              UPDATE public.master_data_crawl_jobs
              SET
                worker_id = $1,
                worker_heartbeat_at = now(),
                updated_at = now()
              WHERE completed = false
                AND pause_requested = false
                AND (
                  assigned_worker_id = $1
                  OR worker_id = $1
                )
            `,
            [workerId]
          );

          return NextResponse.json({
            ok: true,
            workerId,
            message: "worker確認完了",
          });
        }

        if (action === "worker_claim_job") {
          const claimedJobId = await claimWorkerJob(client, workerId, workerName);

          if (!claimedJobId) {
            return NextResponse.json({
              ok: true,
              jobId: null,
              message: "待機中のジョブはありません",
            });
          }

          return NextResponse.json({
            ok: true,
            jobId: claimedJobId,
            message: "ジョブを取得しました",
          });
        }

        if (action === "worker_claim_target") {
          const jobId = normalizeNullableText(body.jobId);

          if (!jobId) {
            return NextResponse.json(
              { ok: false, error: "jobId が空です" },
              { status: 400 }
            );
          }

          crawlJobs.delete(jobId);
          const job = await loadPersistedCrawlJobState(jobId);

          if (!job) {
            return NextResponse.json(
              { ok: false, error: "ジョブが見つかりません" },
              { status: 404 }
            );
          }

          const result = await claimWorkerTarget(client, job, workerId);

          return NextResponse.json({
            ok: true,
            ...result,
          });
        }

        if (action === "worker_claim_targets") {
          const jobId = normalizeNullableText(body.jobId);

          if (!jobId) {
            return NextResponse.json(
              { ok: false, error: "jobId が空です" },
              { status: 400 }
            );
          }

          crawlJobs.delete(jobId);
          const job = await loadPersistedCrawlJobState(jobId);

          if (!job) {
            return NextResponse.json(
              { ok: false, error: "ジョブが見つかりません" },
              { status: 404 }
            );
          }

          const result = await claimWorkerTargets(
            client,
            job,
            workerId,
            Number(body.targetLimit ?? 10)
          );

          return NextResponse.json({
            ok: true,
            ...result,
          });
        }

        if (action === "worker_report_target") {
          const jobId = normalizeNullableText(body.jobId);

          if (!jobId) {
            return NextResponse.json(
              { ok: false, error: "jobId が空です" },
              { status: 400 }
            );
          }

          crawlJobs.delete(jobId);
          const job = await loadPersistedCrawlJobState(jobId);

          if (!job) {
            return NextResponse.json(
              { ok: false, error: "ジョブが見つかりません" },
              { status: 404 }
            );
          }

          await reportWorkerTargetResult(client, job, workerId, body);

          return NextResponse.json({
            ok: true,
            message: "処理結果を保存しました",
          });
        }

        if (action === "worker_report_targets") {
          const jobId = normalizeNullableText(body.jobId);

          if (!jobId) {
            return NextResponse.json(
              { ok: false, error: "jobId が空です" },
              { status: 400 }
            );
          }

          if (!Array.isArray(body.targetResults)) {
            return NextResponse.json(
              { ok: false, error: "targetResults が空です" },
              { status: 400 }
            );
          }

          crawlJobs.delete(jobId);
          const job = await loadPersistedCrawlJobState(jobId);

          if (!job) {
            return NextResponse.json(
              { ok: false, error: "ジョブが見つかりません" },
              { status: 404 }
            );
          }

          await reportWorkerTargetResults(
            client,
            job,
            workerId,
            body.targetResults
          );

          return NextResponse.json({
            ok: true,
            paused: job.paused || job.pauseRequested,
            completed: job.completed,
            jobStatus: job.completed
              ? "completed"
              : job.paused || job.pauseRequested
              ? "paused"
              : "running",
            message: "処理結果をまとめて保存しました",
          });
        }

      } finally {
        client.release();
        client = null;
      }
    }

    if (action === "get_job_status") {
      const statusJobId = normalizeNullableText(body.jobId);
      const isLocal = isLocalDevelopmentRequest(req);

      if (!isLocal && statusJobId) {
        crawlJobs.delete(statusJobId);
      }

      const job = await getCrawlJobFromMemoryOrFile(statusJobId);

      if (!job) {
        return NextResponse.json(
          { ok: false, error: "クローリングジョブが見つかりません" },
          { status: 404 }
        );
      }

      const shouldContinueJob =
        !job.completed &&
        !job.pauseRequested &&
        !job.paused &&
        !job.error;

      if (shouldContinueJob) {
        if (isLocal && !runningCrawlJobIds.has(job.jobId)) {
          job.running = true;
          job.paused = false;
          startCrawlRunTimer(job);
          await persistCrawlJobState(job);
          startCrawlJobRunner(job.jobId);
        } else {
          await persistCrawlJobState(job);
        }
      }

      await syncCrawlElapsedMsFromTargets(job);

      const includePreviewRows =
        job.paused || job.completed || !!job.error;

      if (!includePreviewRows) {
        const response = NextResponse.json(buildJobResponse(job));

        if (shouldRefreshCrawlProgressAuthCookie(job)) {
          refreshMasterDataAuthCookie(response, req);
        }

        return response;
      }

      client = await pool.connect();

      const previewResult = await buildPublicPreviewRows(
        client,
        job,
        body.previewPage ?? 1,
        body.previewPageSize ?? DEFAULT_CRAWL_PREVIEW_PAGE_SIZE,
        body.previewTab ?? "candidate"
      );

      const response = NextResponse.json({
        ...buildJobResponse(job),
        previewRows: previewResult.rows,
        previewTotal: previewResult.total,
        previewPage: previewResult.page,
        previewPageSize: previewResult.pageSize,
      });

      if (shouldRefreshCrawlProgressAuthCookie(job)) {
        refreshMasterDataAuthCookie(response, req);
      }

      return response;
    }

    if (action === "cancel_job") {
      const job = await getCrawlJobFromMemoryOrFile(body.jobId ?? null);

      if (!job) {
        return NextResponse.json(
          { ok: false, error: "クローリングジョブが見つかりません" },
          { status: 404 }
        );
      }

      client = await pool.connect();

      job.pauseRequested = false;
      job.running = false;
      job.paused = false;
      job.completed = false;
      job.error = null;
      job.currentCompany = null;
      job.currentWebsiteUrl = null;
      job.previewItems = [];
      job.savedPreviewRowIds = new Set<string>();

      await deleteAllCrawlPreviewItems(client, job.jobId);

      await client.query(
        `DELETE FROM public.master_data_crawl_targets WHERE job_id = $1`,
        [job.jobId]
      );

      crawlJobs.delete(job.jobId);
      await deletePersistedCrawlJobState(job.jobId);

      return NextResponse.json({
        ok: true,
        message: "クローリングを中止しました",
      });
    }

    if (action === "pause_job") {
      const job = await getCrawlJobFromMemoryOrFile(body.jobId ?? null);

      if (!job) {
        return NextResponse.json(
          { ok: false, error: "クローリングジョブが見つかりません" },
          { status: 404 }
        );
      }

      job.pauseRequested = true;
      markCrawlJobPaused(job);

      await persistCrawlJobState(job);

      client = await pool.connect();

      await updateJobWorkerColumns(client, job.jobId, {
        status: "paused",
        message: "一時停止中",
      });

      return NextResponse.json({
        ...buildJobResponse(job),
        message: "クローリングを中断しました。保存できます。",
      });
    }

    if (action === "resume_job") {
      const job = await getCrawlJobFromMemoryOrFile(body.jobId ?? null);

      if (!job) {
        return NextResponse.json(
          { ok: false, error: "クローリングジョブが見つかりません" },
          { status: 404 }
        );
      }

      if (job.completed) {
        return NextResponse.json({
          ...buildJobResponse(job),
          message: "すでに完了しています",
        });
      }

      const listScopeFilters = await getListScopeFiltersForRequest(req);
      const assignedWorkerId = normalizeWorkerId(body.assignedWorkerId);
      const isLocal = isLocalDevelopmentRequest(req);

      if (!assignedWorkerId && !isLocal) {
        return NextResponse.json(
          {
            ok: false,
            error:
              "このPCのworkerが起動していません。master-crawl-worker.exe を起動してから再度実行してください。",
          },
          { status: 400 }
        );
      }

      client = await pool.connect();

      job.pauseRequested = false;
      job.paused = false;
      job.running = false;
      job.error = null;
      job.currentCompany = null;
      job.currentWebsiteUrl = null;

      await persistCrawlJobState(job);

      if (assignedWorkerId) {
        await updateJobWorkerColumns(client, job.jobId, {
          status: "queued",
          assignedWorkerId,
          message: "worker再開待機中",
        });

        return NextResponse.json({
          ...buildJobResponse(job),
          jobStatus: "running",
          message: "workerに再開を予約しました",
        });
      }

      // localhostでは従来通りNext.js内部runnerで再開する
      startCrawlJobRunner(job.jobId);

      return NextResponse.json({
        ...buildJobResponse(job),
        jobStatus: "running",
        message: "ローカル環境でクローリングを再開しました",
      });
    }

    if (action === "save_partial") {
      const job = await getCrawlJobFromMemoryOrFile(body.jobId ?? null);

      if (!job) {
        return NextResponse.json(
          { ok: false, error: "クローリングジョブが見つかりません" },
          { status: 404 }
        );
      }

      if (job.running) {
        return NextResponse.json(
          { ok: false, error: "保存前にクローリングを中断してください" },
          { status: 400 }
        );
      }

      client = await pool.connect();

      const result = await savePreviewItems(client, job, body.selectedChanges);
      const remainingCount = Math.max(job.total - job.nextIndex, 0);

      const previewResult = await buildPublicPreviewRows(
        client,
        job,
        1,
        DEFAULT_CRAWL_PREVIEW_PAGE_SIZE,
        body.previewTab ?? "candidate"
      );

      if (remainingCount === 0 && previewResult.total === 0) {
        await deleteAllCrawlPreviewItems(client, job.jobId);
        crawlJobs.delete(job.jobId);
        await deletePersistedCrawlJobState(job.jobId);
      } else {
        await persistCrawlJobState(job);
      }

      return NextResponse.json({
        ok: true,
        ...result,
        remainingCount,
        previewRows: previewResult.rows,
        previewTotal: previewResult.total,
        previewPage: previewResult.page,
        previewPageSize: previewResult.pageSize,
        message: `保存完了：対象 ${result.processed} 件 / 更新 ${result.updated} 件 / スキップ ${result.skipped} 件 / 失敗 ${result.failed} 件`,
      });
    }

    if (action === "start_preview_job") {
      client = await pool.connect();

      const selectedFieldSet = normalizeSelectedFields(body.selectedFields);
      const selectedFields = Array.from(selectedFieldSet);

      if (
        action === "start_preview_job" ||
        action === "save_partial"
      ) {
        for (const selectedField of selectedFields) {
          const fieldPermission = await requireMasterDataPermission(
            req,
            `inspection.crawlField.${selectedField}` as MasterDataPermissionKey
          );

          if (fieldPermission.errorResponse) {
            return fieldPermission.errorResponse;
          }
        }
      }

      for (const selectedField of selectedFields) {
        const fieldPermission = await requireMasterDataPermission(
          req,
          `inspection.crawlField.${selectedField}` as MasterDataPermissionKey
        );

        if (fieldPermission.errorResponse) {
          return fieldPermission.errorResponse;
        }
      }

      const assignedWorkerId = normalizeWorkerId(body.assignedWorkerId);
      const isLocal = isLocalDevelopmentRequest(req);
      const listScopeFilters = await getListScopeFiltersForRequest(req);

      if (!assignedWorkerId && !isLocal) {
        return NextResponse.json(
          {
            ok: false,
            error:
              "このPCのworkerが起動していません。master-crawl-worker.exe を起動してから再度実行してください。",
          },
          { status: 400 }
        );
      }

      const jobId = crypto.randomUUID();
      const total = await createCrawlTargets(
        client,
        jobId,
        body,
        listScopeFilters
      );

      if (total === 0) {
        return NextResponse.json({
          ok: true,
          jobId: null,
          totalTargets: 0,
          processed: 0,
          updated: 0,
          skipped: 0,
          failed: 0,
          previewRows: [],
          message: "対象がありませんでした",
        });
      }

      await ensureCrawlPreviewTable(client);
      await ensureCrawlJobTables(client);

      const job: CrawlJobState = {
        jobId,
        selectedFields,
        selectedFieldLabels: buildSelectedFieldLabels(selectedFields),
        nextIndex: 0,
        total,
        processed: 0,
        updated: 0,
        skipped: 0,
        failed: 0,
        currentCompany: null,
        currentWebsiteUrl: null,
        running: true,
        paused: false,
        pauseRequested: false,
        completed: false,
        error: null,
        elapsedMs: 0,
        lastStartedAt: null,
        previewItems: [],
        excludedPreviewRows: [],
        savedPreviewRowIds: new Set<string>(),
        sortKey: body.sortKey,
        sortDirection: body.sortDirection,
      };

      crawlJobs.set(jobId, job);

      await persistCrawlJobState(job);

      if (assignedWorkerId) {
        await updateJobWorkerColumns(client, jobId, {
          status: "queued",
          assignedWorkerId,
          message: "worker待機中",
        });

        return NextResponse.json({
          ...buildJobResponse(job),
          jobStatus: "running",
          message: "クローリングをworkerに予約しました",
        });
      }

      // localhostでは従来通りNext.js内部runnerで実行する
      startCrawlJobRunner(jobId);

      return NextResponse.json({
        ...buildJobResponse(job),
        jobStatus: "running",
        message: "ローカル環境でクローリングを開始しました",
      });
    }

    return NextResponse.json(
      { ok: false, error: "不正なリクエストです" },
      { status: 400 }
    );
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error:
          error instanceof Error
            ? error.message
            : "クローリングでエラーが発生しました",
      },
      { status: 500 }
    );
  } finally {
    client?.release();
  }
}