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

type CrawlRequestBody = {
  action?:
    | "start_preview_job"
    | "get_job_status"
    | "pause_job"
    | "resume_job"
    | "cancel_job"
    | "save_partial";
  jobId?: string | null;
  filterModels?: Partial<Record<FilterKey, FilterModel>>;
  advancedFilters?: AdvancedFilters;
  sortKey?: FilterKey | null;
  sortDirection?: SortDirection | "" | null;
  previewOnly?: boolean;
  selectedChanges?: SelectedCrawlChanges;
  selectedFields?: CrawlSelectableFieldKey[];
  previewPage?: number;
  previewPageSize?: number;
  previewTab?: "candidate" | "excluded";
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
  forceCompanyUpdate: boolean;
};

function normalizeNullableText(value: unknown) {
  if (value == null) return null;
  const text = String(value).trim();
  return text === "" ? null : text;
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
  sourceCompany: string | null
): CrawlPayloadBundle[] {
  const extractedCompany = normalizeNullableText(extracted.company);
  const fallbackCompany = normalizeNullableText(sourceCompany);
  const baseCompany =
    extractedCompany && isLikelyCompanyName(extractedCompany)
      ? extractedCompany
      : fallbackCompany;

  const officeSources: CrawlExtractedOffice[] =
    Array.isArray(extracted.offices) && extracted.offices.length > 0
      ? extracted.offices
      : [
          {
            office_name: null,
            company: baseCompany,
            phone_candidates: extracted.phone ? [extracted.phone] : [],
            fax_candidates: extracted.fax ? [extracted.fax] : [],
            email_candidates: extracted.email ? [extracted.email] : [],
            zipcode_candidates: extracted.zipcode ? [extracted.zipcode] : [],
            address_candidates: extracted.address ? [extracted.address] : [],
          },
        ];

  return officeSources.map((office) => {
    const officeName = normalizeNullableText(office.office_name);
    const officeCompanyBase =
      normalizeNullableText(office.company) ?? baseCompany ?? fallbackCompany;

    const company =
      officeName && officeCompanyBase && !officeCompanyBase.includes(officeName)
        ? `${officeCompanyBase} ${officeName}`
        : officeCompanyBase;

    const payload: CrawlPayload = {
      company: company && isLikelyCompanyName(company) ? company : fallbackCompany,
      website_url: normalizeNullableText(extracted.website_url),
      form_url: normalizeNullableText(extracted.form_url),
      phone: normalizeNullableText(extracted.phone),
      fax: normalizeNullableText(extracted.fax),
      email: normalizeNullableText(extracted.email),
      zipcode: normalizeNullableText(extracted.zipcode),
      address: normalizeNullableText(extracted.address),
      established_date: normalizeNullableText(extracted.established_date),
      representative_name: normalizeNullableText(
        extracted.representative_name
      ),
      representative_name_raw: normalizeNullableText(
        extracted.representative_name_raw
      ),
      representative_name_reason: normalizeNullableText(
        extracted.representative_name_reason
      ),
      representative_title: normalizeNullableText(
        extracted.representative_title
      ),
      capital: normalizeNullableText(extracted.capital),
      employee_count: normalizeNullableText(extracted.employee_count),
      business_content: normalizeNullableText(extracted.business_content),
      permit_number: normalizeNullableText(extracted.permit_number),
    };

    return {
      payload,
      candidates: {
        phone: uniqueTextValues(office.phone_candidates),
        fax: uniqueTextValues(office.fax_candidates),
        email: uniqueTextValues(office.email_candidates),
        zipcode: uniqueTextValues(office.zipcode_candidates),
        address: uniqueTextValues(office.address_candidates),
      },
      forceCompanyUpdate: officeSources.length > 1 || officeName !== null,
    };
  });
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
  return change.candidates.length > 1
    ? null
    : normalizeNullableText(change.after) ??
        normalizeNullableText(change.candidates[0]) ??
        null;
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

  return {
    ...bundle.payload,
    company: selected.company ?? bundle.payload.company,
    website_url: selected.website_url ?? bundle.payload.website_url,
    phone: selected.phone ?? bundle.payload.phone,
    fax: selected.fax ?? bundle.payload.fax,
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
};

const crawlJobs =
  globalForCrawlJobs.__masterDataCrawlJobs ??
  new Map<string, CrawlJobState>();

if (!globalForCrawlJobs.__masterDataCrawlJobs) {
  globalForCrawlJobs.__masterDataCrawlJobs = crawlJobs;
}

const CRAWL_PAUSED_ERROR_MESSAGE = "__MASTER_DATA_CRAWL_PAUSED__";

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

const CRAWL_JOB_PROGRESS_SAVE_EVERY = 10;
const CRAWL_JOB_PROGRESS_SAVE_INTERVAL_MS = 2000;

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
      sort_key text,
      sort_direction text,
      created_at timestamptz NOT NULL DEFAULT now(),
      updated_at timestamptz NOT NULL DEFAULT now()
    )
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

async function fetchCrawlPreviewRowIdSet(client: DbClient, jobId: string) {
  await ensureCrawlPreviewTable(client);

  const res = await client.query(
    `
      SELECT DISTINCT row_id::text AS row_id
      FROM public.master_data_crawl_preview
      WHERE job_id = $1
    `,
    [jobId]
  );

  return new Set(res.rows.map((row) => String(row.row_id ?? "")));
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
          sort_key,
          sort_direction,
          updated_at
        )
        VALUES (
          $1, $2::jsonb, $3::jsonb, $4, $5, $6, $7, $8, $9,
          $10, $11, $12, $13, $14, $15, $16, $17, $18, now()
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
      running: false,
      paused: !!row.paused,
      pauseRequested: false,
      completed: !!row.completed,
      error: normalizeNullableText(row.error),
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

function markCrawlJobPaused(job: CrawlJobState) {
  job.running = false;
  job.paused = true;
  job.currentCompany = null;
  job.currentWebsiteUrl = null;
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
  previewTab: "candidate" | "excluded" = "candidate"
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
        row_id::text,
        company,
        website_url,
        skip_reason,
        error_message
      FROM public.master_data_crawl_targets AS t
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
        address: null,
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
    updated: job.updated,
    skipped: job.skipped,
    failed: job.failed,
    currentCompany: job.currentCompany,
    currentWebsiteUrl: job.currentWebsiteUrl,
    currentFields: job.selectedFieldLabels,
    progressPercent:
      job.total === 0 ? 0 : Math.round((job.processed / job.total) * 100),
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

async function createCrawlTargets(
  client: DbClient,
  jobId: string,
  body: CrawlRequestBody
) {
  await ensureMasterDataIdColumn(client);
  await ensureCrawlJobTables(client);

  const filterModels = body.filterModels ?? {};
  const advancedFilters = body.advancedFilters ?? {};
  const { whereSql, params } = buildWhereClause(filterModels, advancedFilters);
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

async function markCrawlTargetStatus(
  client: DbClient,
  jobId: string,
  targetIndex: number,
  status: "processing" | "done" | "skipped" | "failed",
  reason?: string | null
) {
  await client.query(
    `
      UPDATE public.master_data_crawl_targets
      SET
        status = $3,
        skip_reason = CASE WHEN $3 = 'skipped' THEN $4 ELSE skip_reason END,
        error_message = CASE WHEN $3 = 'failed' THEN $4 ELSE error_message END,
        started_at = CASE WHEN $3 = 'processing' THEN now() ELSE started_at END,
        finished_at = CASE WHEN $3 IN ('done', 'skipped', 'failed') THEN now() ELSE finished_at END
      WHERE job_id = $1
        AND target_index = $2
    `,
    [jobId, targetIndex, status, reason ?? null]
  );
}

async function runCrawlJob(jobId: string) {
  const firstJob = await getCrawlJobFromMemoryOrFile(jobId);
  if (!firstJob || firstJob.running || firstJob.completed) return;

  firstJob.running = true;
  firstJob.paused = false;
  firstJob.pauseRequested = false;
  firstJob.error = null;

  await persistCrawlJobState(firstJob);
  const persistCheckpoint = createCrawlPersistCheckpoint(firstJob);

  const shouldStop = () => {
    const currentJob = crawlJobs.get(jobId);
    return !currentJob || currentJob.pauseRequested;
  };

  let client: DbClient | null = null;

  try {
    client = await pool.connect();
    await ensureCrawlPreviewTable(client);
    await ensureCrawlJobTables(client);

    for (let index = firstJob.nextIndex; index < firstJob.total; index += 1) {
      const job = crawlJobs.get(jobId);
      if (!job) return;

      if (job.pauseRequested) {
        markCrawlJobPaused(job);
        await persistCrawlJobStateIfNeeded(job, persistCheckpoint, true);
        return;
      }

      const row = await fetchCrawlTargetByIndex(client, job.jobId, index);

      if (!row) {
        job.failed += 1;
        job.processed += 1;
        job.nextIndex = index + 1;
        await persistCrawlJobStateIfNeeded(job, persistCheckpoint, true);
        continue;
      }

      job.currentCompany = normalizeNullableText(row.company);
      job.currentWebsiteUrl = normalizeNullableText(row.website_url);

      await markCrawlTargetStatus(client, job.jobId, index, "processing");

      let pauseTriggered = false;
      let targetStatus: "done" | "skipped" | "failed" = "done";
      let statusReason: string | null = null;

      try {
        const selectedFieldSet = new Set(job.selectedFields);
        const websiteUrl = normalizeNullableText(row.website_url);

        if (!websiteUrl) {
          job.skipped += 1;
          targetStatus = "skipped";
          statusReason = "企業サイトURLが空です";
        } else {
          const currentRowData = await fetchSourceRowForPreview(
            client,
            String(row.row_id ?? "")
          );

          if (!currentRowData) {
            job.failed += 1;
            targetStatus = "failed";
            statusReason = "DB上の元データが見つかりません";
          } else {
            const extracted = await crawlCompanyWebsite(
              websiteUrl,
              Array.from(selectedFieldSet),
              {
                company: normalizeNullableText(row.company),
                address: normalizeNullableText(row.address),
              },
              {
                shouldStop,
              }
            );

            if (shouldStop()) {
              pauseTriggered = true;
            } else {
              const bundles = buildCrawlPayloadBundles(
                extracted,
                normalizeNullableText(row.company)
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
                  company:
                    bundle.payload.company ??
                    normalizeNullableText(row.company),
                  website_url: normalizeNullableText(row.website_url),
                  source_row: previewSourceRow,
                  bundle,
                });
              });

              if (rowPreviewItems.length > 0) {
                await insertCrawlPreviewItems(client, job.jobId, rowPreviewItems);
                job.updated += rowPreviewItems.length;
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
        if (isCrawlPausedError(error) || shouldStop()) {
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
          await markCrawlTargetStatus(
            client,
            job.jobId,
            index,
            targetStatus,
            statusReason
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

    job.completed = true;
    job.running = false;
    job.paused = false;
    job.pauseRequested = false;
    job.currentCompany = null;
    job.currentWebsiteUrl = null;

    await persistCrawlJobStateIfNeeded(job, persistCheckpoint, true);
  } catch (error) {
    const job = crawlJobs.get(jobId);
    if (!job) return;

    if (isCrawlPausedError(error) || job.pauseRequested) {
      markCrawlJobPaused(job);
      await persistCrawlJobState(job);
      return;
    }

    job.running = false;
    job.paused = true;
    job.completed = false;
    job.currentCompany = null;
    job.currentWebsiteUrl = null;
    job.error =
      error instanceof Error
        ? error.message
        : "クローリング中にエラーが発生しました";

    await persistCrawlJobState(job);
  } finally {
    client?.release();
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

    if (action === "get_job_status") {
      const job = await getCrawlJobFromMemoryOrFile(body.jobId ?? null);

      if (!job) {
        return NextResponse.json(
          { ok: false, error: "クローリングジョブが見つかりません" },
          { status: 404 }
        );
      }

      if (!job.completed && !job.running && !job.paused && !job.pauseRequested) {
        job.error = null;
        job.paused = false;
        crawlJobs.set(job.jobId, job);
        void runCrawlJob(job.jobId);
      }

      const includePreviewRows =
        job.paused || job.completed || !!job.error;

      if (!includePreviewRows) {
        return NextResponse.json(buildJobResponse(job));
      }

      client = await pool.connect();

      const previewResult = await buildPublicPreviewRows(
        client,
        job,
        body.previewPage ?? 1,
        body.previewPageSize ?? DEFAULT_CRAWL_PREVIEW_PAGE_SIZE,
        body.previewTab ?? "candidate"
      );

      return NextResponse.json({
        ...buildJobResponse(job),
        previewRows: previewResult.rows,
        previewTotal: previewResult.total,
        previewPage: previewResult.page,
        previewPageSize: previewResult.pageSize,
      });
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

      await persistCrawlJobState(job);

      return NextResponse.json({
        ...buildJobResponse(job),
        message: "中断指示を受け付けました",
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

      job.pauseRequested = false;
      job.paused = false;
      job.error = null;

      await persistCrawlJobState(job);

      void runCrawlJob(job.jobId);

      return NextResponse.json({
        ...buildJobResponse(job),
        jobStatus: "running",
        message: "途中から再開しました",
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

      const jobId = crypto.randomUUID();
      const total = await createCrawlTargets(client, jobId, body);

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
        running: false,
        paused: false,
        pauseRequested: false,
        completed: false,
        error: null,
        previewItems: [],
        excludedPreviewRows: [],
        savedPreviewRowIds: new Set<string>(),
        sortKey: body.sortKey,
        sortDirection: body.sortDirection,
      };

      crawlJobs.set(jobId, job);

      await persistCrawlJobState(job);

      void runCrawlJob(jobId);

      return NextResponse.json({
        ...buildJobResponse(job),
        jobStatus: "running",
        message: "クローリングを開始しました",
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