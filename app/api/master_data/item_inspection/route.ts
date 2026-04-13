import { randomUUID } from "crypto";
import { NextRequest, NextResponse } from "next/server";
import { dbReady, pool } from "@/lib/db";

export const dynamic = "force-dynamic";
export const fetchCache = "force-no-store";
export const runtime = "nodejs";

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

type RepresentativeNameInspectionPreviewChange = {
  rowId: string;
  company: string | null;
  fieldLabel: string;
  beforeValue: string | null;
  afterValue: string | null;
  action: "update" | "delete" | "review";
  reason: string;
};

type InspectionTargetRow = {
  rowId: string;
  company: string | null;
  representativeName: string | null;
};

type ItemInspectionJobStatus = "running" | "paused" | "completed" | "error";

type ItemInspectionJob = {
  id: string;
  status: ItemInspectionJobStatus;
  createdAt: number;
  updatedAt: number;
  targetRows: InspectionTargetRow[];
  currentIndex: number;
  totalTargets: number;
  processed: number;
  updated: number;
  skipped: number;
  failed: number;
  currentCompany: string | null;
  currentInspectionValue: string | null;
  currentInspectionFieldLabel: string | null;
  inspectionPreviewChanges: RepresentativeNameInspectionPreviewChange[];
  error: string | null;
  pauseRequested: boolean;
  cancelRequested: boolean;
};

const itemInspectionJobs = new Map<string, ItemInspectionJob>();
const JOB_TTL_MS = 1000 * 60 * 30;

async function ensureMasterDataIdColumn(
  client: { query: (sql: string) => Promise<unknown> }
) {
  await client.query(`
    ALTER TABLE public.master_data
    ADD COLUMN IF NOT EXISTS id BIGSERIAL
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

function scheduleJobCleanup(jobId: string, delayMs = JOB_TTL_MS) {
  setTimeout(() => {
    itemInspectionJobs.delete(jobId);
  }, delayMs);
}

function parseFilterModels(searchParams: URLSearchParams) {
  const raw = searchParams.get("filterModels");
  if (!raw) return {} as Partial<Record<FilterKey, FilterModel>>;

  try {
    return JSON.parse(raw) as Partial<Record<FilterKey, FilterModel>>;
  } catch {
    return {} as Partial<Record<FilterKey, FilterModel>>;
  }
}

function parseAdvancedFilters(searchParams: URLSearchParams) {
  const raw = searchParams.get("advancedFilters");
  if (!raw) return {} as AdvancedFilters;

  try {
    return JSON.parse(raw) as AdvancedFilters;
  } catch {
    return {} as AdvancedFilters;
  }
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

const INDUSTRY_PARENT_SORT_ORDER = [
  "農業、林業",
  "漁業",
  "鉱業、採石業、砂利採取業",
  "建設業",
  "製造業",
  "電気・ガス・熱供給・水道業",
  "情報通信業",
  "運輸業、郵便業",
  "卸売業、小売業",
  "金融業、保険業",
  "不動産業、物品賃貸業",
  "学術研究、専門・技術サービス業",
  "宿泊業、飲食サービス業",
  "生活関連サービス業、娯楽業",
  "教育、学習支援業",
  "医療、福祉",
  "複合サービス事業",
  "サービス業（他に分類されないもの）",
  "公務",
  "その他",
] as const;

function resolveIndustryParent(bigIndustry: string) {
  const value = bigIndustry.trim();

  if (value === "") return "その他";
  if (/製造業|製造|工業/.test(value)) return "製造業";
  if (/建設/.test(value)) return "建設業";
  if (/農業|林業/.test(value)) return "農業、林業";
  if (/漁業/.test(value)) return "漁業";
  if (/鉱業|採石|砂利/.test(value)) return "鉱業、採石業、砂利採取業";
  if (/電気|ガス|熱供給|水道/.test(value))
    return "電気・ガス・熱供給・水道業";
  if (/情報|通信|IT|ソフトウェア/.test(value)) return "情報通信業";
  if (/運輸|郵便|物流|運送/.test(value)) return "運輸業、郵便業";
  if (/卸売|小売|販売/.test(value)) return "卸売業、小売業";
  if (/金融|保険/.test(value)) return "金融業、保険業";
  if (/不動産|賃貸/.test(value)) return "不動産業、物品賃貸業";
  if (/学術|研究|専門|技術|士業/.test(value))
    return "学術研究、専門・技術サービス業";
  if (/宿泊|飲食/.test(value)) return "宿泊業、飲食サービス業";
  if (/生活関連|娯楽|理容|美容|クリーニング/.test(value))
    return "生活関連サービス業、娯楽業";
  if (/教育|学習支援|学校/.test(value)) return "教育、学習支援業";
  if (/医療|福祉|介護/.test(value)) return "医療、福祉";
  if (/複合サービス/.test(value)) return "複合サービス事業";
  if (/公務/.test(value)) return "公務";
  if (/サービス/.test(value)) return "サービス業（他に分類されないもの）";

  if ((INDUSTRY_PARENT_SORT_ORDER as readonly string[]).includes(value)) {
    return value;
  }

  return "その他";
}

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
  params: (string | number)[],
  expression: string,
  values: string[]
) {
  const normalized = toStringArray(values);
  if (normalized.length === 0) return "";

  const placeholders = normalized.map((value) => {
    params.push(value);
    return `$${params.length}`;
  });

  return `${expression} IN (${placeholders.join(", ")})`;
}

function addNumericRangeClause(
  where: string[],
  params: (string | number)[],
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
  params: (string | number)[],
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
  addNumericRangeClause(
    where,
    params,
    EMPLOYEE_NUMERIC_EXPR,
    filters.employeeCount
  );

  const selectedTagParents = toStringArray(filters.tags?.parents ?? []);
  const selectedTags = toStringArray(filters.tags?.tags ?? []);

  if (selectedTagParents.length > 0 || selectedTags.length > 0) {
    const tagConditions: string[] = [];

    if (selectedTags.length > 0) {
      const placeholders = selectedTags.map((value) => {
        params.push(value);
        return `$${params.length}`;
      });
      tagConditions.push(`BTRIM(tag_value) IN (${placeholders.join(", ")})`);
    }

    if (selectedTagParents.length > 0) {
      const placeholders = selectedTagParents.map((value) => {
        params.push(value);
        return `$${params.length}`;
      });
      tagConditions.push(
        `${TAG_PARENT_CASE_FROM_SPLIT} IN (${placeholders.join(", ")})`
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

function buildBlankCheckClause(column: string) {
  return `NULLIF(regexp_replace(COALESCE(${column}::text, ''), '[\\s　]+', '', 'g'), '') IS NULL`;
}

function buildNotBlankCheckClause(column: string) {
  return `NULLIF(regexp_replace(COALESCE(${column}::text, ''), '[\\s　]+', '', 'g'), '') IS NOT NULL`;
}

function addConditionClause(
  where: string[],
  params: (string | number)[],
  column: string,
  model?: FilterModel
) {
  if (!model) return;

  const type = model.conditionType || "";
  const value = (model.conditionValue || "").trim();
  const textColumn = `COALESCE(${column}::text, '')`;

  if (type === "") return;

  if (type === "is_empty") {
    where.push(buildBlankCheckClause(column));
    return;
  }

  if (type === "is_not_empty") {
    where.push(buildNotBlankCheckClause(column));
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
    pieces.push(buildBlankCheckClause(column));
  }

  if (pieces.length > 0) {
    where.push(`(${pieces.join(" OR ")})`);
  }
}

function buildWhereClause(
  searchParams: URLSearchParams,
  skipKey?: FilterKey
) {
  const filterModels = parseFilterModels(searchParams);
  const advancedFilters = parseAdvancedFilters(searchParams);
  const where: string[] = [];
  const params: (string | number)[] = [];

  (Object.entries(FILTER_COLUMN_MAP) as [FilterKey, string][]).forEach(
    ([key, column]) => {
      if (skipKey && key === skipKey) return;

      const model = filterModels[key];
      addConditionClause(where, params, column, model);
      addValueFilterClause(where, params, column, model);
    }
  );

  addAdvancedFilterClauses(where, params, advancedFilters);

  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  return { whereSql, params };
}

function buildSearchParamsFromReadPayload(payload: Record<string, unknown>) {
  const searchParams = new URLSearchParams();

  const simpleKeys = [
    "page",
    "limit",
    "sortKey",
    "sortDirection",
    "valuesFor",
    "advancedValuesFor",
    "valueSearch",
    "valueOffset",
    "valueLimit",
    "currentValueFilterEnabled",
  ] as const;

  simpleKeys.forEach((key) => {
    const value = payload[key];
    if (value !== undefined && value !== null && value !== "") {
      searchParams.set(key, String(value));
    }
  });

  if ("filterModels" in payload) {
    searchParams.set(
      "filterModels",
      JSON.stringify(payload.filterModels ?? {})
    );
  }

  if ("advancedFilters" in payload) {
    searchParams.set(
      "advancedFilters",
      JSON.stringify(payload.advancedFilters ?? {})
    );
  }

  if ("currentSelectedValues" in payload) {
    searchParams.set(
      "currentSelectedValues",
      JSON.stringify(
        Array.isArray(payload.currentSelectedValues)
          ? payload.currentSelectedValues
          : []
      )
    );
  }

  return searchParams;
}

const REPRESENTATIVE_TITLE_REGEX =
  /代表取締役会長CEO|代表取締役社長COO|代表取締役副社長|代表取締役専務|代表取締役常務|代表取締役|取締役会長|取締役社長|取締役副社長|取締役専務|取締役常務|取締役|会長|社長|副社長|専務|常務|執行役員|監査役|理事長|院長|所長|支店長|本部長|部長|課長|店長|工場長|センター長|室長|主任|係長|担当役員|担当者|担当|責任者|マネージャー|CEO|COO|CFO|CTO|CMO/gu;

const REPRESENTATIVE_STOPWORDS = [
  "営業",
  "採用",
  "人事",
  "総務",
  "経理",
  "広報",
  "受付",
  "窓口",
  "担当者",
  "責任者",
  "事務局",
  "センター",
  "グループ",
  "取締役",
  "執行役員",
  "監査役",
  "理事長",
  "院長",
  "支店長",
  "本部長",
  "工場長",
  "マネージャー",
  "manager",
  "mgr",
  "ceo",
  "coo",
  "cfo",
  "cto",
] as const;

const REPRESENTATIVE_NON_NAME_EXACT_VALUES = new Set([
  "不明",
  "ふめい",
  "未定",
  "なし",
  "無し",
  "該当なし",
  "担当者不明",
  "代表者不明",
  "各位",
  "御中",
  "一同",
]);

const REPRESENTATIVE_AREA_NAME_TOKENS = new Set<string>([
  ...PREFECTURE_NAMES,
  "北海道",
  "東北",
  "関東",
  "中部",
  "近畿",
  "関西",
  "中国",
  "四国",
  "九州",
  "沖縄",
  "東海",
  "札幌",
  "仙台",
  "東京",
  "横浜",
  "川崎",
  "相模原",
  "新潟",
  "静岡",
  "浜松",
  "名古屋",
  "京都",
  "大阪",
  "堺",
  "神戸",
  "岡山",
  "広島",
  "北九州",
  "福岡",
  "熊本",
]);

const REPRESENTATIVE_STRICT_NON_NAME_AREA_TOKENS = new Set<string>([
  "北海道",
  "東北",
  "関東",
  "中部",
  "近畿",
  "関西",
  "中国",
  "四国",
  "九州",
  "沖縄",
  "東海",
  "名古屋",
  "北名古屋",
  "北九州",
  "伊勢志摩",
  "東近江",
  "西三河",
  "東三河",
]);

const REPRESENTATIVE_STRONG_NAME_TOKEN_REGEX =
  /^(?:[\p{sc=Han}々ヶヵ]{1,5}|[\p{sc=Hiragana}]{2,8}|[\p{sc=Katakana}ー]{2,12}|[A-Za-z]{2,20})$/u;

const REPRESENTATIVE_ORGANIZATION_LIKE_REGEX =
  /(?:紙器|紙工|鋼材|電装|工業|工務|建設|住建|工機|工房|工藝|工芸|製材|製茶|製粉|製菓|製鋼|製作所|製作|機工|機器|器械|設備|電工|電設|電機|電子|通信|運輸|通運|産業|化学|化工|化成|鐵工|鉄工|織機|理化|光学|薬品|薬局|眼科|歯科|医院|病院|幼稚園|保育所|保育園|信用金庫|銀行|郵便局|研究所|研究機関|大学|短期大|学園|学校|高校|小学校|中学校|生協|協会|神宮|神社|茶屋|温泉|商店|家具|無線|木材|測量|登記|缶詰|道路|海運|建材|空調|鉄道|製本|看板|解体|葬祭|整体院|料理|酒房|生花|工作所|製麺|総業|乳業|産機)$/u;

const REPRESENTATIVE_NON_NAME_PREFIX_REGEX =
  /^(?:関係者各位|各位|御中|一同|不明|ふめい|未定|該当なし|お問い合わせ)$/u;

const REPRESENTATIVE_NON_NAME_SUFFIX_REGEX =
  /(?:会社|法人|組合|協会|事務局|センター|会館|病院|医院|クリニック|学校|学園|大学|高校|中学|小学校|幼稚園|保育園|施設|寮|館|ホール|ビル|タワー|本社|支社|支店|営業所|工場|研究所|製作所|製麺所|商店|店舗|ホテル|旅館|神社|寺院|農場|牧場|倉庫|公園|市場|駅|空港|港|団地|マンション|ハイツ|コーポ|号室|事務所|部署|部門|売場|園|店|会)$/u;

const REPRESENTATIVE_MUNICIPALITY_LIKE_REGEX =
  /^(?:[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]{3,}(?:市|区|町|村)|[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]{2,}市立[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]+)$/u;

const REPRESENTATIVE_NON_NAME_CONTENT_REGEX = new RegExp(
  [
    "規約",
    "概要",
    "案内",
    "情報",
    "紹介",
    "募集",
    "理念",
    "方針",
    "住宅",
    "新着",
    "連絡先",
    "仕事内容",
    "保証",
    "解析",
    "購入",
    "教室",
    "開発",
    "販売",
    "支援",
    "金融",
    "運行",
    "事業",
    "店舗",
    "製品",
    "商品",
    "商品名",
    "注文",
    "対象者",
    "取引先",
    "取引銀行",
    "問題",
    "品質",
    "利便性",
    "維持",
    "施工",
    "設計",
    "製作",
    "制作",
    "製缶",
    "板金",
    "鈑金",
    "塗装",
    "修理",
    "整備",
    "工事",
    "加工",
    "精密加工",
    "機械加工",
    "大型機械加工",
    "大型精密機械加工",
    "生産",
    "一貫生産体制",
    "流体",
    "計測",
    "測定",
    "測定機",
    "分析",
    "診断",
    "診断装置",
    "試作",
    "量産",
    "量産対応",
    "包装",
    "梱包",
    "発送",
    "印刷",
    "輪転機",
    "設備",
    "省力化設備",
    "機械",
    "電子機器",
    "電機",
    "電装",
    "電気設備工事",
    "電気工事",
    "電気制御",
    "自動制御",
    "材料",
    "素材",
    "部材",
    "衝撃吸収材",
    "制振遮音技術",
    "治具",
    "工具",
    "重量",
    "燃料電池",
    "防水",
    "断熱",
    "気密",
    "特殊鋼",
    "特殊印刷",
    "熱処理",
    "表面処理",
    "真空技術",
    "不動産",
    "建設",
    "建築",
    "建設工業",
    "工務店",
    "修繕",
    "大規模修繕工事",
    "分譲",
    "分譲地",
    "土地",
    "賃貸",
    "売却",
    "売買",
    "中古車",
    "新車",
    "輸入車",
    "車検",
    "車両",
    "車輌",
    "車専門物流",
    "運輸",
    "運送",
    "輸送",
    "配送",
    "物流",
    "産業用",
    "産業廃棄物",
    "保育",
    "介護",
    "看護",
    "障害福祉",
    "障害者共同生活",
    "福祉用具",
    "訪問介護",
    "訪問看護",
    "訪問入浴",
    "介護事業",
    "在宅診療",
    "専門入院自然療法",
    "診療所",
    "病院",
    "医院",
    "診療",
    "内科",
    "外科",
    "小児科",
    "循環器内",
    "呼吸器内",
    "放射線科",
    "腎臓内科",
    "神経科",
    "精神科",
    "皮膚科",
    "眼科",
    "耳鼻科",
    "鼻咽喉科",
    "整形外科",
    "接骨院",
    "歯科",
    "歯医者",
    "矯正歯科",
    "美容皮膚科",
    "調剤薬局",
    "薬局",
    "医薬品",
    "農業薬品",
    "化粧品",
    "司法書士",
    "行政書士",
    "税理士",
    "会計士",
    "認会計士",
    "理事長",
    "病院長",
    "最高経営責任者",
    "役員指名",
    "役員報酬",
    "取締役",
    "代表取締",
    "監査役",
    "役員",
    "理事",
    "副会長",
    "報酬",
    "報酬規程",
    "報酬支給基準",
    "名簿",
    "顧問名簿",
    "組織図",
    "組織概要",
    "組織機構",
    "職員",
    "全従業員",
    "全職員",
    "顧問",
    "氏名",
    "名前",
    "必須",
    "入力",
    "確認",
    "送信",
    "受信可能",
    "事項",
    "項目",
    "編集",
    "削除",
    "権限",
    "公式",
    "最新",
    "最近",
    "履歴",
    "流行",
    "歴史",
    "沿革",
    "活動",
    "活動内容",
    "取組",
    "体制",
    "変更",
    "業務改変",
    "配色変更",
    "余白設定追加",
    "色変換",
    "策定",
    "実行",
    "戦略",
    "計画",
    "創業",
    "創業以来",
    "設立",
    "年月日",
    "設立年月日",
    "法人設立年月日",
    "体験",
    "参加同意",
    "見学",
    "公演",
    "授業",
    "受験対策",
    "講座",
    "作品",
    "油彩",
    "日本画",
    "特産物",
    "地産地消",
    "洋菓子",
    "生菓子",
    "焼菓子",
    "和菓子",
    "味噌",
    "料理",
    "中華",
    "泡盛",
    "焼酎",
    "着物",
    "呉服",
    "弁当",
    "腕時計",
    "時計",
    "宝石",
    "花火",
    "園芸",
    "野菜収穫",
    "薬草",
    "焼肉",
    "研究会",
    "研究",
    "生涯学習",
    "国立長寿医療研究",
    "世界基準",
    "世界情勢",
    "世界最高水準",
    "持続可能",
    "創意工夫",
    "付加価値",
    "人材派遣",
    "人材育成",
    "進路",
    "就職支援",
    "新卒",
    "中途採用",
    "求人",
    "求人情報",
    "広告",
    "広告代",
    "映像",
    "動画",
    "動画付記事",
    "解説動画",
    "展示情報",
    "展示会",
    "展示車",
    "試乗車",
    "宿泊予約",
    "会員登録",
    "資料請求",
    "商品検索",
    "物件検索",
    "導入事例",
    "施工事例",
    "施工事例集",
    "施工実績",
    "施工実例",
    "制作事例",
    "納入事例",
    "対応事例",
    "参考事例",
    "紹介事例",
    "事例",
    "実績",
    "受賞履歴",
    "作業内容",
    "作業工程",
    "事前",
    "作業所",
    "事業所",
    "事業場",
    "店舗情報",
    "情報公開",
    "情報提供",
    "提案",
    "企画",
    "受注生産",
    "地図検索",
    "周辺情報",
    "利用案内",
    "利用方法",
    "利用時間",
    "利用料金",
    "最低価格",
    "午後最速",
    "無料貸出",
    "送料無料",
    "全画面表示",
    "表示拡大",
    "言語切替",
    "今準備中",
    "年末年始休業",
    "冬季休業",
    "定休日",
    "受付時間",
    "参加同意",
    "保存",
    "更新",
    "同意管理",
    "目的",
    "役割",
    "税務行政",
    "多国語展開",
    "中国料理",
    "中文",
    "简体",
    "繁體",
    "中文簡体",
    "中文简体字",
    "全球",
    "网络",
    "網絡",
    "集团",
    "举报",
    "站点",
    "浏览",
    "关于我们",
    "关于征途国际数码",
    "您的域名已过期",
    "参观公司张澜文献"
  ].join("|"),
  "u"
);

const REPRESENTATIVE_ADDRESS_LIKE_REGEX =
  /(?:[0-9０-９]|丁目|番地|番|号|都道府県|市.+区|県.+市|市.+町|市.+村|区.+町|区.+村)/u;

const REPRESENTATIVE_PREFIX_TITLE_TRIM_REGEX =
  /^(?:代表取締役会長CEO|代表取締役社長COO|代表取締役副社長|代表取締役専務|代表取締役常務|代表取締役|取締役会長|取締役社長|取締役副社長|取締役専務|取締役常務|取締役|代表|会長|社長|副社長|専務|常務|執行役員|監査役|理事長|院長|所長|支店長|本部長|部長|課長|店長|工場長|センター長|室長|主任|係長|担当役員|担当者|担当|責任者|マネージャー)+/u;

const REPRESENTATIVE_SUFFIX_TITLE_TRIM_REGEX =
  /(?:代表取締役会長CEO|代表取締役社長COO|代表取締役副社長|代表取締役専務|代表取締役常務|代表取締役|取締役会長|取締役社長|取締役副社長|取締役専務|取締役常務|取締役|代表|会長|社長|副社長|専務|常務|執行役員|監査役|理事長|院長|所長|支店長|本部長|部長|課長|店長|工場長|センター長|室長|主任|係長|担当役員|担当者|担当|責任者|マネージャー|様|さん|氏)+$/u;

const REPRESENTATIVE_NOISE_TOKEN_REGEX =
  /^(?:男性|女性|男|女|担当|担当者|責任者|窓口|受付|御中|様|さん|氏|代表|社長|会長)$/u;

function normalizeRepresentativeComparisonValue(value: string) {
  return value
    .normalize("NFKC")
    .replace(/\s+/g, " ")
    .trim();
}
  
function trimRepresentativeAffixes(value: string) {
  let text = value.normalize("NFKC").trim();
  let previous = "";

  while (text !== previous) {
    previous = text;
    text = text
      .replace(REPRESENTATIVE_PREFIX_TITLE_TRIM_REGEX, "")
      .replace(REPRESENTATIVE_SUFFIX_TITLE_TRIM_REGEX, "")
      .trim();
  }

  return text;
}

function normalizeRepresentativeSource(value: string) {
  return value
    .normalize("NFKC")
    .replace(/[（(][^）)]*[）)]/g, " ")
    .replace(/[【】\[\]「」『』<>〈〉《》〔〕]/g, " ")
    .replace(/[\/／|｜,，、・｡。!！?？:：;；"'`´]/g, " ")
    .replace(/[^\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}A-Za-z\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeRepresentativeToken(token: string) {
  const normalized = token
    .normalize("NFKC")
    .replace(
      /^[A-Za-z]+(?=[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}])/gu,
      " "
    )
    .replace(/[【】\[\]「」『』<>〈〉《》〔〕]/g, " ")
    .replace(/[\/／|｜,，、・｡。!！?？:：;；"'`´]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return trimRepresentativeAffixes(
    normalized
      .replace(
        /^[^A-Za-z\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]+/gu,
        ""
      )
      .replace(
        /[^A-Za-z\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]+$/gu,
        ""
      )
      .trim()
  );
}

function containsRepresentativeStopword(value: string) {
  const lower = value.toLowerCase();

  return REPRESENTATIVE_STOPWORDS.some((word) => {
    const target = word.toLowerCase();
    return (
      lower === target ||
      lower.startsWith(target) ||
      lower.endsWith(target)
    );
  });
}

function isRepresentativeAreaToken(value: string) {
  return REPRESENTATIVE_AREA_NAME_TOKENS.has(value.normalize("NFKC"));
}

function looksLikeProtectedRepresentativeName(value: string) {
  const text = trimRepresentativeAffixes(value.trim());

  if (!text) return false;
  if (/[0-9０-９]/.test(text)) return false;
  if (/株式会社|有限会社|合同会社|御中|様/.test(text)) return false;
  if (REPRESENTATIVE_ADDRESS_LIKE_REGEX.test(text)) return false;

  const parts = text.split(/\s+/).filter((part) => part !== "");

  if (parts.length === 0 || parts.length > 2) return false;

  return parts.every((part) => {
    const normalized = part.normalize("NFKC");

    if (!REPRESENTATIVE_STRONG_NAME_TOKEN_REGEX.test(part)) return false;
    if (REPRESENTATIVE_STRICT_NON_NAME_AREA_TOKENS.has(normalized)) return false;
    if (REPRESENTATIVE_MUNICIPALITY_LIKE_REGEX.test(part)) return false;
    if (REPRESENTATIVE_ORGANIZATION_LIKE_REGEX.test(part)) return false;
    if (REPRESENTATIVE_NON_NAME_CONTENT_REGEX.test(part)) return false;

    return true;
  });
}

function looksLikeNonNameToken(value: string) {
  const text = trimRepresentativeAffixes(value.trim());
  if (!text) return true;

  if (looksLikeProtectedRepresentativeName(text)) return false;
  if (REPRESENTATIVE_NON_NAME_EXACT_VALUES.has(text)) return true;
  if (REPRESENTATIVE_STRICT_NON_NAME_AREA_TOKENS.has(text.normalize("NFKC"))) {
    return true;
  }
  if (REPRESENTATIVE_NON_NAME_PREFIX_REGEX.test(text)) return true;
  if (REPRESENTATIVE_NON_NAME_SUFFIX_REGEX.test(text)) return true;
  if (REPRESENTATIVE_MUNICIPALITY_LIKE_REGEX.test(text)) return true;
  if (REPRESENTATIVE_ORGANIZATION_LIKE_REGEX.test(text)) return true;
  if (REPRESENTATIVE_NON_NAME_CONTENT_REGEX.test(text)) return true;
  if (REPRESENTATIVE_ADDRESS_LIKE_REGEX.test(text)) return true;
  if (/株式会社|有限会社|合同会社|御中|様/.test(text)) return true;
  if (containsRepresentativeStopword(text)) return true;

  return false;
}

function looksLikeRepresentativeNameToken(value: string) {
  const text = trimRepresentativeAffixes(value.trim());

  if (!text) return false;
  if (/[0-9０-９]/.test(text)) return false;
  if (looksLikeNonNameToken(text)) return false;

  if (/^[\p{sc=Han}々ヶヵ\p{sc=Hiragana}\p{sc=Katakana}ー]{1,10}$/u.test(text)) {
    return true;
  }

  if (/^[A-Za-z]{2,20}$/u.test(text)) {
    return true;
  }

  return false;
}

function looksLikeRepresentativeName(value: string) {
  const text = trimRepresentativeAffixes(value.trim());

  if (!text) return false;
  if (looksLikeProtectedRepresentativeName(text)) return true;
  if (/[0-9０-９]/.test(text)) return false;
  if (/株式会社|有限会社|合同会社|御中|様/.test(text)) return false;
  if (REPRESENTATIVE_ORGANIZATION_LIKE_REGEX.test(text)) return false;
  if (REPRESENTATIVE_NON_NAME_CONTENT_REGEX.test(text)) return false;
  if (REPRESENTATIVE_ADDRESS_LIKE_REGEX.test(text)) return false;

  const parts = text.split(/\s+/).filter((part) => part !== "");

  if (parts.length === 0 || parts.length > 2) return false;
  if (parts.some((part) => looksLikeNonNameToken(part))) return false;

  if (
    parts.length === 1 &&
    REPRESENTATIVE_STRICT_NON_NAME_AREA_TOKENS.has(parts[0].normalize("NFKC"))
  ) {
    return false;
  }

  if (
    parts.length === 2 &&
    parts.every(
      (part) =>
        isRepresentativeAreaToken(part) ||
        REPRESENTATIVE_STRICT_NON_NAME_AREA_TOKENS.has(part.normalize("NFKC"))
    )
  ) {
    return false;
  }

  return parts.every((part) => looksLikeRepresentativeNameToken(part));
}

function getRepresentativeTokenVariants(token: string) {
  const normalized = normalizeRepresentativeToken(token);
  const variants = new Set<string>();

  if (!normalized) return [];

  variants.add(normalized);

  const leadingJapanese =
    normalized.match(/^[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}ー]{1,12}/u)?.[0];
  if (leadingJapanese) {
    variants.add(leadingJapanese);
  }

  const trailingJapanese =
    normalized.match(/[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}ー]{1,12}$/u)?.[0];
  if (trailingJapanese) {
    variants.add(trailingJapanese);
  }

  const japaneseChunks = Array.from(
    normalized.matchAll(/[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}ー]{1,12}/gu)
  ).map((match) => match[0]);

  japaneseChunks.forEach((chunk) => variants.add(chunk));

  const leadingAscii = normalized.match(/^[A-Za-z]{2,20}/u)?.[0];
  if (leadingAscii) {
    variants.add(leadingAscii);
  }

  const trailingAscii = normalized.match(/[A-Za-z]{2,20}$/u)?.[0];
  if (trailingAscii) {
    variants.add(trailingAscii);
  }

  return Array.from(variants).filter(
    (variant) =>
      variant !== "" &&
      !REPRESENTATIVE_NOISE_TOKEN_REGEX.test(variant) &&
      !looksLikeNonNameToken(variant)
  );
}

function extractRepresentativeName(value: string) {
  const normalized = normalizeRepresentativeSource(value);
  if (!normalized) return null;

  const withoutTitles = trimRepresentativeAffixes(
    normalized
      .replace(REPRESENTATIVE_TITLE_REGEX, " ")
      .replace(/\s+/g, " ")
      .trim()
  );

  if (!withoutTitles) return null;

  const tokens = withoutTitles
    .split(" ")
    .map((token) => normalizeRepresentativeToken(token))
    .filter(
      (token) => token !== "" && !REPRESENTATIVE_NOISE_TOKEN_REGEX.test(token)
    );

  for (let i = tokens.length - 2; i >= 0; i--) {
    const firstVariants = getRepresentativeTokenVariants(tokens[i]);
    const secondVariants = getRepresentativeTokenVariants(tokens[i + 1]);

    for (const first of firstVariants) {
      for (const second of secondVariants) {
        const candidate = `${first} ${second}`.replace(/\s+/g, " ").trim();

        if (looksLikeRepresentativeName(candidate)) {
          return candidate;
        }
      }
    }
  }

  for (let i = tokens.length - 1; i >= 0; i--) {
    const variants = getRepresentativeTokenVariants(tokens[i]);

    for (const variant of variants) {
      if (looksLikeRepresentativeName(variant)) {
        return variant;
      }
    }
  }

  const compactMatches = Array.from(
    withoutTitles.matchAll(
      /[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}ーA-Za-z]{1,20}/gu
    )
  ).map((match) => normalizeRepresentativeToken(match[0]));

  for (let i = compactMatches.length - 1; i >= 0; i--) {
    if (looksLikeRepresentativeName(compactMatches[i])) {
      return compactMatches[i];
    }
  }

  return null;
}

function inspectRepresentativeNameValue(value: string | null) {
  const original = typeof value === "string" ? value.trim() : "";

  if (original === "") {
    return {
      cleanedValue: null as string | null,
      shouldUpdate: false,
      shouldDelete: false,
      shouldReview: false,
      reason: "",
    };
  }

  const extractedName = extractRepresentativeName(original);

  if (extractedName) {
    const normalizedOriginal = normalizeRepresentativeComparisonValue(original);
    const normalizedExtracted =
      normalizeRepresentativeComparisonValue(extractedName);

    if (normalizedOriginal !== normalizedExtracted) {
      return {
        cleanedValue: extractedName,
        shouldUpdate: true,
        shouldDelete: false,
        shouldReview: false,
        reason: "氏名を抽出できたため更新候補",
      };
    }

    return {
      cleanedValue: extractedName,
      shouldUpdate: false,
      shouldDelete: false,
      shouldReview: false,
      reason: "",
    };
  }

  const normalizedSource = trimRepresentativeAffixes(
    normalizeRepresentativeSource(original)
  );

  if (!normalizedSource || looksLikeNonNameToken(normalizedSource)) {
    return {
      cleanedValue: null as string | null,
      shouldUpdate: false,
      shouldDelete: true,
      shouldReview: false,
      reason: "氏名ではない可能性が高いため削除候補",
    };
  }

  return {
    cleanedValue: null as string | null,
    shouldUpdate: false,
    shouldDelete: false,
    shouldReview: true,
    reason: "氏名か断定できないため要確認",
  };
}

function getJobSnapshot(job: ItemInspectionJob) {
  const processingCount =
    job.status === "running" && job.currentCompany ? 1 : 0;

  const updateCount = job.inspectionPreviewChanges.filter(
    (row) => row.action === "update"
  ).length;

  const deleteCount = job.inspectionPreviewChanges.filter(
    (row) => row.action === "delete"
  ).length;

  const reviewCount = job.inspectionPreviewChanges.filter(
    (row) => row.action === "review"
  ).length;

  return {
    ok: true,
    jobId: job.id,
    jobStatus: job.status,
    totalTargets: job.totalTargets,
    processed: job.processed,
    updated: job.updated,
    skipped: job.skipped,
    failed: job.failed,
    currentCompany: job.currentCompany,
    currentInspectionValue: job.currentInspectionValue,
    currentInspectionFieldLabel: job.currentInspectionFieldLabel,
    remainingCount: Math.max(
      job.totalTargets - job.processed - processingCount,
      0
    ),
    inspectionPreviewChanges: job.inspectionPreviewChanges,
    error: job.error ?? undefined,
    paused: job.status === "paused",
    completed: job.status === "completed",
    message:
      job.status === "completed"
        ? updateCount === 0 && deleteCount === 0 && reviewCount === 0
          ? "候補はありませんでした"
          : `更新候補 ${updateCount.toLocaleString()}件 / 削除候補 ${deleteCount.toLocaleString()}件 / 要確認 ${reviewCount.toLocaleString()}件`
        : job.status === "paused"
        ? "項目精査を中断しました"
        : undefined,
  };
}

async function fetchInspectionTargets(
  payload: Record<string, unknown>
): Promise<InspectionTargetRow[]> {
  await dbReady;
  const client = await pool.connect();

  try {
    await ensureMasterDataIdColumn(client);

    const inspectionScope =
      payload.inspectionScope === "all" ? "all" : "filtered";

    const searchParams = buildSearchParamsFromReadPayload(payload);
    const { whereSql, params } =
      inspectionScope === "filtered"
        ? buildWhereClause(searchParams)
        : { whereSql: "", params: [] as (string | number)[] };

    const targetWhereSql =
      whereSql !== ""
        ? `${whereSql} AND ${buildNotBlankCheckClause(`"代表者名"`)}`
        : `WHERE ${buildNotBlankCheckClause(`"代表者名"`)} `;

    const res = await client.query(
      `
        SELECT
          id,
          "企業名" AS company,
          "代表者名" AS representative_name
        FROM public.master_data
        ${targetWhereSql}
        ORDER BY id ASC
      `,
      params
    );

    return res.rows.map((row) => ({
      rowId: String(row.id ?? ""),
      company: typeof row.company === "string" ? row.company : null,
      representativeName:
        typeof row.representative_name === "string"
          ? row.representative_name
          : null,
    }));
  } finally {
    client.release();
  }
}

async function processItemInspectionJob(jobId: string) {
  while (true) {
    const job = itemInspectionJobs.get(jobId);
    if (!job) return;

    if (job.cancelRequested) {
      itemInspectionJobs.delete(jobId);
      return;
    }

    if (job.pauseRequested) {
      job.status = "paused";
      job.currentCompany = null;
      job.currentInspectionValue = null;
      job.currentInspectionFieldLabel = "代表者名";
      job.updatedAt = Date.now();
      scheduleJobCleanup(job.id);
      return;
    }

    if (job.currentIndex >= job.targetRows.length) {
      job.status = "completed";
      job.currentCompany = null;
      job.currentInspectionValue = null;
      job.currentInspectionFieldLabel = "代表者名";
      job.updatedAt = Date.now();
      scheduleJobCleanup(job.id);
      return;
    }

    const row = job.targetRows[job.currentIndex];

    job.currentCompany = row.company;
    job.currentInspectionValue = row.representativeName;
    job.currentInspectionFieldLabel = "代表者名";
    job.updatedAt = Date.now();

    try {
      const result = inspectRepresentativeNameValue(row.representativeName);

      if (result.shouldDelete) {
        job.inspectionPreviewChanges.push({
          rowId: row.rowId,
          company: row.company,
          fieldLabel: "代表者名",
          beforeValue: row.representativeName,
          afterValue: null,
          action: "delete",
          reason: result.reason,
        });
      } else if (result.shouldUpdate) {
        job.inspectionPreviewChanges.push({
          rowId: row.rowId,
          company: row.company,
          fieldLabel: "代表者名",
          beforeValue: row.representativeName,
          afterValue: result.cleanedValue,
          action: "update",
          reason: result.reason,
        });
      } else if (result.shouldReview) {
        job.inspectionPreviewChanges.push({
          rowId: row.rowId,
          company: row.company,
          fieldLabel: "代表者名",
          beforeValue: row.representativeName,
          afterValue: null,
          action: "review",
          reason: result.reason,
        });
      } else {
        job.skipped += 1;
      }
    } catch (error) {
      job.failed += 1;
      job.error =
        error instanceof Error
          ? error.message
          : "項目精査中にエラーが発生しました";
    } finally {
      job.processed += 1;
      job.currentIndex += 1;
      job.updatedAt = Date.now();
    }

    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

async function handleStartJob(payload: Record<string, unknown>) {
  const selectedFields = Array.isArray(payload.selectedFields)
    ? payload.selectedFields.map((value) => String(value ?? ""))
    : [];

  const methodSelections =
    payload.methodSelections && typeof payload.methodSelections === "object"
      ? (payload.methodSelections as Record<string, unknown>)
      : {};

  if (
    !(
      selectedFields.length === 1 &&
      selectedFields[0] === "representative_name"
    )
  ) {
    return NextResponse.json(
      { ok: false, error: "現在は代表者名のみ項目精査に対応しています" },
      { status: 400 }
    );
  }

  if (methodSelections.representative_name_remove_non_name !== true) {
    return NextResponse.json(
      { ok: false, error: "精査方法が選択されていません" },
      { status: 400 }
    );
  }

  const targetRows = await fetchInspectionTargets(payload);

  if (targetRows.length === 0) {
    return NextResponse.json({
      ok: true,
      message: "対象がありませんでした",
      totalTargets: 0,
      processed: 0,
      updated: 0,
      skipped: 0,
      failed: 0,
      currentCompany: null,
      currentInspectionValue: null,
      currentInspectionFieldLabel: "代表者名",
      remainingCount: 0,
      inspectionPreviewChanges: [],
    });
  }

  const jobId = randomUUID();
  const now = Date.now();

  const job: ItemInspectionJob = {
    id: jobId,
    status: "running",
    createdAt: now,
    updatedAt: now,
    targetRows,
    currentIndex: 0,
    totalTargets: targetRows.length,
    processed: 0,
    updated: 0,
    skipped: 0,
    failed: 0,
    currentCompany: null,
    currentInspectionValue: null,
    currentInspectionFieldLabel: "代表者名",
    inspectionPreviewChanges: [],
    error: null,
    pauseRequested: false,
    cancelRequested: false,
  };

  itemInspectionJobs.set(jobId, job);

  queueMicrotask(() => {
    void processItemInspectionJob(jobId);
  });

  return NextResponse.json(getJobSnapshot(job));
}

async function handleGetJobStatus(payload: Record<string, unknown>) {
  const jobId = String(payload.jobId ?? "").trim();

  if (!jobId) {
    return NextResponse.json(
      { ok: false, error: "jobIdが指定されていません" },
      { status: 400 }
    );
  }

  const job = itemInspectionJobs.get(jobId);

  if (!job) {
    return NextResponse.json(
      { ok: false, error: "項目精査ジョブが見つかりません" },
      { status: 404 }
    );
  }

  return NextResponse.json(getJobSnapshot(job));
}

async function handlePauseJob(payload: Record<string, unknown>) {
  const jobId = String(payload.jobId ?? "").trim();

  if (!jobId) {
    return NextResponse.json(
      { ok: false, error: "jobIdが指定されていません" },
      { status: 400 }
    );
  }

  const job = itemInspectionJobs.get(jobId);

  if (!job) {
    return NextResponse.json(
      { ok: false, error: "項目精査ジョブが見つかりません" },
      { status: 404 }
    );
  }

  if (job.status === "completed") {
    return NextResponse.json(getJobSnapshot(job));
  }

  if (job.status === "paused") {
    return NextResponse.json(getJobSnapshot(job));
  }

  job.pauseRequested = true;
  job.updatedAt = Date.now();

  return NextResponse.json({
    ...getJobSnapshot(job),
    message: "中断指示を受け付けました",
  });
}

async function handleCancelJob(payload: Record<string, unknown>) {
  const jobId = String(payload.jobId ?? "").trim();

  if (!jobId) {
    return NextResponse.json(
      { ok: false, error: "jobIdが指定されていません" },
      { status: 400 }
    );
  }

  const job = itemInspectionJobs.get(jobId);

  if (!job) {
    return NextResponse.json({
      ok: true,
      message: "すでに中止されています",
    });
  }

  job.cancelRequested = true;
  job.updatedAt = Date.now();

  return NextResponse.json({
    ok: true,
    message: "項目精査を中止しました",
  });
}

async function handleApplyPreviewChanges(payload: Record<string, unknown>) {
  const changes = Array.isArray(payload.changes)
    ? payload.changes
        .filter(
          (
            value
          ): value is {
            rowId: string;
            action: "update" | "delete";
            afterValue: string | null;
          } =>
            typeof value === "object" &&
            value !== null &&
            typeof (value as { rowId?: unknown }).rowId === "string" &&
            ((value as { action?: unknown }).action === "update" ||
              (value as { action?: unknown }).action === "delete")
        )
        .map((value) => ({
          rowId: String(value.rowId).trim(),
          action: value.action,
          afterValue:
            value.afterValue == null ? null : String(value.afterValue),
        }))
        .filter((value) => value.rowId !== "")
    : [];

  if (changes.length === 0) {
    return NextResponse.json(
      { ok: false, error: "反映する項目が選択されていません" },
      { status: 400 }
    );
  }

  const client = await pool.connect();

  try {
    await dbReady;
    await ensureMasterDataIdColumn(client);
    await client.query("BEGIN");

    let applied = 0;
    let updatedCount = 0;
    let deletedCount = 0;

    for (const change of changes) {
      if (change.action === "update") {
        const nextValue = (change.afterValue ?? "").trim();

        if (nextValue === "") {
          continue;
        }

        await client.query(
          `
            UPDATE public.master_data
            SET "代表者名" = $1
            WHERE id = $2::bigint
          `,
          [nextValue, change.rowId]
        );

        applied += 1;
        updatedCount += 1;
        continue;
      }

      await client.query(
        `
          UPDATE public.master_data
          SET "代表者名" = NULL
          WHERE id = $1::bigint
        `,
        [change.rowId]
      );

      applied += 1;
      deletedCount += 1;
    }

    await client.query("COMMIT");

    return NextResponse.json({
      ok: true,
      applied,
      updated: updatedCount,
      deleted: deletedCount,
      message: `${applied.toLocaleString()}件を反映しました（更新 ${updatedCount.toLocaleString()}件 / 削除 ${deletedCount.toLocaleString()}件）`,
    });
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {}

    return NextResponse.json(
      {
        ok: false,
        error:
          error instanceof Error
            ? error.message
            : "精査結果の反映でエラーが発生しました",
      },
      { status: 500 }
    );
  } finally {
    client.release();
  }
}

export async function POST(req: NextRequest) {
  try {
    await dbReady;
    const payload = (await req.json()) as Record<string, unknown>;
    const action = String(payload.action ?? "");

    if (action === "start_job") {
      return handleStartJob(payload);
    }

    if (action === "get_job_status") {
      return handleGetJobStatus(payload);
    }

    if (action === "pause_job") {
      return handlePauseJob(payload);
    }

    if (action === "cancel_job") {
      return handleCancelJob(payload);
    }

    if (action === "apply_preview_changes") {
      return handleApplyPreviewChanges(payload);
    }

    return NextResponse.json(
      { ok: false, error: "不正なactionです" },
      { status: 400 }
    );
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error:
          error instanceof Error
            ? error.message
            : "項目精査APIでエラーが発生しました",
      },
      { status: 500 }
    );
  }
}