import { NextRequest, NextResponse } from "next/server";
import { pool } from "@/lib/db";

const FILTER_COLUMN_MAP = {
  company: `"企業名"`,
  zipcode: `"郵便番号"`,
  address: `"住所"`,
  big_industry: `"大業種名"`,
  small_industry: `"小業種名"`,
  company_kana: `"企業名（かな）"`,
  summary: `"企業概要"`,
  business_content: `"事業内容"`,
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
  new_tag: `"新規登録タグ"`,
  delete_tag: `"削除タグ"`,
  delete_flag: `"削除フラグ"`,
  force_flag: `"強制フラグ"`,
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
}

type AdvancedFilters = {
  companyName?: AdvancedCompanyNameFilter;
  prefectures?: AdvancedPrefectureFilters;
  industries?: AdvancedIndustryFilters;
  established?: AdvancedEstablishedFilters;
  capital?: AdvancedRangeFilters;
  employeeCount?: AdvancedRangeFilters;
  tags?: AdvancedTagFilters;
};

const CSV_COLUMNS = [
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
] as const;

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
  const firstNumber = `NULLIF(substring(${noComma} from '([0-9]+)'), '')`;

  return `CASE
    WHEN NULLIF(BTRIM(${textColumn}), '') IS NULL THEN NULL
    WHEN ${firstNumber} IS NOT NULL
      THEN ${firstNumber}::numeric
    ELSE NULL
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
  if(companyKeyword !== "") {
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

function addConditionClause(
  where: string[],
  params: (string | number)[],
  column: string,
  model?: FilterModel
) {
  if (!model) return;

  const type = model.conditionType || "";
  const value = (model.conditionValue || "").trim();

  if (type === "") return;

  if (type === "is_empty") {
    where.push(`NULLIF(BTRIM(COALESCE(${column}, '')), '') IS NULL`);
    return;
  }

  if (type === "is_not_empty") {
    where.push(`NULLIF(BTRIM(COALESCE(${column}, '')), '') IS NOT NULL`);
    return;
  }

  if (value === "") return;

  if (type === "contains") {
    params.push(`%${value}%`);
    where.push(`${column} ILIKE $${params.length}`);
    return;
  }

  if (type === "not_contains") {
    params.push(`%${value}%`);
    where.push(`(${column} IS NULL OR ${column} NOT ILIKE $${params.length})`);
    return;
  }

  if (type === "equals") {
    params.push(value);
    where.push(`COALESCE(${column}, '') = $${params.length}`);
    return;
  }

  if (type === "not_equals") {
    params.push(value);
    where.push(`COALESCE(${column}, '') <> $${params.length}`);
    return;
  }

  if (type === "starts_with") {
    params.push(`${value}%`);
    where.push(`${column} ILIKE $${params.length}`);
    return;
  }

  if (type === "ends_with") {
    params.push(`%${value}`);
    where.push(`${column} ILIKE $${params.length}`);
  }
}

function addValueFilterClause(
  where: string[],
  params: (string | number)[],
  column: string,
  model?: FilterModel
) {
  if (!model?.valueFilterEnabled) return;

  const selectedValues = Array.isArray(model.selectedValues)
    ? model.selectedValues
    : [];

  if (selectedValues.length === 0) {
    where.push("1 = 0");
    return;
  }

  const normalValues = selectedValues.filter((value) => value !== "");
  const includeEmpty = selectedValues.includes("");

  const pieces: string[] = [];

  if (normalValues.length > 0) {
    const placeholders = normalValues.map((value) => {
      params.push(value);
      return `$${params.length}`;
    });
    pieces.push(`COALESCE(${column}, '') IN (${placeholders.join(", ")})`);
  }

  if (includeEmpty) {
    pieces.push(`NULLIF(BTRIM(COALESCE(${column}, '')), '') IS NULL`);
  }

  if (pieces.length > 0) {
    where.push(`(${pieces.join(" OR ")})`);
  }
}

function buildWhereClause(searchParams: URLSearchParams) {
  const filterModels = parseFilterModels(searchParams);
  const advancedFilters = parseAdvancedFilters(searchParams);
  const where: string[] = [];
  const params: (string | number)[] = [];

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

function buildOrderBy(searchParams: URLSearchParams) {
  const sortKey = searchParams.get("sortKey") as FilterKey | null;
  const sortDirection = searchParams.get("sortDirection") as
    | SortDirection
    | null;

  if (
    sortKey &&
    sortDirection &&
    FILTER_COLUMN_MAP[sortKey] &&
    (sortDirection === "asc" || sortDirection === "desc")
  ) {
    return `ORDER BY COALESCE(${FILTER_COLUMN_MAP[sortKey]}, '') ${sortDirection.toUpperCase()}, COALESCE("企業名", ''), COALESCE("住所", '')`;
  }

  return `ORDER BY COALESCE("企業名", ''), COALESCE("住所", '')`;
}

function escapeCsvValue(value: unknown) {
  const text = value == null ? "" : String(value);
  return `"${text.replace(/"/g, `""`)}"`;
}

function createFileName() {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const dd = String(now.getDate()).padStart(2, "0");
  const hh = String(now.getHours()).padStart(2, "0");
  const mi = String(now.getMinutes()).padStart(2, "0");
  const ss = String(now.getSeconds()).padStart(2, "0");
  return `master_data_${yyyy}${mm}${dd}_${hh}${mi}${ss}.csv`;
}

export async function GET(req: NextRequest) {
  try {
    const { searchParams } = new URL(req.url);
    const { whereSql, params } = buildWhereClause(searchParams);

    const sql = `
      SELECT
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
        "強制フラグ"
      FROM public.master_data
      ${whereSql}
      ${buildOrderBy(searchParams)}
    `;

    const result = await pool.query(sql, params);

    const lines = [
      CSV_COLUMNS.map((col) => escapeCsvValue(col)).join(","),
      ...result.rows.map((row) =>
        CSV_COLUMNS.map((col) => escapeCsvValue(row[col])).join(",")
      ),
    ];

    const csv = "\uFEFF" + lines.join("\r\n");
    const fileName = createFileName();

    return new NextResponse(csv, {
      status: 200,
      headers: {
        "Content-Type": "text/csv; charset=utf-8",
        "Content-Disposition": `attachment; filename="${fileName}"; filename*=UTF-8''${encodeURIComponent(fileName)}`,
        "Cache-Control": "no-store",
      },
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "CSVエクスポートでエラーが発生しました",
      },
      { status: 500 }
    );
  }
}