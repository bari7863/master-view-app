import { NextRequest, NextResponse } from "next/server";
export const dynamic = "force-dynamic";
export const fetchCache = "force-no-store";
export const runtime = "nodejs";
import { dbReady, pool } from "@/lib/db";

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
  new_tag: `"新規登録タグ"`,
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

const CSV_HEADER_COLUMNS = [
  "企業名",
  "郵便番号",
  "住所",
  "大業種",
  "小業種",
  "企業名（かな）",
  "企業概要",
  "企業URL",
  "お問い合わせフォームURL",
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
  "タグ",
  "業種",
  "事業内容",
  "業界",
  "メモ",
] as const;

const DB_INSERT_COLUMNS = [
  "企業名",
  "郵便番号",
  "住所",
  "大業種名",
  "小業種名",
  "企業名（かな）",
  "企業概要",
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
  "業種",
  "事業内容",
  "業界",
  "メモ",
] as const;

const DB_INSERT_TO_CSV_HEADER: Record<
  (typeof DB_INSERT_COLUMNS)[number],
  (typeof CSV_HEADER_COLUMNS)[number]
> = {
  "企業名": "企業名",
  "郵便番号": "郵便番号",
  "住所": "住所",
  "大業種名": "大業種",
  "小業種名": "小業種",
  "企業名（かな）": "企業名（かな）",
  "企業概要": "企業概要",
  "企業サイトURL": "企業URL",
  "問い合わせフォームURL": "お問い合わせフォームURL",
  "電話番号": "電話番号",
  "FAX番号": "FAX番号",
  "メールアドレス": "メールアドレス",
  "設立年月": "設立年月",
  "代表者名": "代表者名",
  "代表者役職": "代表者役職",
  "資本金": "資本金",
  "従業員数": "従業員数",
  "従業員数年度": "従業員数年度",
  "前年売上高": "前年売上高",
  "直近売上高": "直近売上高",
  "決算月": "決算月",
  "事業所数": "事業所数",
  "新規登録タグ": "タグ",
  "業種": "業種",
  "事業内容": "事業内容",
  "業界": "業界",
  "メモ": "メモ",
};

function hasExactCsvHeaders(headerRow: string[]) {
  return (
    headerRow.length === CSV_HEADER_COLUMNS.length &&
    CSV_HEADER_COLUMNS.every((column, index) => headerRow[index] === column)
  );
}

function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const char = text[i];

    if (char === `"`) {
      if (inQuotes && text[i + 1] === `"`) {
        cell += `"`;
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (!inQuotes && char === ",") {
      row.push(cell);
      cell = "";
      continue;
    }

    if (!inQuotes && (char === "\n" || char === "\r")) {
      if (char === "\r" && text[i + 1] === "\n") {
        i++;
      }
      row.push(cell);
      if (row.some((value) => value !== "")) {
        rows.push(row);
      }
      row = [];
      cell = "";
      continue;
    }

    cell += char;
  }

  row.push(cell);
  if (row.some((value) => value !== "")) {
    rows.push(row);
  }

  return rows;
}

function normalizeHeader(value: string) {
  return value.replace(/^\uFEFF/, "").trim();
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

function isRepresentativeNameEmptyFilter(searchParams: URLSearchParams) {
  const filterModels = parseFilterModels(searchParams);
  const model = filterModels.representative_name;

  if (!model) return false;

  if (model.conditionType === "is_empty") {
    return true;
  }

  if (
    model.valueFilterEnabled &&
    Array.isArray(model.selectedValues) &&
    model.selectedValues.length === 1 &&
    model.selectedValues[0] === ""
  ) {
    return true;
  }

  return false;
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
  if (/運輸|郵便|物流|倉庫|運送/.test(value)) return "運輸業、郵便業";
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

  const textColumn = `COALESCE(${column}::text, '')`;
  const normalValues = selectedValues.filter((value) => value !== "");
  const includeEmpty = selectedValues.includes("");

  const pieces: string[] = [];

  if (normalValues.length > 0) {
    const placeholders = normalValues.map((value) => {
      params.push(value);
      return `$${params.length}`;
    });
    pieces.push(`${textColumn} IN (${placeholders.join(", ")})`);
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
    const sortColumnText = `COALESCE(${FILTER_COLUMN_MAP[sortKey]}::text, '')`;
    return `ORDER BY ${sortColumnText} ${sortDirection.toUpperCase()}, COALESCE("企業名"::text, ''), COALESCE("住所"::text, '')`;
  }

  return `ORDER BY COALESCE("企業名"::text, ''), COALESCE("住所"::text, '')`;
}

export async function GET(req: NextRequest) {
  try {
    await dbReady;
    const { searchParams } = new URL(req.url);
    const valuesFor = searchParams.get("valuesFor") as FilterKey | null;
    const advancedValuesFor = searchParams.get("advancedValuesFor");

    if (advancedValuesFor === "prefecture") {
      const { whereSql, params } = buildWhereClause(searchParams);

      const sql = `
        SELECT
          prefecture,
          city,
          COUNT(*)::int AS count
        FROM (
          SELECT
            ${PREFECTURE_EXPR} AS prefecture,
            ${CITY_EXPR} AS city
          FROM public.master_data
          ${whereSql}
        ) src
        WHERE prefecture IS NOT NULL
        GROUP BY prefecture, city
        ORDER BY prefecture ASC, city ASC
      `;

      const res = await pool.query(sql, params);
      const cityMap = new Map<string, Set<string>>();
      const prefectureCountMap = new Map<string, number>();
      const cityCountMap = new Map<string, Record<string, number>>();

      res.rows.forEach((row) => {
        const prefecture =
          typeof row.prefecture === "string" ? row.prefecture.trim() : "";
        const city = typeof row.city === "string" ? row.city.trim() : "";
        const count = Number(row.count ?? 0);

        if (!prefecture) return;

        prefectureCountMap.set(
          prefecture,
          (prefectureCountMap.get(prefecture) ?? 0) + count
        );

        if (!cityMap.has(prefecture)) {
          cityMap.set(prefecture, new Set<string>());
        }

        if (!cityCountMap.has(prefecture)) {
          cityCountMap.set(prefecture, {});
        }

        if (city) {
          cityMap.get(prefecture)?.add(city);
          cityCountMap.get(prefecture)![city] =
            (cityCountMap.get(prefecture)?.[city] ?? 0) + count;
        }
      });

      const items = PREFECTURE_NAMES.filter((prefecture) =>
        prefectureCountMap.has(prefecture)
      ).map((prefecture) => ({
        region: PREFECTURE_TO_REGION[prefecture],
        prefecture,
        prefectureCount: prefectureCountMap.get(prefecture) ?? 0,
        cities: Array.from(cityMap.get(prefecture) ?? []).sort(),
        cityCounts: cityCountMap.get(prefecture) ?? {},
      }));

      return NextResponse.json({
        ok: true,
        regions: Array.from(new Set(items.map((item) => item.region))),
        prefectures: items.map((item) => item.prefecture),
        items,
      });
    }

    if (advancedValuesFor === "industry") {
      const { whereSql, params } = buildWhereClause(searchParams);

      const sql = `
        SELECT
          ${BIG_INDUSTRY_TEXT} AS big_industry,
          ${SMALL_INDUSTRY_TEXT} AS small_industry,
          COUNT(*)::int AS count
        FROM public.master_data
        ${whereSql}
        GROUP BY 1, 2
      `;

      const res = await pool.query(sql, params);
      const smallMap = new Map<string, Set<string>>();
      const bigCountMap = new Map<string, number>();
      const smallCountMap = new Map<string, Record<string, number>>();

      res.rows.forEach((row) => {
        const bigIndustry =
          typeof row.big_industry === "string" ? row.big_industry.trim() : "";
        const smallIndustry =
          typeof row.small_industry === "string"
            ? row.small_industry.trim()
            : "";
        const count = Number(row.count ?? 0);

        if (!bigIndustry) return;

        bigCountMap.set(
          bigIndustry,
          (bigCountMap.get(bigIndustry) ?? 0) + count
        );

        if (!smallMap.has(bigIndustry)) {
          smallMap.set(bigIndustry, new Set<string>());
        }

        if (!smallCountMap.has(bigIndustry)) {
          smallCountMap.set(bigIndustry, {});
        }

        if (smallIndustry) {
          smallMap.get(bigIndustry)?.add(smallIndustry);
          smallCountMap.get(bigIndustry)![smallIndustry] =
            (smallCountMap.get(bigIndustry)?.[smallIndustry] ?? 0) + count;
        }
      });

      const items = Array.from(smallMap.keys())
        .map((bigIndustry) => ({
          industryParent: resolveIndustryParent(bigIndustry),
          bigIndustry,
          bigIndustryCount: bigCountMap.get(bigIndustry) ?? 0,
          smallIndustries: Array.from(smallMap.get(bigIndustry) ?? []).sort((a, b) =>
            a.localeCompare(b, "ja")
          ),
          smallIndustryCounts: smallCountMap.get(bigIndustry) ?? {},
        }))
        .sort((a, b) => {
          const parentIndexA = INDUSTRY_PARENT_SORT_ORDER.indexOf(
            a.industryParent as never
          );
          const parentIndexB = INDUSTRY_PARENT_SORT_ORDER.indexOf(
            b.industryParent as never
          );

          if (parentIndexA !== parentIndexB) {
            return parentIndexA - parentIndexB;
          }

          return a.bigIndustry.localeCompare(b.bigIndustry, "ja");
        });

      return NextResponse.json({
        ok: true,
        bigIndustries: items.map((item) => item.bigIndustry),
        smallIndustries: Array.from(
          new Set(items.flatMap((item) => item.smallIndustries))
        ).sort(),
        items,
      });
    }

    if (advancedValuesFor === "established") {
      const { whereSql, params } = buildWhereClause(searchParams);

      const sql = `
        SELECT DISTINCT year_month
        FROM (
          SELECT ${ESTABLISHED_YM_EXPR} AS year_month
          FROM public.master_data
          ${whereSql}
        ) src
        WHERE year_month IS NOT NULL
        ORDER BY year_month ASC
      `;

      const res = await pool.query(sql, params);

      const yearMonths = res.rows
        .map((row) =>
          typeof row.year_month === "string" ? row.year_month.trim() : ""
        )
        .filter((value) => value !== "");

      const monthsByYear: Record<string, string[]> = {};

      yearMonths.forEach((ym) => {
        const year = ym.slice(0, 4);
        const month = ym.slice(4, 6);

        if (!monthsByYear[year]) {
          monthsByYear[year] = [];
        }

        if (!monthsByYear[year].includes(month)) {
          monthsByYear[year].push(month);
        }
      });

      Object.keys(monthsByYear).forEach((year) => {
        monthsByYear[year].sort();
      });

      return NextResponse.json({
        ok: true,
        years: Object.keys(monthsByYear).sort(),
        monthsByYear,
        yearMonths,
      });
    }

    if (advancedValuesFor === "tag") {
      const { whereSql, params } = buildWhereClause(searchParams);
      const tagWhereSql = whereSql
        ? `${whereSql} AND NULLIF(BTRIM(tag_value), '') IS NOT NULL`
        : `WHERE NULLIF(BTRIM(tag_value), '') IS NOT NULL`;

      const sql = `
        SELECT
          BTRIM(tag_value) AS tag,
          ${TAG_PARENT_CASE_FROM_SPLIT} AS parent,
          COUNT(*)::int AS count
        FROM public.master_data
        CROSS JOIN LATERAL regexp_split_to_table(COALESCE("新規登録タグ"::text, ''), E'\\s*;\\s*') AS tag_value
        ${tagWhereSql}
        GROUP BY 1, 2
        ORDER BY parent ASC, tag ASC
      `;

      const res = await pool.query(sql, params);
      const tagsByParent: Record<string, string[]> = {};
      const tagCountsByParent: Record<string, Record<string, number>> = {};
      const parentCountMap = new Map<string, number>();

      res.rows.forEach((row) => {
        const parent = typeof row.parent === "string" ? row.parent.trim() : "";
        const tag = typeof row.tag === "string" ? row.tag.trim() : "";
        const count = Number(row.count ?? 0);

        if (!parent || !tag) return;

        if (!tagsByParent[parent]) {
          tagsByParent[parent] = [];
        }

        if (!tagCountsByParent[parent]) {
          tagCountsByParent[parent] = {};
        }

        if (!tagsByParent[parent].includes(tag)) {
          tagsByParent[parent].push(tag);
        }

        tagCountsByParent[parent][tag] =
          (tagCountsByParent[parent][tag] ?? 0) + count;

        parentCountMap.set(
          parent,
          (parentCountMap.get(parent) ?? 0) + count
        );
      });

      Object.keys(tagsByParent).forEach((parent) => {
        tagsByParent[parent].sort();
      });

      const parents = Object.keys(tagsByParent).sort();

      return NextResponse.json({
        ok: true,
        parents,
        tags: Array.from(
          new Set(res.rows.map((row) => row.tag).filter(Boolean))
        ).sort(),
        items: parents.map((parent) => ({
          parent,
          parentCount: parentCountMap.get(parent) ?? 0,
          tags: tagsByParent[parent],
          tagCounts: tagCountsByParent[parent] ?? {},
        })),
      });
    }

    if (valuesFor && FILTER_COLUMN_MAP[valuesFor]) {
      const { whereSql, params } = buildWhereClause(searchParams, valuesFor);

      const valueColumnText = `COALESCE(${FILTER_COLUMN_MAP[valuesFor]}::text, '')`;

      const valuesSql = `
        SELECT
          value,
          COUNT(*)::int AS count
        FROM (
          SELECT ${valueColumnText} AS value
          FROM public.master_data
          ${whereSql}
        ) src
        GROUP BY value
        ORDER BY value ASC
        LIMIT 300
      `;

      const valuesRes = await pool.query(valuesSql, params);

      const values = valuesRes.rows.map((row) => row.value ?? "");
      const valueCounts = Object.fromEntries(
        valuesRes.rows.map((row) => [row.value ?? "", Number(row.count ?? 0)])
      );

      return NextResponse.json({
        ok: true,
        values,
        valueCounts,
      });
    }

    const page = Math.max(Number(searchParams.get("page") || "1"), 1);
    const limitParam = searchParams.get("limit") || "200";
    const isAll = limitParam === "all";
    const limit = isAll ? null : Math.max(Number(limitParam), 1);

    const { whereSql, params } = buildWhereClause(searchParams);

    const totalSql = `
      SELECT COUNT(*)::int AS total_count
      FROM public.master_data
      ${whereSql}
    `;

    const totalRes = await pool.query(totalSql, params);
    const total = Number(totalRes.rows[0]?.total_count ?? 0);

    let rowsSql = `
      SELECT
        "企業名" AS company,
        "郵便番号" AS zipcode,
        "住所" AS address,
        "大業種名" AS big_industry,
        "小業種名" AS small_industry,
        "企業名（かな）" AS company_kana,
        "企業概要" AS summary,
        "企業サイトURL" AS website_url,
        "問い合わせフォームURL" AS form_url,
        "電話番号" AS phone,
        "FAX番号" AS fax,
        "メールアドレス" AS email,
        "設立年月" AS established_date,
        "代表者名" AS representative_name,
        "代表者役職" AS representative_title,
        "資本金" AS capital,
        "従業員数" AS employee_count,
        "従業員数年度" AS employee_count_year,
        "前年売上高" AS previous_sales,
        "直近売上高" AS latest_sales,
        "決算月" AS closing_month,
        "事業所数" AS office_count,
        "新規登録タグ" AS new_tag,
        "業種" AS business_type,
        "事業内容" AS business_content,
        "業界" AS industry_category,
        "メモ" AS memo
      FROM public.master_data
      ${whereSql}
      ${buildOrderBy(searchParams)}
    `;

    const queryParams = [...params];

    if (!isAll) {
      const offset = (page - 1) * (limit || 200);
      queryParams.push(limit || 200, offset);
      rowsSql += ` LIMIT $${queryParams.length - 1} OFFSET $${queryParams.length}`;
    }

    const rowsRes = await pool.query(rowsSql, queryParams);
    const rows = rowsRes.rows;

    return NextResponse.json({
      ok: true,
      total,
      page: isAll ? 1 : page,
      limit: isAll ? "all" : limit,
      totalPages: isAll ? 1 : Math.max(Math.ceil(total / (limit || 200)), 1),
      rows: rows,
    });
  } catch (error) {
    console.error("API ERROR:", error);
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "不明なエラー",
      },
      { status: 500 }
    );
  }
}

export async function POST(req: NextRequest) {
  const client = await pool.connect();

  try {
    await dbReady;
    const formData = await req.formData();
    const files = formData
      .getAll("files")
      .filter((item): item is File => item instanceof File);

    const singleFile = formData.get("file");
    if (files.length === 0 && singleFile instanceof File) {
      files.push(singleFile);
    }

    const skipDuplicateCheck =
      formData.get("skipDuplicateCheck") === "1";

    if (files.length === 0) {
      return NextResponse.json(
        { ok: false, error: "CSVファイルが選択されていません" },
        { status: 400 }
      );
    }

    const records: Record<(typeof CSV_HEADER_COLUMNS)[number], string | null>[] = [];

    const invalidHeaderFiles: string[] = [];

    for (const file of files) {
      const text = (await file.text()).replace(/^\uFEFF/, "");
      const rows = parseCsv(text);

      if (rows.length < 2) {
        continue;
      }

      const headerRow = rows[0].map((value) => normalizeHeader(value));

      if (!hasExactCsvHeaders(headerRow)) {
        invalidHeaderFiles.push(file.name);
        continue;
      }

      const headerIndexMap = Object.fromEntries(
        CSV_HEADER_COLUMNS.map((column, index) => [column, index])
      ) as Record<(typeof CSV_HEADER_COLUMNS)[number], number>;

      const fileRecords = rows
        .slice(1)
        .filter((row) => row.some((value) => value.trim() !== ""))
        .map((row) => {
          const record = Object.fromEntries(
            CSV_HEADER_COLUMNS.map((column) => [column, null])
          ) as Record<(typeof CSV_HEADER_COLUMNS)[number], string | null>;

          CSV_HEADER_COLUMNS.forEach((column) => {
            const value = row[headerIndexMap[column]] ?? "";
            record[column] = value.trim() === "" ? null : value.trim();
          });

          return record;
        });

      records.push(...fileRecords);
    }

    if (invalidHeaderFiles.length > 0) {
      return NextResponse.json(
        {
          ok: false,
          error: `CSVヘッダーが一致しないファイルがあります: ${invalidHeaderFiles.join(", ")}`,
        },
        { status: 400 }
      );
    }

    if (records.length === 0) {
      return NextResponse.json(
        { ok: false, error: "CSVに取込対象データがありません" },
        { status: 400 }
      );
    }

    let recordsToInsert = records;
    let skipped = 0;

    if (!skipDuplicateCheck) {
      const csvCompanies = Array.from(
        new Set(
          records
            .map((record) => record["企業名"]?.trim() ?? "")
            .filter((company) => company !== "")
        )
      );

      const existingCompanySet = new Set<string>();

      if (csvCompanies.length > 0) {
        const existingRes = await client.query(
          `
            SELECT "企業名"
            FROM public.master_data
            WHERE "企業名" = ANY($1::text[])
          `,
          [csvCompanies]
        );

        existingRes.rows.forEach((row) => {
          const company =
            typeof row["企業名"] === "string" ? row["企業名"].trim() : "";

          if (company !== "") {
            existingCompanySet.add(company);
          }
        });
      }

      const seenCsvCompanySet = new Set<string>();

      recordsToInsert = records.filter((record) => {
        const company = record["企業名"]?.trim() ?? "";

        if (company === "") {
          return true;
        }

        if (existingCompanySet.has(company)) {
          return false;
        }

        if (seenCsvCompanySet.has(company)) {
          return false;
        }

        seenCsvCompanySet.add(company);
        return true;
      });

      skipped = records.length - recordsToInsert.length;
    }

    if (recordsToInsert.length === 0) {
      return NextResponse.json({
        ok: true,
        inserted: 0,
        message: `0件を取り込みました（重複 ${skipped.toLocaleString()} 件をスキップ）`,
      });
    }

    await client.query("BEGIN");

    const batchSize = 300;

    for (let i = 0; i < recordsToInsert.length; i += batchSize) {
      const batch = recordsToInsert.slice(i, i + batchSize);
      const values: (string | null)[] = [];

      const placeholders = batch
        .map((record) => {
          const rowPlaceholders = DB_INSERT_COLUMNS.map((dbColumn) => {
            const csvHeader = DB_INSERT_TO_CSV_HEADER[dbColumn];
            values.push(record[csvHeader] ?? null);
            return `$${values.length}`;
          });
          return `(${rowPlaceholders.join(", ")})`;
        })
        .join(", ");

      const sql = `
        INSERT INTO public.master_data (
          ${DB_INSERT_COLUMNS.map((column) => `"${column}"`).join(", ")}
        )
        VALUES ${placeholders}
      `;

      await client.query(sql, values);
    }

    await client.query("COMMIT");

    return NextResponse.json({
      ok: true,
      inserted: recordsToInsert.length,
      message: skipDuplicateCheck
        ? `${recordsToInsert.length.toLocaleString()}件を取り込みました`
        : `${recordsToInsert.length.toLocaleString()}件を取り込みました（重複 ${skipped.toLocaleString()} 件をスキップ）`,
    });
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("CSV IMPORT ERROR:", error);

    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "CSV取込でエラーが発生しました",
      },
      { status: 500 }
    );
  } finally {
    client.release();
  }
}

export async function DELETE(req: NextRequest) {
  const client = await pool.connect();

  try {
    await dbReady;
    const { searchParams } = new URL(req.url);
    const deleteMode = searchParams.get("deleteMode");

    await client.query("BEGIN");

    if (deleteMode === "list") {
      const deleteScope =
        searchParams.get("deleteScope") === "all" ? "all" : "filtered";

      const { whereSql, params } =
        deleteScope === "filtered"
          ? buildWhereClause(searchParams)
          : { whereSql: "", params: [] as (string | number)[] };

      const result = await client.query(
        `
          WITH deleted AS (
            DELETE FROM public.master_data
            ${whereSql}
            RETURNING 1
          )
          SELECT COUNT(*)::int AS deleted_count
          FROM deleted
        `,
        params
      );

      await client.query("COMMIT");

      const deleted = result.rows[0]?.deleted_count ?? 0;

      return NextResponse.json({
        ok: true,
        deleted,
        message:
          deleteScope === "all"
            ? `${deleted.toLocaleString()}件の全リストを削除しました`
            : `${deleted.toLocaleString()}件の現在絞り込んでいるリストを削除しました`,
      });
    }

    const result = await client.query(`
      WITH duplicate_rows AS (
        SELECT
          ctid,
          ROW_NUMBER() OVER (
            PARTITION BY BTRIM("企業名"::text)
            ORDER BY ctid
          ) AS rn
        FROM public.master_data
        WHERE "企業名" IS NOT NULL
          AND BTRIM("企業名"::text) <> ''
      ),
      deleted AS (
        DELETE FROM public.master_data target
        USING duplicate_rows dup
        WHERE target.ctid = dup.ctid
          AND dup.rn > 1
        RETURNING 1
      )
      SELECT COUNT(*)::int AS deleted_count
      FROM deleted
    `);

    await client.query("COMMIT");

    const deleted = result.rows[0]?.deleted_count ?? 0;

    return NextResponse.json({
      ok: true,
      deleted,
      message: `${deleted.toLocaleString()}件の重複データを削除しました`,
    });
  } catch (error) {
    await client.query("ROLLBACK");

    return NextResponse.json(
      {
        ok: false,
        error:
          error instanceof Error
            ? error.message
            : "削除処理でエラーが発生しました",
      },
      { status: 500 }
    );
  } finally {
    client.release();
  }
}