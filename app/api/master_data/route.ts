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
  "許可番号",
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
  "許可番号",
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
  "許可番号": "許可番号",
  "メモ": "メモ",
};

async function ensureMasterDataIdColumn(
  client: { query: (sql: string) => Promise<unknown> }
) {
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

type RepresentativeNameDeleteCandidate = {
  rowId: string;
  company: string | null;
  representativeName: string | null;
  afterValue: string | null;
  action: "update" | "delete" | "review";
  reason: string;
};

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
  /^(?:[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]{4,}(?:市|区|町|村)|[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]{2,}市立[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]+)$/u;

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

function isLikelyPersonalNameEndingWithShi(value: string) {
  const text = trimRepresentativeAffixes(value.trim()).normalize("NFKC");

  if (!/^[\p{sc=Han}々ヶヵ]{3,5}市$/u.test(text)) return false;
  if (REPRESENTATIVE_STRICT_NON_NAME_AREA_TOKENS.has(text)) return false;
  if (isRepresentativeAreaToken(text)) return false;
  if (REPRESENTATIVE_ORGANIZATION_LIKE_REGEX.test(text)) return false;
  if (REPRESENTATIVE_NON_NAME_CONTENT_REGEX.test(text)) return false;

  return true;
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
    
    if (
      REPRESENTATIVE_MUNICIPALITY_LIKE_REGEX.test(part) &&
      !isLikelyPersonalNameEndingWithShi(part)
    ) {
      return false;
    }

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

  if (
    REPRESENTATIVE_MUNICIPALITY_LIKE_REGEX.test(text) &&
    !isLikelyPersonalNameEndingWithShi(text)
  ) {
    return true;
  }

  if (REPRESENTATIVE_ORGANIZATION_LIKE_REGEX.test(text)) return true;
  if (REPRESENTATIVE_NON_NAME_CONTENT_REGEX.test(text)) return true;
  if (REPRESENTATIVE_ADDRESS_LIKE_REGEX.test(text)) return true;
  if (/株式会社|有限会社|合同会社|御中|様/.test(text)) return true;
  if (containsRepresentativeStopword(text)) return true;

  return false;
}

function shouldAutoDeleteRepresentativeName(value: string) {
  const normalizedSource = trimRepresentativeAffixes(
    normalizeRepresentativeSource(value)
  );

  if (!normalizedSource) {
    return true;
  }

  return (
    REPRESENTATIVE_NON_NAME_EXACT_VALUES.has(normalizedSource) ||
    REPRESENTATIVE_NON_NAME_PREFIX_REGEX.test(normalizedSource)
  );
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

  if (!normalizedSource) {
    return {
      cleanedValue: null as string | null,
      shouldUpdate: false,
      shouldDelete: true,
      shouldReview: false,
      reason: "空欄相当のため削除候補",
    };
  }

  if (shouldAutoDeleteRepresentativeName(original)) {
    return {
      cleanedValue: null as string | null,
      shouldUpdate: false,
      shouldDelete: true,
      shouldReview: false,
      reason: "氏名ではないことが明確なため削除候補",
    };
  }

  if (looksLikeNonNameToken(normalizedSource)) {
    return {
      cleanedValue: null as string | null,
      shouldUpdate: false,
      shouldDelete: false,
      shouldReview: true,
      reason: "氏名ではない可能性はあるが自動削除は危険なため要確認",
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

async function handleRepresentativeNameInspectionPreview(
  payload: Record<string, unknown>
) {
  const client = await pool.connect();

  try {
    await dbReady;
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

    const targetRes = await client.query(
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

    const removeNonNameSelected =
      payload.methodSelections != null &&
      typeof payload.methodSelections === "object" &&
      (payload.methodSelections as Record<string, unknown>)
        .representative_name_remove_non_name === true;

    const inspectNameSelected =
      payload.methodSelections != null &&
      typeof payload.methodSelections === "object" &&
      (payload.methodSelections as Record<string, unknown>)
        .representative_name_inspect_name === true;

    const inspectionCandidates: RepresentativeNameDeleteCandidate[] = [];

    targetRes.rows.forEach((row) => {
      const rowId = String(row.id ?? "");
      const company =
        typeof row.company === "string" ? row.company : null;
      const representativeName =
        typeof row.representative_name === "string"
          ? row.representative_name
          : null;

      const result = inspectRepresentativeNameValue(representativeName);

      if (result.shouldDelete && removeNonNameSelected) {
        inspectionCandidates.push({
          rowId,
          company,
          representativeName,
          afterValue: null,
          action: "delete",
          reason: result.reason,
        });
      } else if (result.shouldUpdate && inspectNameSelected) {
        inspectionCandidates.push({
          rowId,
          company,
          representativeName,
          afterValue: result.cleanedValue,
          action: "update",
          reason: result.reason,
        });
      } else if (result.shouldReview && inspectNameSelected) {
        inspectionCandidates.push({
          rowId,
          company,
          representativeName,
          afterValue: null,
          action: "review",
          reason: result.reason,
        });
      }
    });

    const updateCount = inspectionCandidates.filter(
      (candidate) => candidate.action === "update"
    ).length;

    const deleteCount = inspectionCandidates.filter(
      (candidate) => candidate.action === "delete"
    ).length;

    const reviewCount = inspectionCandidates.filter(
      (candidate) => candidate.action === "review"
    ).length;

    return NextResponse.json({
      ok: true,
      updated: inspectionCandidates.length,
      inspectionCandidates,
      inspectionDeleteCandidates: inspectionCandidates.filter(
        (candidate) => candidate.action === "delete"
      ),
      message:
        inspectionCandidates.length > 0
          ? `更新候補 ${updateCount.toLocaleString()}件 / 削除候補 ${deleteCount.toLocaleString()}件 / 要確認 ${reviewCount.toLocaleString()}件`
          : "候補はありませんでした",
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
            : "代表者名精査でエラーが発生しました",
      },
      { status: 500 }
    );
  } finally {
    client.release();
  }
}

async function handleRepresentativeNameInspectionDelete(
  payload: Record<string, unknown>
) {
  const client = await pool.connect();

  try {
    await dbReady;
    await ensureMasterDataIdColumn(client);

    const explicitChanges = Array.isArray(payload.changes)
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

    const legacyDeleteChanges =
      explicitChanges.length === 0 && Array.isArray(payload.rowIds)
        ? payload.rowIds
            .map((rowId) => String(rowId ?? "").trim())
            .filter((rowId) => rowId !== "")
            .map((rowId) => ({
              rowId,
              action: "delete" as const,
              afterValue: null as string | null,
            }))
        : [];

    const changes =
      explicitChanges.length > 0 ? explicitChanges : legacyDeleteChanges;

    if (changes.length === 0) {
      return NextResponse.json(
        { ok: false, error: "反映する候補が選択されていません" },
        { status: 400 }
      );
    }

    await client.query("BEGIN");

    let applied = 0;
    let updatedCount = 0;
    let deletedCount = 0;

    for (const change of changes) {
      const currentRes = await client.query(
        `
          SELECT "代表者名" AS representative_name
          FROM public.master_data
          WHERE id = $1::bigint
          LIMIT 1
        `,
        [change.rowId]
      );

      const currentRepresentativeName =
        typeof currentRes.rows[0]?.representative_name === "string"
          ? currentRes.rows[0].representative_name
          : null;

      const currentResult = inspectRepresentativeNameValue(
        currentRepresentativeName
      );

      if (!currentResult.shouldDelete) {
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
            : "削除候補反映でエラーが発生しました",
      },
      { status: 500 }
    );
  } finally {
    client.release();
  }
}

async function handleReadRequest(searchParams: URLSearchParams) {
  try {
    await dbReady;
    await ensureMasterDataIdColumn(pool);
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

      const valueSearch = (searchParams.get("valueSearch") || "").trim();
      const valueOffset = Math.max(
        Number(searchParams.get("valueOffset") || "0"),
        0
      );
      const valueLimit = Math.min(
        Math.max(Number(searchParams.get("valueLimit") || "200"), 1),
        1000
      );
      const currentValueFilterEnabled =
        searchParams.get("currentValueFilterEnabled") === "1";

      let currentSelectedValues: string[] = [];
      try {
        const parsed = JSON.parse(
          searchParams.get("currentSelectedValues") || "[]"
        );

        if (Array.isArray(parsed)) {
          currentSelectedValues = parsed
            .map((value) => String(value ?? ""))
            .filter((value, index, self) => self.indexOf(value) === index);
        }
      } catch {
        currentSelectedValues = [];
      }

      const valueColumnText = `COALESCE(${FILTER_COLUMN_MAP[valuesFor]}::text, '')`;

      const allGroupedValuesSql = `
        SELECT
          value,
          COUNT(*)::int AS count
        FROM (
          SELECT ${valueColumnText} AS value
          FROM public.master_data
          ${whereSql}
        ) src
        GROUP BY value
      `;

      const totalItemCountSql = `
        SELECT
          COALESCE(SUM(count), 0)::int AS total_item_count
        FROM (${allGroupedValuesSql}) grouped_values
        WHERE NULLIF(BTRIM(value), '') IS NOT NULL
      `;

      const totalItemCountRes = await pool.query(totalItemCountSql, params);
      const totalItemCount = Number(
        totalItemCountRes.rows[0]?.total_item_count ?? 0
      );

      let checkedItemCount = totalItemCount;

      if (currentValueFilterEnabled) {
        const selectedNonEmptyValues = currentSelectedValues.filter(
          (value) => value.trim() !== ""
        );

        if (selectedNonEmptyValues.length === 0) {
          checkedItemCount = 0;
        } else {
          const checkedItemParams = [...params, selectedNonEmptyValues];

          const checkedItemCountSql = `
            SELECT
              COALESCE(SUM(count), 0)::int AS checked_item_count
            FROM (${allGroupedValuesSql}) grouped_values
            WHERE value = ANY($${checkedItemParams.length}::text[])
              AND NULLIF(BTRIM(value), '') IS NOT NULL
          `;

          const checkedItemCountRes = await pool.query(
            checkedItemCountSql,
            checkedItemParams
          );

          checkedItemCount = Number(
            checkedItemCountRes.rows[0]?.checked_item_count ?? 0
          );
        }
      }

      const groupedParams = [...params];
      const groupedWhereParts: string[] = [];

      if (valueSearch !== "") {
        groupedParams.push(`%${valueSearch}%`);
        groupedWhereParts.push(`value ILIKE $${groupedParams.length}`);
      }

      const groupedWhereSql = groupedWhereParts.length
        ? `WHERE ${groupedWhereParts.join(" AND ")}`
        : "";

      const groupedValuesSql = `
        SELECT
          value,
          COUNT(*)::int AS count
        FROM (
          SELECT ${valueColumnText} AS value
          FROM public.master_data
          ${whereSql}
        ) src
        ${groupedWhereSql}
        GROUP BY value
      `;

      const summarySql = `
        SELECT
          COUNT(*)::int AS total_count,
          COALESCE(SUM(count), 0)::int AS matched_count
        FROM (${groupedValuesSql}) grouped_values
      `;

      const summaryRes = await pool.query(summarySql, groupedParams);
      const valueTotal = Number(summaryRes.rows[0]?.total_count ?? 0);
      const valueMatchedCount = Number(summaryRes.rows[0]?.matched_count ?? 0);

      const valuesQueryParams = [...groupedParams, valueLimit, valueOffset];

      const valuesSql = `
        SELECT
          value,
          count
        FROM (${groupedValuesSql}) grouped_values
        ORDER BY value ASC
        LIMIT $${valuesQueryParams.length - 1}
        OFFSET $${valuesQueryParams.length}
      `;

      const valuesRes = await pool.query(valuesSql, valuesQueryParams);

      const values = valuesRes.rows.map((row) => row.value ?? "");
      const valueCounts = Object.fromEntries(
        valuesRes.rows.map((row) => [row.value ?? "", Number(row.count ?? 0)])
      );

      let allValues: string[] | undefined = undefined;

      if (!currentValueFilterEnabled) {
        const allValuesSql = `
          SELECT
            value
          FROM (${allGroupedValuesSql}) grouped_values
          ORDER BY value ASC
        `;

        const allValuesRes = await pool.query(allValuesSql, params);
        allValues = allValuesRes.rows.map((row) => row.value ?? "");
      }

      return NextResponse.json({
        ok: true,
        values,
        allValues,
        valueCounts,
        valueTotal,
        valueMatchedCount,
        totalItemCount,
        checkedItemCount,
        valueOffset,
        valueLimit,
        hasMoreValues: valueOffset + values.length < valueTotal,
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
        "新規登録タグ" AS tag,
        "業種" AS business_type,
        "事業内容" AS business_content,
        "業界" AS industry_category,
        "許可番号" AS permit_number,
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

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  return handleReadRequest(searchParams);
}

export async function POST(req: NextRequest) {
  const contentType = req.headers.get("content-type") || "";

  if (contentType.includes("application/json")) {
    try {
      await dbReady;
      const payload = (await req.json()) as Record<string, unknown>;
      const action = String(payload.action ?? "");

      if (action === "read") {
        const searchParams = buildSearchParamsFromReadPayload(payload);
        return handleReadRequest(searchParams);
      }

      if (action === "preview_representative_name_item_inspection") {
        return handleRepresentativeNameInspectionPreview(payload);
      }

      if (action === "apply_representative_name_item_inspection_delete") {
        return handleRepresentativeNameInspectionDelete(payload);
      }

      return NextResponse.json(
        { ok: false, error: "不正なJSONリクエストです" },
        { status: 400 }
      );
    } catch (error) {
      console.error("JSON API ERROR:", error);
      return NextResponse.json(
        {
          ok: false,
          error: error instanceof Error ? error.message : "不明なエラー",
        },
        { status: 500 }
      );
    }
  }

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

    if (deleteMode === "item") {
      const deleteScope =
        searchParams.get("deleteScope") === "all" ? "all" : "filtered";

      const rawSelectedFields = searchParams.get("selectedFields");
      let selectedFields: FilterKey[] = [];

      if (rawSelectedFields) {
        try {
          const parsed = JSON.parse(rawSelectedFields);

          if (Array.isArray(parsed)) {
            selectedFields = parsed.filter(
              (field): field is FilterKey =>
                typeof field === "string" && field in FILTER_COLUMN_MAP
            );
          }
        } catch {
          selectedFields = [];
        }
      }

      if (selectedFields.length === 0) {
        await client.query("ROLLBACK");
        return NextResponse.json(
          { ok: false, error: "削除する項目が選択されていません" },
          { status: 400 }
        );
      }

      const { whereSql, params } =
        deleteScope === "filtered"
          ? buildWhereClause(searchParams)
          : { whereSql: "", params: [] as (string | number)[] };

      const setSql = selectedFields
        .map((field) => `${FILTER_COLUMN_MAP[field]} = NULL`)
        .join(", ");

      const result = await client.query(
        `
          WITH updated AS (
            UPDATE public.master_data
            SET ${setSql}
            ${whereSql}
            RETURNING 1
          )
          SELECT COUNT(*)::int AS updated_count
          FROM updated
        `,
        params
      );

      await client.query("COMMIT");

      const updated = result.rows[0]?.updated_count ?? 0;

      return NextResponse.json({
        ok: true,
        updated,
        message:
          deleteScope === "all"
            ? `${updated.toLocaleString()}件の全てのリストから選択項目を削除しました`
            : `${updated.toLocaleString()}件の現在絞り込んでいるリストから選択項目を削除しました`,
      });
    }

    if (deleteMode === "list") {
      const deleteScope =
        searchParams.get("deleteScope") === "all" ? "all" : "filtered";

      const { whereSql, params } =
        deleteScope === "filtered"
          ? buildWhereClause(searchParams)
          : { whereSql: "", params: [] as (string | number)[] };

      await ensureMasterDataIdColumn(client);

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
          id,
          ROW_NUMBER() OVER (
            PARTITION BY BTRIM("企業名"::text)
            ORDER BY id
          ) AS rn
        FROM public.master_data
        WHERE "企業名" IS NOT NULL
          AND BTRIM("企業名"::text) <> ''
      ),
      deleted AS (
        DELETE FROM public.master_data target
        USING duplicate_rows dup
        WHERE target.id = dup.id
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