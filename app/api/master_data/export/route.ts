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

const CSV_COLUMNS = [
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

function buildBlankCheckClause(column: string) {
  return `NULLIF(regexp_replace(COALESCE(${column}::text, ''), '[\\s　]+', '', 'g'), '') IS NULL`;
}

function buildNotBlankCheckClause(column: string) {
  return `NULLIF(regexp_replace(COALESCE(${column}::text, ''), '[\\s　]+', '', 'g'), '') IS NOT NULL`;
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

function buildWhereClause(searchParams: URLSearchParams) {
  const filterModels = parseFilterModels(searchParams);
  const advancedFilters = parseAdvancedFilters(searchParams);
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

const EXPORT_BATCH_SIZE = 1000;

function escapeCsvValue(value: unknown) {
  const text = value == null ? "" : String(value);
  return `"${text.replace(/"/g, `""`)}"`;
}

function createCsvDownloadStream(options: {
  baseSql: string;
  baseParams: unknown[];
  dedupeByCompany: boolean;
}) {
  const { baseSql, baseParams, dedupeByCompany } = options;
  const encoder = new TextEncoder();

  return new ReadableStream<Uint8Array>({
    async start(controller) {
      const client = await pool.connect();
      const seenCompanies = new Set<string>();
      let offset = 0;

      try {
        controller.enqueue(new Uint8Array([0xef, 0xbb, 0xbf]));
        controller.enqueue(
          encoder.encode(
            `${CSV_COLUMNS.map((col) => escapeCsvValue(col)).join(",")}\r\n`
          )
        );

        while (true) {
          const queryParams = [...baseParams, EXPORT_BATCH_SIZE, offset];
          const chunkSql = `
            ${baseSql}
            LIMIT $${queryParams.length - 1}
            OFFSET $${queryParams.length}
          `;

          const result = await client.query(chunkSql, queryParams);
          const rows = result.rows as Array<Record<string, unknown>>;

          if (rows.length === 0) {
            break;
          }

          for (const row of rows) {
            if (dedupeByCompany) {
              const company = String(row["企業名"] ?? "").trim();

              if (company !== "") {
                if (seenCompanies.has(company)) {
                  continue;
                }
                seenCompanies.add(company);
              }
            }

            const line = CSV_COLUMNS.map((col) =>
              escapeCsvValue(row[col])
            ).join(",");
            controller.enqueue(encoder.encode(`${line}\r\n`));
          }

          offset += rows.length;

          if (rows.length < EXPORT_BATCH_SIZE) {
            break;
          }
        }

        controller.close();
      } catch (error) {
        controller.error(error);
      } finally {
        client.release();
      }
    },
  });
}

function buildSearchParamsFromExportPayload(payload: Record<string, unknown>) {
  const searchParams = new URLSearchParams();

  const simpleKeys = [
    "sortKey",
    "sortDirection",
    "exportScope",
  ] as const;

  simpleKeys.forEach((key) => {
    const value = payload[key];
    if (value !== undefined && value !== null && value !== "") {
      searchParams.set(key, String(value));
    }
  });

  if (payload.dedupeByCompany === true || payload.dedupeByCompany === "1") {
    searchParams.set("dedupeByCompany", "1");
  }

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

  return searchParams;
}

async function handleExportRequest(searchParams: URLSearchParams) {
  await dbReady;
  await ensureMasterDataIdColumn(pool);

  const exportScope = searchParams.get("exportScope") || "filtered";
  const dedupeByCompany = searchParams.get("dedupeByCompany") === "1";

  const { whereSql, params } =
    exportScope === "all"
      ? { whereSql: "", params: [] as unknown[] }
      : buildWhereClause(searchParams);

  const baseSql = `
    SELECT
      "企業名" AS "企業名",
      "郵便番号" AS "郵便番号",
      "住所" AS "住所",
      "大業種名" AS "大業種",
      "小業種名" AS "小業種",
      "企業名（かな）" AS "企業名（かな）",
      "企業概要" AS "企業概要",
      "企業サイトURL" AS "企業URL",
      "問い合わせフォームURL" AS "お問い合わせフォームURL",
      "電話番号" AS "電話番号",
      "FAX番号" AS "FAX番号",
      "メールアドレス" AS "メールアドレス",
      "設立年月" AS "設立年月",
      "代表者名" AS "代表者名",
      "代表者役職" AS "代表者役職",
      "資本金" AS "資本金",
      "従業員数" AS "従業員数",
      "従業員数年度" AS "従業員数年度",
      "前年売上高" AS "前年売上高",
      "直近売上高" AS "直近売上高",
      "決算月" AS "決算月",
      "事業所数" AS "事業所数",
      "新規登録タグ" AS "タグ",
      "業種" AS "業種",
      "事業内容" AS "事業内容",
      "業界" AS "業界",
      "許可番号" AS "許可番号",
      "メモ" AS "メモ"
    FROM public.master_data
    ${whereSql}
    ${buildOrderBy(searchParams)}
  `;

  const csvStream = createCsvDownloadStream({
    baseSql,
    baseParams: params,
    dedupeByCompany,
  });

  const fileName = createFileName();

  return new NextResponse(csvStream, {
    status: 200,
    headers: {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="${fileName}"; filename*=UTF-8''${encodeURIComponent(fileName)}`,
      "Cache-Control": "no-store",
      "X-Content-Type-Options": "nosniff",
    },
  });
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
    return await handleExportRequest(searchParams);
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "CSV抽出でエラーが発生しました",
      },
      { status: 500 }
    );
  }
}

export async function POST(req: NextRequest) {
  try {
    const payload = (await req.json()) as Record<string, unknown>;
    const searchParams = buildSearchParamsFromExportPayload(payload);
    return await handleExportRequest(searchParams);
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "CSV抽出でエラーが発生しました",
      },
      { status: 500 }
    );
  }
}