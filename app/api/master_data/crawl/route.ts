import { NextRequest, NextResponse } from "next/server";
import { pool } from "@/lib/db";
import { crawlCompanyWebsite } from "@/lib/master-data-crawler";

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
  representative_title: string | null;
  capital: string | null;
  employee_count: string | null;
  business_content: string | null;
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
  | "business_content";

type CrawlPayloadCandidateKey = Extract<
  CrawlSelectableFieldKey,
  "phone" | "fax" | "email" | "zipcode" | "address"
>;

type CrawlPreviewSelectableKey = CrawlSelectableFieldKey;

type CrawlPreviewChange = {
  key: CrawlPreviewSelectableKey;
  label: string;
  before: string | null;
  after: string | null;
  candidates: string[];
};

type CrawlPreviewRow = {
  row_id: string;
  preview_row_id: string;
  company: string | null;
  website_url: string | null;
  changes: CrawlPreviewChange[];
};

type SelectedCrawlChanges = Partial<
  Record<string, Partial<Record<CrawlPreviewSelectableKey, string>>>
>;

type CrawlRequestBody = {
  filterModels?: Partial<Record<FilterKey, FilterModel>>;
  advancedFilters?: AdvancedFilters;
  sortKey?: FilterKey | null;
  sortDirection?: SortDirection | "" | null;
  previewOnly?: boolean;
  selectedChanges?: SelectedCrawlChanges;
  selectedFields?: CrawlSelectableFieldKey[];
};

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
];

function normalizeSelectedFields(selectedFields?: CrawlSelectableFieldKey[]) {
  const requested = Array.isArray(selectedFields)
    ? selectedFields.filter((field): field is CrawlSelectableFieldKey =>
        CRAWL_SELECTABLE_FIELDS.includes(field)
      )
    : null;

  return new Set<CrawlSelectableFieldKey>(
    requested === null ? CRAWL_SELECTABLE_FIELDS : requested
  );
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

function buildCrawlPayloadBundles(
  extracted: Awaited<ReturnType<typeof crawlCompanyWebsite>>,
  sourceCompany: string | null
): CrawlPayloadBundle[] {
  const extractedCompany = normalizeNullableText(extracted.company);
  const fallbackCompany = normalizeNullableText(sourceCompany);
  const baseCompany =
    extractedCompany && isLikelyCompanyName(extractedCompany)
      ? extractedCompany
      : fallbackCompany;

  const officeSources =
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

    return {
      payload: {
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
        representative_title: normalizeNullableText(
          extracted.representative_title
        ),
        capital: normalizeNullableText(extracted.capital),
        employee_count: normalizeNullableText(extracted.employee_count),
        business_content: normalizeNullableText(extracted.business_content),
      },
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
    if (!selectedFieldSet.has(field.key)) continue;

    const before = normalizeNullableText(row[field.key]);
    const after = getResolvedValue(row[field.key], bundle.payload[field.key]);

    if (before === after) continue;
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

function buildSelectedPayload(
  bundle: CrawlPayloadBundle,
  selectedChanges: SelectedCrawlChanges | undefined,
  previewRowId: string
): CrawlPayload {
  const selected = selectedChanges?.[previewRowId] ?? {};

  return {
    ...bundle.payload,
    company: normalizeNullableText(selected.company) ?? bundle.payload.company,
    website_url:
      normalizeNullableText(selected.website_url) ?? bundle.payload.website_url,
    phone: normalizeNullableText(selected.phone) ?? bundle.payload.phone,
    fax: normalizeNullableText(selected.fax) ?? bundle.payload.fax,
    email: normalizeNullableText(selected.email) ?? bundle.payload.email,
    zipcode: normalizeNullableText(selected.zipcode) ?? bundle.payload.zipcode,
    address: normalizeNullableText(selected.address) ?? bundle.payload.address,
    form_url: normalizeNullableText(selected.form_url) ?? bundle.payload.form_url,
    established_date:
      normalizeNullableText(selected.established_date) ??
      bundle.payload.established_date,
    representative_name:
      normalizeNullableText(selected.representative_name) ??
      bundle.payload.representative_name,
    representative_title: bundle.payload.representative_title,
    capital: normalizeNullableText(selected.capital) ?? bundle.payload.capital,
    employee_count:
      normalizeNullableText(selected.employee_count) ??
      bundle.payload.employee_count,
    business_content:
      normalizeNullableText(selected.business_content) ??
      bundle.payload.business_content,
  };
}

function getSelectedChangeKeys(
  previewRowId: string,
  selectedChanges: SelectedCrawlChanges | undefined
) {
  const selected = selectedChanges?.[previewRowId];
  if (!selected) return [];

  return Object.entries(selected)
    .filter(([, value]) => normalizeNullableText(value) !== null)
    .map(([key]) => key as CrawlPreviewSelectableKey);
}

function hasAnySelectedCandidate(
  selectedChanges: SelectedCrawlChanges | undefined,
  previewRowId: string
) {
  return getSelectedChangeKeys(previewRowId, selectedChanges).length > 0;
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
  };
}

export async function POST(req: NextRequest) {
  const client = await pool.connect();

  try {
    const body = (await req.json()) as CrawlRequestBody;
    const filterModels = body.filterModels ?? {};
    const advancedFilters = body.advancedFilters ?? {};
    const selectedFieldSet = normalizeSelectedFields(body.selectedFields);

    const { whereSql, params } = buildWhereClause(filterModels, advancedFilters);

    const websiteRequired = `NULLIF(BTRIM(COALESCE("企業サイトURL"::text, '')), '') IS NOT NULL`;
    const targetWhereSql = whereSql
      ? `${whereSql} AND ${websiteRequired}`
      : `WHERE ${websiteRequired}`;

    const targetSql = `
      SELECT
        md.ctid::text AS row_id,
        to_jsonb(md) AS source_row,
        md."企業名" AS company,
        md."企業サイトURL" AS website_url,
        md."問い合わせフォームURL" AS form_url,
        md."電話番号" AS phone,
        md."FAX番号" AS fax,
        md."メールアドレス" AS email,
        md."郵便番号" AS zipcode,
        md."住所" AS address,
        md."設立年月" AS established_date,
        md."代表者名" AS representative_name,
        md."代表者役職" AS representative_title,
        md."資本金" AS capital,
        md."従業員数" AS employee_count,
        md."事業内容" AS business_content
      FROM public.master_data AS md
      ${targetWhereSql}
      ${buildOrderBy(body.sortKey, body.sortDirection)}
    `;

    const targetRes = await client.query(targetSql, params);
    const targets = targetRes.rows;

    let processed = 0;
    let updated = 0;
    let skipped = 0;
    let failed = 0;

    const previewRows: CrawlPreviewRow[] = [];

    for (const row of targets) {
      processed += 1;

      try {
        const extracted = await crawlCompanyWebsite(
          String(row.website_url ?? ""),
          Array.from(selectedFieldSet)
        );
        const bundles = buildCrawlPayloadBundles(
          extracted,
          normalizeNullableText(row.company)
        );

        if (bundles.length === 0) {
          skipped += 1;
          continue;
        }

        if (body.previewOnly) {
          let previewAdded = 0;

          bundles.forEach((bundle, officeIndex) => {
            const changes = buildPreviewChanges(
              row as Record<string, unknown>,
              bundle,
              selectedFieldSet
            );

            if (changes.length === 0) {
              return;
            }

            previewRows.push({
              row_id: String(row.row_id ?? ""),
              preview_row_id: `${String(row.row_id ?? "")}__${officeIndex}`,
              company: bundle.payload.company ?? normalizeNullableText(row.company),
              website_url: normalizeNullableText(row.website_url),
              changes,
            });

            previewAdded += 1;
          });

          if (previewAdded > 0) {
            updated += previewAdded;
          } else {
            skipped += 1;
          }

          continue;
        }

        let rowHandled = false;
        const sourceRow = (row.source_row ?? {}) as Record<string, unknown>;

        for (const [officeIndex, bundle] of bundles.entries()) {
          const previewRowId = `${String(row.row_id ?? "")}__${officeIndex}`;

          if (!hasAnySelectedCandidate(body.selectedChanges, previewRowId)) {
            continue;
          }

          const selectedKeys = new Set(
            getSelectedChangeKeys(previewRowId, body.selectedChanges)
          );

          const payload = buildSelectedPayload(
            bundle,
            body.selectedChanges,
            previewRowId
          );

          if (officeIndex === 0) {
            const updateSql = `
              UPDATE public.master_data
              SET
                "企業名" = CASE
                  WHEN $14::boolean THEN COALESCE($1, "企業名")
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
                "事業内容" = COALESCE($13, "事業内容")
              WHERE ctid = $15::tid
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
              selectedKeys.has("representative_name") ? payload.representative_name : null,
              selectedKeys.has("capital") ? payload.capital : null,
              selectedKeys.has("employee_count") ? payload.employee_count : null,
              selectedKeys.has("business_content") ? payload.business_content : null,
              selectedKeys.has("company") && bundle.forceCompanyUpdate,
              row.row_id,
            ];

            const updateRes = await client.query(updateSql, updateValues);

            if ((updateRes.rowCount ?? 0) > 0) {
              updated += 1;
              rowHandled = true;
            }

            continue;
          }

          const insertRow = buildInsertRow(sourceRow, bundle, payload, selectedKeys);
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
          rowHandled = true;
        }

        if (!rowHandled) {
          skipped += 1;
        }
      } catch {
        failed += 1;
      }
    }

    if (body.previewOnly) {
      return NextResponse.json({
        ok: true,
        preview: true,
        previewRows,
        processed,
        updated: previewRows.length,
        skipped,
        failed,
        message:
          previewRows.length > 0
            ? `保存候補 ${previewRows.length} 件です`
            : "保存候補はありませんでした",
      });
    }

    return NextResponse.json({
      ok: true,
      processed,
      updated,
      skipped,
      failed,
      message: `クローリング完了：対象 ${processed} 件 / 更新 ${updated} 件 / スキップ ${skipped} 件 / 失敗 ${failed} 件`,
    });
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
    client.release();
  }
}