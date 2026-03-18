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

const CSV_COLUMNS = [
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
  const where: string[] = [];
  const params: (string | number)[] = [];

  (Object.entries(FILTER_COLUMN_MAP) as [FilterKey, string][]).forEach(
    ([key, column]) => {
      const model = filterModels[key];
      addConditionClause(where, params, column, model);
      addValueFilterClause(where, params, column, model);
    }
  );

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