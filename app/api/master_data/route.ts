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
  searchParams: URLSearchParams,
  skipKey?: FilterKey
) {
  const filterModels = parseFilterModels(searchParams);
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
    const { searchParams } = new URL(req.url);
    const valuesFor = searchParams.get("valuesFor") as FilterKey | null;

    if (valuesFor && FILTER_COLUMN_MAP[valuesFor]) {
      const { whereSql, params } = buildWhereClause(searchParams, valuesFor);

      const valueColumnText = `COALESCE(${FILTER_COLUMN_MAP[valuesFor]}::text, '')`;

      const valuesSql = `
        SELECT DISTINCT ${valueColumnText} AS value
        FROM public.master_data
        ${whereSql}
        ORDER BY ${valueColumnText} ASC
        LIMIT 300
      `;

      const valuesRes = await pool.query(valuesSql, params);

      return NextResponse.json({
        ok: true,
        values: valuesRes.rows.map((row) => row.value ?? ""),
      });
    }

    const page = Math.max(Number(searchParams.get("page") || "1"), 1);
    const limitParam = searchParams.get("limit") || "200";
    const isAll = limitParam === "all";
    const limit = isAll ? null : Math.max(Number(limitParam), 1);

    const { whereSql, params } = buildWhereClause(searchParams);

    const countSql = `
      SELECT COUNT(*)::int AS total
      FROM public.master_data
      ${whereSql}
    `;
    const countRes = await pool.query(countSql, params);
    const total = countRes.rows[0]?.total ?? 0;

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
        "削除タグ" AS delete_tag,
        "削除フラグ" AS delete_flag,
        "強制フラグ" AS force_flag
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

    return NextResponse.json({
      ok: true,
      total,
      page: isAll ? 1 : page,
      limit: isAll ? "all" : limit,
      totalPages: isAll ? 1 : Math.max(Math.ceil(total / (limit || 200)), 1),
      rows: rowsRes.rows,
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
    const formData = await req.formData();
    const file = formData.get("file");

    if (!(file instanceof File)) {
      return NextResponse.json(
        { ok: false, error: "CSVファイルが選択されていません" },
        { status: 400 }
      );
    }

    const text = (await file.text()).replace(/^\uFEFF/, "");
    const rows = parseCsv(text);

    if (rows.length < 2) {
      return NextResponse.json(
        { ok: false, error: "CSVにデータ行がありません" },
        { status: 400 }
      );
    }

    const headerRow = rows[0].map((value) => normalizeHeader(value));
    const existingColumns = CSV_COLUMNS.filter((column) =>
      headerRow.includes(column)
    );

    if (existingColumns.length === 0) {
      return NextResponse.json(
        {
          ok: false,
          error: "CSVヘッダーに取込対象の列が見つかりませんでした",
        },
        { status: 400 }
      );
    }

    const headerIndexMap = Object.fromEntries(
      existingColumns.map((column) => [column, headerRow.indexOf(column)])
    ) as Record<(typeof CSV_COLUMNS)[number], number>;

    const records = rows
      .slice(1)
      .filter((row) => row.some((value) => value.trim() !== ""))
      .map((row) => {
        const record: Record<string, string | null> = {};
        existingColumns.forEach((column) => {
          const value = row[headerIndexMap[column]] ?? "";
          record[column] = value.trim() === "" ? null : value.trim();
        });
        return record;
      });

    if (records.length === 0) {
      return NextResponse.json(
        { ok: false, error: "CSVに取込対象データがありません" },
        { status: 400 }
      );
    }

    await client.query("BEGIN");

    const batchSize = 300;

    for (let i = 0; i < records.length; i += batchSize) {
      const batch = records.slice(i, i + batchSize);
      const values: (string | null)[] = [];

      const placeholders = batch
        .map((record) => {
          const rowPlaceholders = existingColumns.map((column) => {
            values.push(record[column] ?? null);
            return `$${values.length}`;
          });
          return `(${rowPlaceholders.join(", ")})`;
        })
        .join(", ");

      const sql = `
        INSERT INTO public.master_data (
          ${existingColumns.map((column) => `"${column}"`).join(", ")}
        )
        VALUES ${placeholders}
      `;

      await client.query(sql, values);
    }

    await client.query("COMMIT");

    return NextResponse.json({
      ok: true,
      inserted: records.length,
      message: `${records.length.toLocaleString()}件を取り込みました`,
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