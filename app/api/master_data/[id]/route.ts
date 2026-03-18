import { NextRequest, NextResponse } from "next/server";
import { pool } from "@/lib/db";

const FIELD_COLUMN_MAP = {
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

type EditableField = keyof typeof FIELD_COLUMN_MAP;

export async function PATCH(
  req: NextRequest,
  context: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await context.params;
    const rowId = Number(id);

    if (!Number.isInteger(rowId) || rowId <= 0) {
      return NextResponse.json(
        { ok: false, error: "IDが不正です" },
        { status: 400 }
      );
    }

    const body = await req.json();
    const field = body?.field as EditableField;
    const value = body?.value;

    if (!field || !(field in FIELD_COLUMN_MAP)) {
      return NextResponse.json(
        { ok: false, error: "更新対象の項目が不正です" },
        { status: 400 }
      );
    }

    const column = FIELD_COLUMN_MAP[field];
    const normalizedValue =
      value == null || String(value).trim() === "" ? null : String(value).trim();

    const sql = `
      UPDATE public.master_data
      SET ${column} = $1
      WHERE id = $2
      RETURNING id
    `;

    const result = await pool.query(sql, [normalizedValue, rowId]);

    if (result.rowCount === 0) {
      return NextResponse.json(
        { ok: false, error: "対象データが見つかりません" },
        { status: 404 }
      );
    }

    return NextResponse.json({
      ok: true,
      message: "更新しました",
      id: rowId,
      field,
      value: normalizedValue,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: error instanceof Error ? error.message : "更新でエラーが発生しました",
      },
      { status: 500 }
    );
  }
}