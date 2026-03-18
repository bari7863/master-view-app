import { pool } from '@/lib/db'

type CompanyRow = {
  company_name: string | null
  zip_code: string | null
  address: string | null
  industry_major: string | null
  industry_minor: string | null
  company_name_kana: string | null
  company_summary: string | null
  company_site_url: string | null
  contact_form_url: string | null
  phone: string | null
  fax: string | null
  email: string | null
  established_at: string | null
  representative_name: string | null
  representative_title: string | null
  capital: string | null
  employee_count: string | null
  employee_count_year: string | null
  sales_previous: string | null
  sales_latest: string | null
  fiscal_month: string | null
  office_count: string | null
  new_tag: string | null
  delete_tag: string | null
  delete_flag: string | null
  force_flag: string | null
}

async function getCompanies(): Promise<CompanyRow[]> {
  const result = await pool.query<CompanyRow>(`
    SELECT
      "企業名" AS company_name,
      "郵便番号" AS zip_code,
      "住所" AS address,
      "大業種名" AS industry_major,
      "小業種名" AS industry_minor,
      "企業名（かな）" AS company_name_kana,
      "企業概要" AS company_summary,
      "企業サイトURL" AS company_site_url,
      "問い合わせフォームURL" AS contact_form_url,
      "電話番号" AS phone,
      "FAX番号" AS fax,
      "メールアドレス" AS email,
      "設立年月" AS established_at,
      "代表者名" AS representative_name,
      "代表者役職" AS representative_title,
      "資本金" AS capital,
      "従業員数" AS employee_count,
      "従業員数年度" AS employee_count_year,
      "前年売上高" AS sales_previous,
      "直近売上高" AS sales_latest,
      "決算月" AS fiscal_month,
      "事業所数" AS office_count,
      "新規登録タグ" AS new_tag,
      "削除タグ" AS delete_tag,
      "削除フラグ" AS delete_flag,
      "強制フラグ" AS force_flag
    FROM public.master-data
    ORDER BY "企業名" ASC, "住所" ASC
    LIMIT 200
  `)

  return result.rows
}

async function getTotalCount(): Promise<number> {
  const result = await pool.query<{ count: string }>(`
    SELECT COUNT(*)::text AS count
    FROM public.master-data
  `)

  return Number(result.rows[0]?.count ?? 0)
}

function EmptyValue({ value }: { value: string | null }) {
  if (!value || value.trim() === '') {
    return <span className="text-gray-500">-</span>
  }
  return <span>{value}</span>
}

export default async function CompaniesPage() {
  const [companies, totalCount] = await Promise.all([
    getCompanies(),
    getTotalCount(),
  ])

  return (
    <main className="min-h-screen bg-[#0a0f1c] text-gray-100">
      <div className="mx-auto max-w-[1800px] px-6 py-8">
        <div className="mb-6 rounded-2xl border border-white/10 bg-[#11182a] p-6 shadow-2xl">
          <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
            <div>
              <p className="mb-2 text-sm text-cyan-400">PostgreSQL / マイナビ新卒(2026)</p>
              <h1 className="text-3xl font-bold tracking-tight text-white">企業一覧</h1>
              <p className="mt-2 text-sm text-gray-400">
                CSVから取り込んだデータを表示しています
              </p>
            </div>

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div className="rounded-xl border border-white/10 bg-[#0d1424] px-4 py-3">
                <div className="text-xs text-gray-400">総件数</div>
                <div className="mt-1 text-2xl font-bold text-white">
                  {totalCount.toLocaleString()}
                </div>
              </div>
              <div className="rounded-xl border border-white/10 bg-[#0d1424] px-4 py-3">
                <div className="text-xs text-gray-400">表示件数</div>
                <div className="mt-1 text-2xl font-bold text-white">
                  {companies.length.toLocaleString()}
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="overflow-hidden rounded-2xl border border-white/10 bg-[#11182a] shadow-2xl">
          <div className="border-b border-white/10 bg-[#162033] px-4 py-3 text-sm text-gray-300">
            横スクロールで全項目を確認できます
          </div>

          <div className="overflow-x-auto">
            <table className="min-w-[2800px] text-sm">
              <thead className="bg-zinc-800 text-zinc-200">
                <tr>
                  <th className="px-4 py-3 text-left">企業名</th>
                  <th className="px-4 py-3 text-left">郵便番号</th>
                  <th className="px-4 py-3 text-left">住所</th>
                  <th className="px-4 py-3 text-left">大業種名</th>
                  <th className="px-4 py-3 text-left">小業種名</th>
                  <th className="px-4 py-3 text-left">企業名（かな）</th>
                  <th className="px-4 py-3 text-left">企業概要</th>
                  <th className="px-4 py-3 text-left">企業サイトURL</th>
                  <th className="px-4 py-3 text-left">問い合わせフォームURL</th>
                  <th className="px-4 py-3 text-left">電話番号</th>
                  <th className="px-4 py-3 text-left">FAX番号</th>
                  <th className="px-4 py-3 text-left">メールアドレス</th>
                  <th className="px-4 py-3 text-left">設立年月</th>
                  <th className="px-4 py-3 text-left">代表者名</th>
                  <th className="px-4 py-3 text-left">代表者役職</th>
                  <th className="px-4 py-3 text-left">資本金</th>
                  <th className="px-4 py-3 text-left">従業員数</th>
                  <th className="px-4 py-3 text-left">従業員数年度</th>
                  <th className="px-4 py-3 text-left">前年売上高</th>
                  <th className="px-4 py-3 text-left">直近売上高</th>
                  <th className="px-4 py-3 text-left">決算月</th>
                  <th className="px-4 py-3 text-left">事業所数</th>
                  <th className="px-4 py-3 text-left">新規登録タグ</th>
                  <th className="px-4 py-3 text-left">削除タグ</th>
                  <th className="px-4 py-3 text-left">削除フラグ</th>
                  <th className="px-4 py-3 text-left">強制フラグ</th>
                </tr>
              </thead>

              <tbody>
                {companies.length === 0 ? (
                  <tr>
                    <td colSpan={27} className="px-4 py-10 text-center text-gray-400">
                      データがありません
                    </td>
                  </tr>
                ) : (
                  companies.map((row, index) => (
                    <tr
                      key={`${row.company_name}-${row.address}-${index}`}
                      className={
                        index % 2 === 0
                          ? 'border-t border-white/10 bg-[#0f1728] hover:bg-[#162033]'
                          : 'border-t border-white/10 bg-[#111b2e] hover:bg-[#162033]'
                      }
                    >
                      <td className="px-4 py-3 font-medium text-white">
                        <EmptyValue value={row.company_name} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.zip_code} />
                      </td>
                      <td className="px-4 py-3 text-gray-300 min-w-[260px]">
                        <EmptyValue value={row.address} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.industry_major} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.industry_minor} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.company_name_kana} />
                      </td>
                      <td className="px-4 py-3 text-gray-300 min-w-[320px]">
                        <EmptyValue value={row.company_summary} />
                      </td>
                      <td className="px-4 py-3">
                        {row.company_site_url ? (
                          <a
                            href={row.company_site_url}
                            target="_blank"
                            rel="noreferrer"
                            className="inline-flex rounded-lg bg-cyan-500/15 px-3 py-1 text-cyan-300 hover:bg-cyan-500/25"
                          >
                            開く
                          </a>
                        ) : (
                          <span className="text-gray-500">-</span>
                        )}
                      </td>
                      <td className="px-4 py-3">
                        {row.contact_form_url ? (
                          <a
                            href={row.contact_form_url}
                            target="_blank"
                            rel="noreferrer"
                            className="inline-flex rounded-lg bg-cyan-500/15 px-3 py-1 text-cyan-300 hover:bg-cyan-500/25"
                          >
                            開く
                          </a>
                        ) : (
                          <span className="text-gray-500">-</span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.phone} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.fax} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.email} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.established_at} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.representative_name} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.representative_title} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.capital} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.employee_count} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.employee_count_year} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.sales_previous} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.sales_latest} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.fiscal_month} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.office_count} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.new_tag} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.delete_tag} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.delete_flag} />
                      </td>
                      <td className="px-4 py-3 text-gray-300">
                        <EmptyValue value={row.force_flag} />
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </main>
  )
}