"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { List, type RowComponentProps } from "react-window";
import { createPortal } from "react-dom";

type Row = {
  company: string | null;
  zipcode: string | null;
  address: string | null;
  big_industry: string | null;
  small_industry: string | null;
  company_kana: string | null;
  summary: string | null;
  website_url: string | null;
  form_url: string | null;
  phone: string | null;
  fax: string | null;
  email: string | null;
  established_date: string | null;
  representative_name: string | null;
  representative_title: string | null;
  capital: string | null;
  employee_count: string | null;
  employee_count_year: string | null;
  previous_sales: string | null;
  latest_sales: string | null;
  closing_month: string | null;
  office_count: string | null;
  new_tag: string | null;
  delete_tag: string | null;
  delete_flag: string | null;
  force_flag: string | null;
};

type FilterKey = keyof Row;
type SortDirection = "" | "asc" | "desc";
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

type ColumnFilterState = {
  sortDirection: SortDirection;
  conditionType: ConditionType;
  conditionValue: string;
  valueFilterEnabled: boolean;
  selectedValues: string[];
  availableValues: string[];
  valueSearch: string;
};

type ApiResponse = {
  ok: boolean;
  total?: number;
  page?: number;
  limit?: number | "all";
  totalPages?: number;
  rows?: Row[];
  values?: string[];
  error?: string;
  inserted?: number;
  message?: string;
};

async function readApiResponse(res: Response): Promise<ApiResponse> {
  const text = await res.text();

  if (!text) {
    return {
      ok: false,
      error: "APIレスポンスが空です",
    };
  }

  try {
    return JSON.parse(text) as ApiResponse;
  } catch {
    throw new Error("APIの返却形式がJSONではありません");
  }
}

const GRID_TEMPLATE = `
  minmax(220px,2fr)
  minmax(110px,0.8fr)
  minmax(320px,2.4fr)
  minmax(150px,1.1fr)
  minmax(150px,1.1fr)
  minmax(180px,1.2fr)
  minmax(320px,2.2fr)
  minmax(240px,1.6fr)
  minmax(260px,1.8fr)
  minmax(140px,1fr)
  minmax(140px,1fr)
  minmax(220px,1.5fr)
  minmax(140px,1fr)
  minmax(160px,1.1fr)
  minmax(160px,1.1fr)
  minmax(140px,1fr)
  minmax(140px,1fr)
  minmax(140px,1fr)
  minmax(160px,1.1fr)
  minmax(160px,1.1fr)
  minmax(120px,0.9fr)
  minmax(120px,0.9fr)
  minmax(140px,1fr)
  minmax(120px,0.9fr)
  minmax(120px,0.9fr)
  minmax(120px,0.9fr)
`;

const pageSizeOptions = [
  { value: "50", label: "50件" },
  { value: "100", label: "100件" },
  { value: "200", label: "200件" },
  { value: "500", label: "500件" },
  { value: "1000", label: "1000件" },
];

const CONDITION_OPTIONS: { value: ConditionType; label: string }[] = [
  { value: "", label: "なし" },
  { value: "contains", label: "含む" },
  { value: "not_contains", label: "含まない" },
  { value: "equals", label: "完全一致" },
  { value: "not_equals", label: "完全一致しない" },
  { value: "starts_with", label: "で始まる" },
  { value: "ends_with", label: "で終わる" },
  { value: "is_empty", label: "空白" },
  { value: "is_not_empty", label: "空白ではない" },
];

const COLUMN_DEFS: { key: FilterKey; label: string }[] = [
  { key: "company", label: "企業名" },
  { key: "zipcode", label: "郵便番号" },
  { key: "address", label: "住所" },
  { key: "big_industry", label: "大業種名" },
  { key: "small_industry", label: "小業種名" },
  { key: "company_kana", label: "企業名（かな）" },
  { key: "summary", label: "企業概要" },
  { key: "website_url", label: "企業サイトURL" },
  { key: "form_url", label: "問い合わせフォームURL" },
  { key: "phone", label: "電話番号" },
  { key: "fax", label: "FAX番号" },
  { key: "email", label: "メールアドレス" },
  { key: "established_date", label: "設立年月" },
  { key: "representative_name", label: "代表者名" },
  { key: "representative_title", label: "代表者役職" },
  { key: "capital", label: "資本金" },
  { key: "employee_count", label: "従業員数" },
  { key: "employee_count_year", label: "従業員数年度" },
  { key: "previous_sales", label: "前年売上高" },
  { key: "latest_sales", label: "直近売上高" },
  { key: "closing_month", label: "決算月" },
  { key: "office_count", label: "事業所数" },
  { key: "new_tag", label: "新規登録タグ" },
  { key: "delete_tag", label: "削除タグ" },
  { key: "delete_flag", label: "削除フラグ" },
  { key: "force_flag", label: "強制フラグ" },
];

function createEmptyColumnState(): ColumnFilterState {
  return {
    sortDirection: "",
    conditionType: "",
    conditionValue: "",
    valueFilterEnabled: false,
    selectedValues: [],
    availableValues: [],
    valueSearch: "",
  };
}

function createInitialColumnStates(): Record<FilterKey, ColumnFilterState> {
  return COLUMN_DEFS.reduce((acc, column) => {
    acc[column.key] = createEmptyColumnState();
    return acc;
  }, {} as Record<FilterKey, ColumnFilterState>);
}

function cloneColumnStates(
  states: Record<FilterKey, ColumnFilterState>
): Record<FilterKey, ColumnFilterState> {
  return COLUMN_DEFS.reduce((acc, column) => {
    acc[column.key] = {
      ...states[column.key],
      selectedValues: [...states[column.key].selectedValues],
      availableValues: [...states[column.key].availableValues],
    };
    return acc;
  }, {} as Record<FilterKey, ColumnFilterState>);
}

function buildRequestFilterModels(
  states: Record<FilterKey, ColumnFilterState>
) {
  return COLUMN_DEFS.reduce((acc, column) => {
    const state = states[column.key];
    const model: Record<string, unknown> = {};

    if (state.sortDirection !== "") {
      model.sortDirection = state.sortDirection;
    }

    if (state.conditionType !== "") {
      model.conditionType = state.conditionType;
    }

    if (
      state.conditionType !== "" &&
      state.conditionType !== "is_empty" &&
      state.conditionType !== "is_not_empty" &&
      state.conditionValue.trim() !== ""
    ) {
      model.conditionValue = state.conditionValue;
    }

    if (state.valueFilterEnabled) {
      model.valueFilterEnabled = true;
      model.selectedValues = state.selectedValues;
    }

    acc[column.key] = model;
    return acc;
  }, {} as Record<string, unknown>);
}

function hasActiveFilter(state: ColumnFilterState) {
  return (
    state.sortDirection !== "" ||
    state.conditionType !== "" ||
    state.valueFilterEnabled
  );
}

function Cell({
  children,
  className = "",
  title,
  onDoubleClick,
}: {
  children: React.ReactNode;
  className?: string;
  title?: string;
  onDoubleClick?: () => void;
}) {
  return (
    <div
      title={title}
      onDoubleClick={onDoubleClick}
      className={`px-4 py-3 text-sm text-slate-100 truncate ${className}`}
    >
      {children}
    </div>
  );
}

function EmptyValue({ value }: { value: string | null }) {
  if (!value || value.trim() === "") {
    return <span className="text-slate-500">-</span>;
  }
  return <span>{value}</span>;
}

function LinkCell({ url }: { url: string | null }) {
  if (!url || url.trim() === "") {
    return <span className="text-slate-500">-</span>;
  }

  return (
    <a
      href={url}
      target="_blank"
      rel="noreferrer"
      className="block truncate text-sky-300 underline underline-offset-2 transition hover:text-sky-200"
      title={url}
    >
      {url}
    </a>
  );
}

function HeaderCell({
  label,
  filterKey,
  filterState,
  isOpen,
  onToggleOpen,
  onSortChange,
  onConditionTypeChange,
  onConditionValueChange,
  onValueSearchChange,
  onToggleValue,
  onSelectAllVisible,
  onClearVisible,
  onApply,
  onClear,
}: {
  label: string;
  filterKey: FilterKey;
  filterState: ColumnFilterState;
  isOpen: boolean;
  onToggleOpen: (key: FilterKey) => void;
  onSortChange: (key: FilterKey, direction: SortDirection) => void;
  onConditionTypeChange: (key: FilterKey, value: ConditionType) => void;
  onConditionValueChange: (key: FilterKey, value: string) => void;
  onValueSearchChange: (key: FilterKey, value: string) => void;
  onToggleValue: (key: FilterKey, value: string) => void;
  onSelectAllVisible: (key: FilterKey) => void;
  onClearVisible: (key: FilterKey) => void;
  onApply: (key: FilterKey) => void;
  onClear: (key: FilterKey) => void;
}) {
  const active = hasActiveFilter(filterState);
  const visibleValues = filterState.availableValues.filter((value) => {
    if (!filterState.valueSearch.trim()) return true;
    const labelValue = value === "" ? "(空白)" : value;
    return labelValue
      .toLowerCase()
      .includes(filterState.valueSearch.trim().toLowerCase());
  });

  return (
    <div className="relative border-r border-white/5 last:border-r-0">
      <div
        className={`grid grid-cols-[minmax(0,1fr)_28px] items-center gap-2 px-4 py-3 text-sm font-semibold ${
          active ? "bg-sky-500/10 text-sky-100" : "text-slate-100"
        }`}
      >
        <span className="block min-w-0 truncate text-center">
          {label}
        </span>

        <button
          type="button"
          onClick={() => onToggleOpen(filterKey)}
          className={`inline-flex h-7 w-7 items-center justify-center justify-self-end rounded-lg border text-[11px] transition ${
            active
              ? "border-sky-400/40 bg-sky-500/20 text-sky-100 hover:bg-sky-500/30"
              : "border-white/10 bg-white/5 text-slate-300 hover:bg-white/10"
          }`}
        >
          ▼
        </button>
      </div>

      {isOpen &&
        typeof document !== "undefined" &&
        createPortal(
          <div
            className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
            onClick={() => onToggleOpen(filterKey)}
          >
            <div className="flex min-h-full items-center justify-center">
              <div
                className="flex w-full max-w-[420px] max-h-[calc(100dvh-32px)] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                onClick={(e) => e.stopPropagation()}
              >
                <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                  <div className="text-sm font-semibold text-slate-100">
                    {label} のフィルタ
                  </div>

                  <button
                    type="button"
                    onClick={() => onToggleOpen(filterKey)}
                    className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                  >
                    ×
                  </button>
                </div>

                <div className="flex-1 overflow-y-auto px-4 py-4">
                  <div className="mb-4">
                    <div className="mb-2 text-xs font-semibold text-slate-300">
                      並び替え
                    </div>
                    <div className="grid grid-cols-3 gap-2">
                      <button
                        type="button"
                        onClick={() => onSortChange(filterKey, "")}
                        className={`h-9 rounded-xl border text-xs transition ${
                          filterState.sortDirection === ""
                            ? "border-sky-400/40 bg-sky-500/20 text-sky-100"
                            : "border-white/10 bg-white/5 text-slate-200 hover:bg-white/10"
                        }`}
                      >
                        なし
                      </button>
                      <button
                        type="button"
                        onClick={() => onSortChange(filterKey, "asc")}
                        className={`h-9 rounded-xl border text-xs transition ${
                          filterState.sortDirection === "asc"
                            ? "border-sky-400/40 bg-sky-500/20 text-sky-100"
                            : "border-white/10 bg-white/5 text-slate-200 hover:bg-white/10"
                        }`}
                      >
                        昇順
                      </button>
                      <button
                        type="button"
                        onClick={() => onSortChange(filterKey, "desc")}
                        className={`h-9 rounded-xl border text-xs transition ${
                          filterState.sortDirection === "desc"
                            ? "border-sky-400/40 bg-sky-500/20 text-sky-100"
                            : "border-white/10 bg-white/5 text-slate-200 hover:bg-white/10"
                        }`}
                      >
                        降順
                      </button>
                    </div>
                  </div>

                  <div className="mb-4">
                    <div className="mb-2 text-xs font-semibold text-slate-300">
                      条件でフィルタ
                    </div>

                    <select
                      value={filterState.conditionType}
                      onChange={(e) =>
                        onConditionTypeChange(
                          filterKey,
                          e.target.value as ConditionType
                        )
                      }
                      className="mb-2 h-10 w-full rounded-xl border border-white/10 bg-[#0f172a] px-3 text-sm text-slate-100 outline-none focus:border-sky-500"
                    >
                      {CONDITION_OPTIONS.map((option) => (
                        <option key={option.value || "none"} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>

                    {filterState.conditionType !== "is_empty" &&
                      filterState.conditionType !== "is_not_empty" &&
                      filterState.conditionType !== "" && (
                        <input
                          value={filterState.conditionValue}
                          onChange={(e) =>
                            onConditionValueChange(filterKey, e.target.value)
                          }
                          placeholder={label}
                          className="h-10 w-full rounded-xl border border-white/10 bg-[#0f172a] px-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-sky-500"
                        />
                      )}
                  </div>

                  <div>
                    <div className="mb-2 text-xs font-semibold text-slate-300">
                      値でフィルタ
                    </div>

                    <input
                      value={filterState.valueSearch}
                      onChange={(e) => onValueSearchChange(filterKey, e.target.value)}
                      placeholder="値を検索"
                      className="mb-2 h-10 w-full rounded-xl border border-white/10 bg-[#0f172a] px-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-sky-500"
                    />

                    <div className="mb-2 flex gap-2">
                      <button
                        type="button"
                        onClick={() => onSelectAllVisible(filterKey)}
                        className="h-8 flex-1 rounded-lg border border-white/10 bg-white/5 px-2 text-xs text-slate-200 transition hover:bg-white/10"
                      >
                        すべて選択
                      </button>
                      <button
                        type="button"
                        onClick={() => onClearVisible(filterKey)}
                        className="h-8 flex-1 rounded-lg border border-white/10 bg-white/5 px-2 text-xs text-slate-200 transition hover:bg-white/10"
                      >
                        すべて解除
                      </button>
                    </div>

                    <div className="max-h-[min(40dvh,360px)] overflow-y-auto rounded-xl border border-white/10 bg-[#0f172a] p-2">
                      {visibleValues.length === 0 ? (
                        <div className="px-2 py-6 text-center text-xs text-slate-500">
                          候補がありません
                        </div>
                      ) : (
                        visibleValues.map((value) => {
                          const checked =
                            !filterState.valueFilterEnabled ||
                            filterState.selectedValues.includes(value);
                          return (
                            <label
                              key={`${filterKey}-${value || "__empty__"}`}
                              className="flex cursor-pointer items-center gap-2 rounded-lg px-2 py-2 text-sm text-slate-100 hover:bg-white/5"
                            >
                              <input
                                type="checkbox"
                                checked={checked}
                                onChange={() => onToggleValue(filterKey, value)}
                                className="h-4 w-4 accent-sky-500"
                              />
                              <span className="truncate">
                                {value === "" ? "(空白)" : value}
                              </span>
                            </label>
                          );
                        })
                      )}
                    </div>
                  </div>
                </div>

                <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                  <button
                    type="button"
                    onClick={() => onApply(filterKey)}
                    className="h-10 flex-1 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400"
                  >
                    適用
                  </button>

                  <button
                    type="button"
                    onClick={() => onClear(filterKey)}
                    className="h-10 flex-1 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10"
                  >
                    クリア
                  </button>
                </div>
              </div>
            </div>
          </div>,
          document.body
        )}
    </div>
  );
}

function VirtualRow({
  index,
  style,
  rows,
}: RowComponentProps<{ rows: Row[] }>) {
  const row = rows[index];

  return (
    <div style={{ ...style, width: "100%" }}>
      <div
        className="grid border-b border-white/5 bg-[#0f172a]/60 transition hover:bg-[#162033]"
        style={{ gridTemplateColumns: GRID_TEMPLATE }}
      >
        <Cell title={row.company || ""}><EmptyValue value={row.company} /></Cell>
        <Cell title={row.zipcode || ""}><EmptyValue value={row.zipcode} /></Cell>
        <Cell title={row.address || ""}><EmptyValue value={row.address} /></Cell>
        <Cell title={row.big_industry || ""}><EmptyValue value={row.big_industry} /></Cell>
        <Cell title={row.small_industry || ""}><EmptyValue value={row.small_industry} /></Cell>
        <Cell title={row.company_kana || ""}><EmptyValue value={row.company_kana} /></Cell>
        <Cell title={row.summary || ""} className="whitespace-pre-wrap"><EmptyValue value={row.summary} /></Cell>
        <Cell title={row.website_url || ""}><LinkCell url={row.website_url} /></Cell>
        <Cell title={row.form_url || ""}><LinkCell url={row.form_url} /></Cell>
        <Cell title={row.phone || ""}><EmptyValue value={row.phone} /></Cell>
        <Cell title={row.fax || ""}><EmptyValue value={row.fax} /></Cell>
        <Cell title={row.email || ""}><EmptyValue value={row.email} /></Cell>
        <Cell title={row.established_date || ""}><EmptyValue value={row.established_date} /></Cell>
        <Cell title={row.representative_name || ""}><EmptyValue value={row.representative_name} /></Cell>
        <Cell title={row.representative_title || ""}><EmptyValue value={row.representative_title} /></Cell>
        <Cell title={row.capital || ""}><EmptyValue value={row.capital} /></Cell>
        <Cell title={row.employee_count || ""}><EmptyValue value={row.employee_count} /></Cell>
        <Cell title={row.employee_count_year || ""}><EmptyValue value={row.employee_count_year} /></Cell>
        <Cell title={row.previous_sales || ""}><EmptyValue value={row.previous_sales} /></Cell>
        <Cell title={row.latest_sales || ""}><EmptyValue value={row.latest_sales} /></Cell>
        <Cell title={row.closing_month || ""}><EmptyValue value={row.closing_month} /></Cell>
        <Cell title={row.office_count || ""}><EmptyValue value={row.office_count} /></Cell>
        <Cell title={row.new_tag || ""}><EmptyValue value={row.new_tag} /></Cell>
        <Cell title={row.delete_tag || ""}><EmptyValue value={row.delete_tag} /></Cell>
        <Cell title={row.delete_flag || ""}><EmptyValue value={row.delete_flag} /></Cell>
        <Cell title={row.force_flag || ""}><EmptyValue value={row.force_flag} /></Cell>
      </div>
    </div>
  );
}

export default function Home() {
  const [draftColumnStates, setDraftColumnStates] =
    useState<Record<FilterKey, ColumnFilterState>>(() =>
      createInitialColumnStates()
    );
  const [appliedColumnStates, setAppliedColumnStates] =
    useState<Record<FilterKey, ColumnFilterState>>(() =>
      createInitialColumnStates()
    );

  const [page, setPage] = useState(1);
  const [limit, setLimit] = useState("200");
  const [rows, setRows] = useState<Row[]>([]);
  const [total, setTotal] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [openFilterKey, setOpenFilterKey] = useState<FilterKey | null>(null);

  const topPanelRef = useRef<HTMLDivElement | null>(null);
  const [headerStickyTop, setHeaderStickyTop] = useState(0);

  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [importing, setImporting] = useState(false);
  const [importMessage, setImportMessage] = useState("");
  const [importError, setImportError] = useState("");

  const fetchData = async () => {
    setLoading(true);
    setError("");

    try {
      const params = new URLSearchParams();
      params.set("page", String(page));
      params.set("limit", limit);
      params.set(
        "filterModels",
        JSON.stringify(buildRequestFilterModels(appliedColumnStates))
      );

      const sortColumn = COLUMN_DEFS.find(
        (column) => appliedColumnStates[column.key].sortDirection !== ""
      );

      if (sortColumn) {
        params.set("sortKey", sortColumn.key);
        params.set(
          "sortDirection",
          appliedColumnStates[sortColumn.key].sortDirection
        );
      }

      const res = await fetch(`/api/master_data?${params.toString()}`, {
        cache: "no-store",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "データ取得に失敗しました");
      }

      setRows(data.rows || []);
      setTotal(data.total || 0);
      setTotalPages(data.totalPages || 1);
    } catch (e) {
      setRows([]);
      setTotal(0);
      setTotalPages(1);
      setError(e instanceof Error ? e.message : "不明なエラー");
    } finally {
      setLoading(false);
    }
  };

  const fetchFilterValues = async (key: FilterKey) => {
    try {
      const params = new URLSearchParams();
      params.set("valuesFor", key);
      params.set(
        "filterModels",
        JSON.stringify(buildRequestFilterModels(appliedColumnStates))
      );

      const res = await fetch(`/api/master_data?${params.toString()}`, {
        cache: "no-store",
      });

      const data = await readApiResponse(res);
      if (!res.ok || !data.ok) {
        throw new Error(data.error || "値一覧の取得に失敗しました");
      }

      setDraftColumnStates((prev) => {
        const next = cloneColumnStates(prev);
        next[key].availableValues = data.values || [];
        if (!next[key].valueFilterEnabled) {
          next[key].selectedValues = [];
        } else {
          next[key].selectedValues = next[key].selectedValues.filter((value) =>
            (data.values || []).includes(value)
          );
        }
        return next;
      });
    } catch (e) {
      console.error(e);
    }
  };

  useEffect(() => {
    fetchData();
  }, [page, limit, appliedColumnStates]);

  useEffect(() => {
    setHeaderStickyTop(0);
  }, []);

  const handleOpenFilter = async (key: FilterKey) => {
    if (openFilterKey === key) {
      setOpenFilterKey(null);
      return;
    }

    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);
      next[key] = {
        ...appliedColumnStates[key],
        availableValues:
          prev[key].availableValues.length > 0
            ? [...prev[key].availableValues]
            : [...appliedColumnStates[key].availableValues],
        valueSearch: "",
      };
      return next;
    });

    setOpenFilterKey(key);
    await fetchFilterValues(key);
  };

  const updateSort = (key: FilterKey, direction: SortDirection) => {
    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);
      COLUMN_DEFS.forEach((column) => {
        next[column.key].sortDirection = column.key === key ? direction : "";
      });
      return next;
    });
  };

  const updateConditionType = (key: FilterKey, value: ConditionType) => {
    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);
      next[key].conditionType = value;
      if (value === "" || value === "is_empty" || value === "is_not_empty") {
        next[key].conditionValue = "";
      }
      return next;
    });
  };

  const updateConditionValue = (key: FilterKey, value: string) => {
    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);
      next[key].conditionValue = value;
      return next;
    });
  };

  const updateValueSearch = (key: FilterKey, value: string) => {
    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);
      next[key].valueSearch = value;
      return next;
    });
  };

  const toggleSelectedValue = (key: FilterKey, value: string) => {
    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);
      const exists = next[key].selectedValues.includes(value);

      next[key].valueFilterEnabled = true;
      next[key].selectedValues = exists
        ? next[key].selectedValues.filter((item) => item !== value)
        : [...next[key].selectedValues, value];

      return next;
    });
  };

  const selectAllVisibleValues = (key: FilterKey) => {
    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);
      const visibleValues = next[key].availableValues.filter((value) => {
        if (!next[key].valueSearch.trim()) return true;
        const labelValue = value === "" ? "(空白)" : value;
        return labelValue
          .toLowerCase()
          .includes(next[key].valueSearch.trim().toLowerCase());
      });

      const merged = new Set([...next[key].selectedValues, ...visibleValues]);
      next[key].valueFilterEnabled = true;
      next[key].selectedValues = Array.from(merged);
      return next;
    });
  };

  const clearVisibleValues = (key: FilterKey) => {
    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);
      const visibleValues = next[key].availableValues.filter((value) => {
        if (!next[key].valueSearch.trim()) return true;
        const labelValue = value === "" ? "(空白)" : value;
        return labelValue
          .toLowerCase()
          .includes(next[key].valueSearch.trim().toLowerCase());
      });

      next[key].valueFilterEnabled = true;
      next[key].selectedValues = next[key].selectedValues.filter(
        (value) => !visibleValues.includes(value)
      );
      return next;
    });
  };

  const applyColumnFilter = (key: FilterKey) => {
    setAppliedColumnStates((prev) => {
      const next = cloneColumnStates(prev);
      next[key] = {
        ...draftColumnStates[key],
        selectedValues: draftColumnStates[key].valueFilterEnabled
          ? [...draftColumnStates[key].selectedValues]
          : [],
        availableValues: [...draftColumnStates[key].availableValues],
      };

      if (draftColumnStates[key].sortDirection !== "") {
        COLUMN_DEFS.forEach((column) => {
          if (column.key !== key) {
            next[column.key].sortDirection = "";
          }
        });
      }

      return next;
    });

    setPage(1);
    setOpenFilterKey(null);
  };

  const clearColumnFilter = (key: FilterKey) => {
    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);
      next[key] = {
        ...createEmptyColumnState(),
        availableValues: [...prev[key].availableValues],
      };
      return next;
    });

    setAppliedColumnStates((prev) => {
      const next = cloneColumnStates(prev);
      next[key] = {
        ...createEmptyColumnState(),
        availableValues: [...prev[key].availableValues],
      };
      return next;
    });

    setPage(1);
    setOpenFilterKey(null);
  };

  const handleImport = async () => {
    if (!selectedFile) {
      setImportError("CSVファイルを選択してください");
      setImportMessage("");
      return;
    }

    setImporting(true);
    setImportError("");
    setImportMessage("");

    try {
      const formData = new FormData();
      formData.append("file", selectedFile);

      const res = await fetch("/api/master_data", {
        method: "POST",
        body: formData,
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "CSV取込に失敗しました");
      }

      setImportMessage(data.message || "CSVを取り込みました");
      setSelectedFile(null);
      setPage(1);
      await fetchData();
    } catch (e) {
      setImportError(
        e instanceof Error ? e.message : "CSV取込でエラーが発生しました"
      );
    } finally {
      setImporting(false);
    }
  };

  const handleExport = async () => {
  setImportError("");
  setImportMessage("");

  try {
    const params = new URLSearchParams();
    params.set(
      "filterModels",
      JSON.stringify(buildRequestFilterModels(appliedColumnStates))
    );

    const sortColumn = COLUMN_DEFS.find(
      (column) => appliedColumnStates[column.key].sortDirection !== ""
    );

    if (sortColumn) {
      params.set("sortKey", sortColumn.key);
      params.set(
        "sortDirection",
        appliedColumnStates[sortColumn.key].sortDirection
      );
    }

    const res = await fetch(`/api/master_data/export?${params.toString()}`, {
      cache: "no-store",
    });

    if (!res.ok) {
      const data = await readApiResponse(res);
      throw new Error(data.error || "CSVエクスポートに失敗しました");
    }

    const blob = await res.blob();
    const url = window.URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;

    const disposition = res.headers.get("content-disposition") || "";
    const utf8Match = disposition.match(/filename\*=UTF-8''([^;]+)/);
    const normalMatch = disposition.match(/filename="([^"]+)"/);

    const filename = decodeURIComponent(
      utf8Match?.[1] || normalMatch?.[1] || "master_data.csv"
    );

    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(url);
  } catch (e) {
    setImportError(
      e instanceof Error ? e.message : "CSVエクスポートに失敗しました"
    );
  }
};

  const pageNumbers = useMemo(() => {
    if (limit === "all") return [1];

    const maxButtons = 7;
    let start = Math.max(page - 3, 1);
    let end = Math.min(start + maxButtons - 1, totalPages);

    if (end - start < maxButtons - 1) {
      start = Math.max(end - maxButtons + 1, 1);
    }

    return Array.from({ length: end - start + 1 }, (_, i) => start + i);
  }, [page, totalPages, limit]);

  const usingVirtual = false;

  return (
    <main className="min-h-screen bg-transparent text-slate-100">
      <div className="mx-auto max-w-[1880px] px-6 py-6">
        <div
          ref={topPanelRef}
          className="sticky top-0 z-30 mb-6 rounded-[28px] border border-white/10 bg-[#08101d]/80 p-2 shadow-[0_24px_60px_rgba(0,0,0,0.35)] backdrop-blur-2xl"
        >
          <div className="rounded-[24px] border border-white/10 bg-[#0b1326]/85 p-6">
            <div className="mb-6 flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
              <div>
                <h1 className="text-3xl font-bold tracking-tight text-white">
                  マスタデータ
                </h1>
              </div>

              <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                <div className="flex min-h-[96px] flex-col items-center justify-center rounded-2xl border border-white/10 bg-white/5 px-4 py-4 text-center">
                  <div className="text-xs text-slate-400">総件数</div>
                  <div className="mt-2 text-xl font-semibold text-white">
                    {total.toLocaleString()}件
                  </div>
                </div>

                <div className="flex min-h-[96px] flex-col items-center justify-center rounded-2xl border border-white/10 bg-white/5 px-4 py-4 text-center">
                  <div className="text-xs text-slate-400">現在ページ</div>
                  <div className="mt-2 text-xl font-semibold text-white">
                    {page}
                  </div>
                </div>

                <div className="flex min-h-[96px] flex-col items-center justify-center rounded-2xl border border-white/10 bg-white/5 px-4 py-4 text-center">
                  <div className="text-xs text-slate-400">総ページ数</div>
                  <div className="mt-2 text-xl font-semibold text-white">
                    {totalPages}
                  </div>
                </div>

                <div className="flex min-h-[96px] flex-col items-center justify-center rounded-2xl border border-white/10 bg-white/5 px-4 py-4 text-center">
                  <div className="text-xs text-slate-400">表示件数</div>
                  <div className="mt-2">
                    <select
                      value={limit}
                      onChange={(e) => {
                        setLimit(e.target.value);
                        setPage(1);
                      }}
                      className="h-10 w-full rounded-xl border border-white/10 bg-[#0f172a] px-3 text-center text-sm outline-none focus:border-sky-500"
                    >
                      {pageSizeOptions.map((opt) => (
                        <option key={opt.value} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
              </div>
            </div>

            <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
              <div className="mb-3 text-sm font-semibold text-slate-200">
                CSV取込
              </div>

              <div className="flex flex-col gap-3 xl:flex-row xl:items-center">
                <input
                  type="file"
                  accept=".csv,text/csv"
                  onChange={(e) => setSelectedFile(e.target.files?.[0] ?? null)}
                  className="block w-full rounded-xl border border-white/10 bg-[#0f172a] px-4 py-3 text-sm text-slate-200 file:mr-4 file:rounded-lg file:border-0 file:bg-sky-500 file:px-4 file:py-2 file:text-sm file:font-medium file:text-white hover:file:bg-sky-400"
                />

                <div className="flex shrink-0 gap-3">
                  <button
                    onClick={handleImport}
                    disabled={importing}
                    className="h-11 rounded-xl bg-emerald-500 px-5 text-sm font-medium text-white transition hover:bg-emerald-400 disabled:opacity-50"
                  >
                    {importing ? "取り込み中..." : "CSVを投入"}
                  </button>

                  <button
                    onClick={handleExport}
                    className="h-11 rounded-xl bg-sky-500 px-5 text-sm font-medium text-white transition hover:bg-sky-400"
                  >
                    CSVをエクスポート
                  </button>
                </div>
              </div>

              {importMessage && (
                <div className="mt-3 rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200">
                  {importMessage}
                </div>
              )}

              {importError && (
                <div className="mt-3 rounded-xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
                  {importError}
                </div>
              )}
            </div>
          </div>
        </div>

        {loading && (
          <div className="mb-4 rounded-2xl border border-white/10 bg-[#0b1326]/90 px-4 py-3 text-sm text-slate-400">
            読み込み中です...
          </div>
        )}

        {error && (
          <div className="mb-4 rounded-2xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
            エラー: {error}
          </div>
        )}

        <div className="rounded-[24px] border border-white/10 bg-[#0b1326]/90 shadow-[0_24px_60px_rgba(0,0,0,0.35)]">
          <div className="overflow-auto max-h-[70vh]">
            <div className="min-w-[5200px]">
              <div
                className="sticky z-20 grid border-b border-white/10 bg-[#162033]/95 backdrop-blur-xl"
                style={{
                  gridTemplateColumns: GRID_TEMPLATE,
                  top: `${headerStickyTop}px`,
                }}
              >
                {COLUMN_DEFS.map((column) => (
                  <HeaderCell
                    key={column.key}
                    label={column.label}
                    filterKey={column.key}
                    filterState={draftColumnStates[column.key]}
                    isOpen={openFilterKey === column.key}
                    onToggleOpen={handleOpenFilter}
                    onSortChange={updateSort}
                    onConditionTypeChange={updateConditionType}
                    onConditionValueChange={updateConditionValue}
                    onValueSearchChange={updateValueSearch}
                    onToggleValue={toggleSelectedValue}
                    onSelectAllVisible={selectAllVisibleValues}
                    onClearVisible={clearVisibleValues}
                    onApply={applyColumnFilter}
                    onClear={clearColumnFilter}
                  />
                ))}
              </div>

              {rows.length === 0 && !loading ? (
                <div className="px-4 py-12 text-center text-slate-500">
                  データがありません
                </div>
              ) : usingVirtual ? (
                <List
                  defaultHeight={650}
                  rowComponent={VirtualRow}
                  rowCount={rows.length}
                  rowHeight={58}
                  rowProps={{ rows }}
                  style={{ width: "100%" }}
                />
              ) : (
                <div>
                  {rows.map((row, i) => (
                    <div
                      key={`${row.company}-${row.address}-${i}`}
                      className="grid border-b border-white/5 bg-[#0f172a]/60 transition hover:bg-[#162033]"
                      style={{ gridTemplateColumns: GRID_TEMPLATE }}
                    >
                      <Cell title={row.company || ""}>
                        <EmptyValue value={row.company} />
                      </Cell>
                      <Cell title={row.zipcode || ""}><EmptyValue value={row.zipcode} /></Cell>
                      <Cell title={row.address || ""}><EmptyValue value={row.address} /></Cell>
                      <Cell title={row.big_industry || ""}><EmptyValue value={row.big_industry} /></Cell>
                      <Cell title={row.small_industry || ""}><EmptyValue value={row.small_industry} /></Cell>
                      <Cell title={row.company_kana || ""}><EmptyValue value={row.company_kana} /></Cell>
                      <Cell title={row.summary || ""} className="whitespace-pre-wrap"><EmptyValue value={row.summary} /></Cell>
                      <Cell title={row.website_url || ""}><LinkCell url={row.website_url} /></Cell>
                      <Cell title={row.form_url || ""}><LinkCell url={row.form_url} /></Cell>
                      <Cell title={row.phone || ""}><EmptyValue value={row.phone} /></Cell>
                      <Cell title={row.fax || ""}><EmptyValue value={row.fax} /></Cell>
                      <Cell title={row.email || ""}><EmptyValue value={row.email} /></Cell>
                      <Cell title={row.established_date || ""}><EmptyValue value={row.established_date} /></Cell>
                      <Cell title={row.representative_name || ""}><EmptyValue value={row.representative_name} /></Cell>
                      <Cell title={row.representative_title || ""}><EmptyValue value={row.representative_title} /></Cell>
                      <Cell title={row.capital || ""}><EmptyValue value={row.capital} /></Cell>
                      <Cell title={row.employee_count || ""}><EmptyValue value={row.employee_count} /></Cell>
                      <Cell title={row.employee_count_year || ""}><EmptyValue value={row.employee_count_year} /></Cell>
                      <Cell title={row.previous_sales || ""}><EmptyValue value={row.previous_sales} /></Cell>
                      <Cell title={row.latest_sales || ""}><EmptyValue value={row.latest_sales} /></Cell>
                      <Cell title={row.closing_month || ""}><EmptyValue value={row.closing_month} /></Cell>
                      <Cell title={row.office_count || ""}><EmptyValue value={row.office_count} /></Cell>
                      <Cell title={row.new_tag || ""}><EmptyValue value={row.new_tag} /></Cell>
                      <Cell title={row.delete_tag || ""}><EmptyValue value={row.delete_tag} /></Cell>
                      <Cell title={row.delete_flag || ""}><EmptyValue value={row.delete_flag} /></Cell>
                      <Cell title={row.force_flag || ""}><EmptyValue value={row.force_flag} /></Cell>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>

        <div className="mt-5 flex flex-wrap items-center gap-2">
          <button
            onClick={() => setPage(1)}
            disabled={page <= 1 || limit === "all"}
            className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-200 transition hover:bg-white/10 disabled:opacity-40"
          >
            最初
          </button>

          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1 || limit === "all"}
            className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-200 transition hover:bg-white/10 disabled:opacity-40"
          >
            前へ
          </button>

          {pageNumbers.map((n) => (
            <button
              key={n}
              onClick={() => setPage(n)}
              disabled={limit === "all"}
              className={`rounded-xl px-4 py-2 text-sm transition ${
                page === n
                  ? "bg-sky-500 text-white"
                  : "border border-white/10 bg-white/5 text-slate-200 hover:bg-white/10"
              } ${limit === "all" ? "opacity-40" : ""}`}
            >
              {n}
            </button>
          ))}

          <button
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page >= totalPages || limit === "all"}
            className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-200 transition hover:bg-white/10 disabled:opacity-40"
          >
            次へ
          </button>

          <button
            onClick={() => setPage(totalPages)}
            disabled={page >= totalPages || limit === "all"}
            className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-slate-200 transition hover:bg-white/10 disabled:opacity-40"
          >
            最後
          </button>
        </div>
      </div>
    </main>
  );
}