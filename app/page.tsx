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

type AdvancedFilterModalKey =
  | "prefecture"
  | "industry"
  | "established"
  | "capital"
  | "employeeCount"
  | "tag";

type PrefectureValueItem = {
  region: string;
  prefecture: string;
  cities: string[];
};

type IndustryValueItem = {
  industryParent: string;
  bigIndustry: string;
  smallIndustries: string[];
};

type TagValueItem = {
  parent: string;
  tags: string[];
};

type AdvancedFiltersState = {
  prefectures: {
    regions: string[];
    prefectures: string[];
    cities: string[];
  };
  industries: {
    bigIndustries: string[];
    smallIndustries: string[];
  };
  established: {
    years: string[];
    yearMonths: string[];
    from: string;
    to: string;
  };
  capital: {
    min: string;
    max: string;
  };
  employeeCount: {
    min: string;
    max: string;
  };
  tags: {
    parents: string[];
    tags: string[];
  };
};

type AdvancedValueOptions = {
  regions: string[];
  prefectureItems: PrefectureValueItem[];
  industryItems: IndustryValueItem[];
  establishedYears: string[];
  establishedMonthsByYear: Record<string, string[]>;
  tagItems: TagValueItem[];
};

type ApiResponse = {
  ok: boolean;
  total?: number;
  page?: number;
  limit?: number | "all";
  totalPages?: number;
  rows?: Row[];
  values?: string[];
  regions?: string[];
  prefectures?: string[];
  bigIndustries?: string[];
  smallIndustries?: string[];
  years?: string[];
  monthsByYear?: Record<string, string[]>;
  yearMonths?: string[];
  parents?: string[];
  tags?: string[];
  items?: unknown[];
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

const ADVANCED_FILTER_BUTTONS: {
  key: AdvancedFilterModalKey;
  label: string;
}[] = [
  { key: "prefecture", label: "都道府県" },
  { key: "industry", label: "業種" },
  { key: "established", label: "設立" },
  { key: "capital", label: "資本金" },
  { key: "employeeCount", label: "従業員数" },
  { key: "tag", label: "タグ" },
];

const ADVANCED_FILTER_TITLES: Record<AdvancedFilterModalKey, string> = {
  prefecture: "都道府県",
  industry: "業種",
  established: "設立",
  capital: "資本金",
  employeeCount: "従業員数",
  tag: "タグ",
};

function createInitialAdvancedFiltersState(): AdvancedFiltersState {
  return {
    prefectures: {
      regions: [],
      prefectures: [],
      cities: [],
    },
    industries: {
      bigIndustries: [],
      smallIndustries: [],
    },
    established: {
      years: [],
      yearMonths: [],
      from: "",
      to: "",
    },
    capital: {
      min: "",
      max: "",
    },
    employeeCount: {
      min: "",
      max: "",
    },
    tags: {
      parents: [],
      tags: [],
    },
  };
}

function cloneAdvancedFiltersState(
  state: AdvancedFiltersState
): AdvancedFiltersState {
  return {
    prefectures: {
      regions: [...state.prefectures.regions],
      prefectures: [...state.prefectures.prefectures],
      cities: [...state.prefectures.cities],
    },
    industries: {
      bigIndustries: [...state.industries.bigIndustries],
      smallIndustries: [...state.industries.smallIndustries],
    },
    established: {
      years: [...state.established.years],
      yearMonths: [...state.established.yearMonths],
      from: state.established.from,
      to: state.established.to,
    },
    capital: {
      min: state.capital.min,
      max: state.capital.max,
    },
    employeeCount: {
      min: state.employeeCount.min,
      max: state.employeeCount.max,
    },
    tags: {
      parents: [...state.tags.parents],
      tags: [...state.tags.tags],
    },
  };
}

function createInitialAdvancedValueOptions(): AdvancedValueOptions {
  return {
    regions: [],
    prefectureItems: [],
    industryItems: [],
    establishedYears: [],
    establishedMonthsByYear: {},
    tagItems: [],
  };
}

function toggleArrayValue(values: string[], value: string) {
  return values.includes(value)
    ? values.filter((item) => item !== value)
    : [...values, value];
}

function addArrayValues(base: string[], values: string[]) {
  return Array.from(new Set([...base, ...values]));
}

function removeArrayValues(base: string[], values: string[]) {
  const removeSet = new Set(values);
  return base.filter((value) => !removeSet.has(value));
}

function includesAllValues(base: string[], values: string[]) {
  if (values.length === 0) return false;
  return values.every((value) => base.includes(value));
}

function formatYearMonthInputValue(value: string) {
  if (!value || value.length !== 6) return "";
  return `${value.slice(0, 4)}-${value.slice(4, 6)}`;
}

function parseYearMonthInputValue(value: string) {
  return value ? value.replace("-", "") : "";
}

const ESTABLISHED_MONTH_OPTIONS = Array.from({ length: 12 }, (_, i) => {
  const value = String(i + 1).padStart(2, "0");
  return {
    value,
    label: `${i + 1}月`,
  };
});

function getYearPart(value: string) {
  return value && value.length >= 4 ? value.slice(0, 4) : "";
}

function getMonthPart(value: string) {
  if (!value) return "";
  if (value.length === 6) return value.slice(4, 6);
  if (value.length === 2) return value;
  return "";
}

function buildYearMonthValue(year: string, month: string) {
  if (year && month) return `${year}${month}`;
  if (year) return year;
  if (month) return month;
  return "";
}

function buildRequestAdvancedFilters(state: AdvancedFiltersState) {
  const result: Record<string, unknown> = {};

  if (
    state.prefectures.prefectures.length > 0 ||
    state.prefectures.cities.length > 0
  ) {
    result.prefectures = {
      regions: [],
      prefectures: state.prefectures.prefectures,
      cities: state.prefectures.cities,
    };
  }

  if (
    state.industries.bigIndustries.length > 0 ||
    state.industries.smallIndustries.length > 0
  ) {
    result.industries = {
      bigIndustries: state.industries.bigIndustries,
      smallIndustries: state.industries.smallIndustries,
    };
  }

  const normalizedEstablishedFrom =
    state.established.from.length === 6 ? state.established.from : "";
  const normalizedEstablishedTo =
    state.established.to.length === 6 ? state.established.to : "";

  if (normalizedEstablishedFrom !== "" || normalizedEstablishedTo !== "") {
    result.established = {
      years: [],
      yearMonths: [],
      from: normalizedEstablishedFrom,
      to: normalizedEstablishedTo,
    };
  }

  if (state.capital.min !== "" || state.capital.max !== "") {
    result.capital = {
      min: state.capital.min,
      max: state.capital.max,
    };
  }

  if (state.employeeCount.min !== "" || state.employeeCount.max !== "") {
    result.employeeCount = {
      min: state.employeeCount.min,
      max: state.employeeCount.max,
    };
  }

  if (state.tags.tags.length > 0) {
    result.tags = {
      parents: [],
      tags: state.tags.tags,
    };
  }

  return result;
}

function hasActiveAdvancedFilter(
  key: AdvancedFilterModalKey,
  state: AdvancedFiltersState
) {
  switch (key) {
    case "prefecture":
      return (
        state.prefectures.regions.length > 0 ||
        state.prefectures.prefectures.length > 0 ||
        state.prefectures.cities.length > 0
      );
    case "industry":
      return (
        state.industries.bigIndustries.length > 0 ||
        state.industries.smallIndustries.length > 0
      );
    case "established":
      return (
        state.established.years.length > 0 ||
        state.established.yearMonths.length > 0 ||
        state.established.from !== "" ||
        state.established.to !== ""
      );
    case "capital":
      return state.capital.min !== "" || state.capital.max !== "";
    case "employeeCount":
      return (
        state.employeeCount.min !== "" || state.employeeCount.max !== ""
      );
    case "tag":
      return state.tags.parents.length > 0 || state.tags.tags.length > 0;
    default:
      return false;
  }
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

  const [draftAdvancedFilters, setDraftAdvancedFilters] =
    useState<AdvancedFiltersState>(() => createInitialAdvancedFiltersState());
  const [appliedAdvancedFilters, setAppliedAdvancedFilters] =
    useState<AdvancedFiltersState>(() => createInitialAdvancedFiltersState());
  const [advancedValueOptions, setAdvancedValueOptions] =
    useState<AdvancedValueOptions>(() => createInitialAdvancedValueOptions());
  const [openAdvancedFilterKey, setOpenAdvancedFilterKey] =
    useState<AdvancedFilterModalKey | null>(null);
  const [advancedLoading, setAdvancedLoading] = useState(false);

  const [expandedPrefectures, setExpandedPrefectures] = useState<
    Record<string, boolean>
  >({});
  const [expandedPrefectureRegions, setExpandedPrefectureRegions] = useState<
    Record<string, boolean>
  >({});
  const [expandedIndustries, setExpandedIndustries] = useState<
    Record<string, boolean>
  >({});
  const [expandedIndustryParents, setExpandedIndustryParents] = useState<
    Record<string, boolean>
  >({});
  const [expandedYears, setExpandedYears] = useState<Record<string, boolean>>({});
  const [expandedTagParents, setExpandedTagParents] = useState<
    Record<string, boolean>
  >({});

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
        params.set(
          "advancedFilters",
          JSON.stringify(buildRequestAdvancedFilters(appliedAdvancedFilters))
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
        params.set(
          "advancedFilters",
          JSON.stringify(buildRequestAdvancedFilters(appliedAdvancedFilters))
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

  const fetchAdvancedFilterValues = async (key: AdvancedFilterModalKey) => {
    if (key === "capital" || key === "employeeCount") {
      return;
    }

    setAdvancedLoading(true);

    try {
      const params = new URLSearchParams();
      params.set("advancedValuesFor", key);
      params.set(
        "filterModels",
        JSON.stringify(buildRequestFilterModels(appliedColumnStates))
      );
      params.set(
        "advancedFilters",
        JSON.stringify(buildRequestAdvancedFilters(appliedAdvancedFilters))
      );

      const res = await fetch(`/api/master_data?${params.toString()}`, {
        cache: "no-store",
      });

      const data = await readApiResponse(res);
      if (!res.ok || !data.ok) {
        throw new Error(data.error || "候補の取得に失敗しました");
      }

      setAdvancedValueOptions((prev) => {
        const next = { ...prev };

        if (key === "prefecture") {
          next.regions = data.regions || [];
          next.prefectureItems = Array.isArray(data.items)
            ? (data.items as PrefectureValueItem[])
            : [];
        }

        if (key === "industry") {
          next.industryItems = Array.isArray(data.items)
            ? (data.items as IndustryValueItem[])
            : [];
        }

        if (key === "established") {
          next.establishedYears = data.years || [];
          next.establishedMonthsByYear = data.monthsByYear || {};
        }

        if (key === "tag") {
          next.tagItems = Array.isArray(data.items)
            ? (data.items as TagValueItem[])
            : [];
        }

        return next;
      });
    } catch (e) {
      console.error(e);
    } finally {
      setAdvancedLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [page, limit, appliedColumnStates, appliedAdvancedFilters]);

  useEffect(() => {
    setHeaderStickyTop(0);
  }, []);

    const handleOpenFilter = async (key: FilterKey) => {
      if (openFilterKey === key) {
        setOpenFilterKey(null);
        return;
      }

      setOpenAdvancedFilterKey(null);

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

    const handleOpenAdvancedFilter = async (key: AdvancedFilterModalKey) => {
      if (openAdvancedFilterKey === key) {
        setOpenAdvancedFilterKey(null);
        return;
      }

      setOpenFilterKey(null);
      setDraftAdvancedFilters(cloneAdvancedFiltersState(appliedAdvancedFilters));
      setOpenAdvancedFilterKey(key);

      if (key === "prefecture") {
        setExpandedPrefectureRegions({});
        setExpandedPrefectures({});
      }
      if (key === "industry") {
        setExpandedIndustryParents({});
        setExpandedIndustries({});
      }
      if (key === "established") {
        setExpandedYears({});
      }
      if (key === "tag") {
        setExpandedTagParents({});
      }

      await fetchAdvancedFilterValues(key);
    };

    const clearAdvancedFilter = (key: AdvancedFilterModalKey) => {
      const empty = createInitialAdvancedFiltersState();

      setDraftAdvancedFilters((prev) => {
        const next = cloneAdvancedFiltersState(prev);

        if (key === "prefecture") next.prefectures = empty.prefectures;
        if (key === "industry") next.industries = empty.industries;
        if (key === "established") next.established = empty.established;
        if (key === "capital") next.capital = empty.capital;
        if (key === "employeeCount") next.employeeCount = empty.employeeCount;
        if (key === "tag") next.tags = empty.tags;

        return next;
      });

      setAppliedAdvancedFilters((prev) => {
        const next = cloneAdvancedFiltersState(prev);

        if (key === "prefecture") next.prefectures = empty.prefectures;
        if (key === "industry") next.industries = empty.industries;
        if (key === "established") next.established = empty.established;
        if (key === "capital") next.capital = empty.capital;
        if (key === "employeeCount") next.employeeCount = empty.employeeCount;
        if (key === "tag") next.tags = empty.tags;

        return next;
      });

      setPage(1);
      setOpenAdvancedFilterKey(null);
    };

    const applyAdvancedFilter = () => {
      setAppliedAdvancedFilters(cloneAdvancedFiltersState(draftAdvancedFilters));
      setPage(1);
      setOpenAdvancedFilterKey(null);
    };

  const renderAdvancedFilterContent = () => {
    if (openAdvancedFilterKey === "prefecture") {
      const items = advancedValueOptions.prefectureItems;
      const regions =
        advancedValueOptions.regions.length > 0
          ? advancedValueOptions.regions
          : Array.from(new Set(items.map((item) => item.region)));

      return (
        <div className="space-y-4">
          {regions.map((region) => {
            const regionItems = items.filter((item) => item.region === region);
            const isRegionOpen = !!expandedPrefectureRegions[region];
            const regionPrefectures = regionItems.map((item) => item.prefecture);
            const regionCities = regionItems.flatMap((item) => item.cities);
            const isRegionChecked =
              includesAllValues(
                draftAdvancedFilters.prefectures.prefectures,
                regionPrefectures
              ) &&
              includesAllValues(
                draftAdvancedFilters.prefectures.cities,
                regionCities
              );

            return (
              <div
                key={region}
                className="rounded-xl border border-white/10 bg-[#0f172a] p-4"
              >
                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    checked={isRegionChecked}
                    onChange={() =>
                      setDraftAdvancedFilters((prev) => {
                        const next = cloneAdvancedFiltersState(prev);
                        next.prefectures.regions = [];

                        if (isRegionChecked) {
                          next.prefectures.prefectures = removeArrayValues(
                            next.prefectures.prefectures,
                            regionPrefectures
                          );
                          next.prefectures.cities = removeArrayValues(
                            next.prefectures.cities,
                            regionCities
                          );
                        } else {
                          next.prefectures.prefectures = addArrayValues(
                            next.prefectures.prefectures,
                            regionPrefectures
                          );
                          next.prefectures.cities = addArrayValues(
                            next.prefectures.cities,
                            regionCities
                          );
                        }

                        return next;
                      })
                    }
                    className="h-4 w-4 accent-sky-500"
                  />

                  <button
                    type="button"
                    onClick={() =>
                      setExpandedPrefectureRegions((prev) => ({
                        ...prev,
                        [region]: !prev[region],
                      }))
                    }
                    className="flex-1 text-left text-sm font-semibold text-slate-100"
                  >
                    {isRegionOpen ? "▼" : "▶"} {region}
                  </button>
                </div>

                {isRegionOpen && (
                  <div className="mt-4 grid grid-cols-1 gap-3 xl:grid-cols-2">
                    {regionItems.map((item) => {
                      const isPrefectureOpen =
                        !!expandedPrefectures[item.prefecture];
                      const isPrefectureChecked =
                        draftAdvancedFilters.prefectures.prefectures.includes(
                          item.prefecture
                        ) &&
                        includesAllValues(
                          draftAdvancedFilters.prefectures.cities,
                          item.cities
                        );

                      return (
                        <div
                          key={item.prefecture}
                          className="rounded-xl border border-white/10 bg-white/5 p-3"
                        >
                          <div className="flex items-center gap-2">
                            <input
                              type="checkbox"
                              checked={isPrefectureChecked}
                              onChange={() =>
                                setDraftAdvancedFilters((prev) => {
                                  const next = cloneAdvancedFiltersState(prev);
                                  next.prefectures.regions = [];

                                  if (isPrefectureChecked) {
                                    next.prefectures.prefectures =
                                      removeArrayValues(
                                        next.prefectures.prefectures,
                                        [item.prefecture]
                                      );
                                    next.prefectures.cities = removeArrayValues(
                                      next.prefectures.cities,
                                      item.cities
                                    );
                                  } else {
                                    next.prefectures.prefectures =
                                      addArrayValues(
                                        next.prefectures.prefectures,
                                        [item.prefecture]
                                      );
                                    next.prefectures.cities = addArrayValues(
                                      next.prefectures.cities,
                                      item.cities
                                    );
                                  }

                                  return next;
                                })
                              }
                              className="h-4 w-4 accent-sky-500"
                            />

                            <button
                              type="button"
                              onClick={() =>
                                setExpandedPrefectures((prev) => ({
                                  ...prev,
                                  [item.prefecture]: !prev[item.prefecture],
                                }))
                              }
                              className="flex-1 text-left text-sm font-medium text-slate-100"
                            >
                              {isPrefectureOpen ? "▼" : "▶"} {item.prefecture}
                            </button>
                          </div>

                          {isPrefectureOpen && item.cities.length > 0 && (
                            <div className="mt-3 ml-4 grid grid-cols-1 gap-2 sm:grid-cols-2">
                              {item.cities.map((city) => (
                                <label
                                  key={`${item.prefecture}-${city}`}
                                  className="flex items-center gap-2 rounded-lg px-2 py-2 text-sm text-slate-200 hover:bg-white/5"
                                >
                                  <input
                                    type="checkbox"
                                    checked={draftAdvancedFilters.prefectures.cities.includes(
                                      city
                                    )}
                                    onChange={() =>
                                      setDraftAdvancedFilters((prev) => {
                                        const next = cloneAdvancedFiltersState(
                                          prev
                                        );
                                        next.prefectures.regions = [];
                                        next.prefectures.prefectures =
                                          removeArrayValues(
                                            next.prefectures.prefectures,
                                            [item.prefecture]
                                          );
                                        next.prefectures.cities =
                                          toggleArrayValue(
                                            next.prefectures.cities,
                                            city
                                          );
                                        return next;
                                      })
                                    }
                                    className="h-4 w-4 accent-sky-500"
                                  />
                                  <span>{city}</span>
                                </label>
                              ))}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      );
    }

    if (openAdvancedFilterKey === "industry") {
      const industryParents = Array.from(
        new Set(
          advancedValueOptions.industryItems.map((item) => item.industryParent)
        )
      );

      return (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2 2xl:grid-cols-3">
          {industryParents.map((industryParent) => {
            const parentItems = advancedValueOptions.industryItems.filter(
              (item) => item.industryParent === industryParent
            );
            const isParentOpen = !!expandedIndustryParents[industryParent];
            const parentBigIndustries = parentItems.map(
              (item) => item.bigIndustry
            );
            const parentSmallIndustries = parentItems.flatMap(
              (item) => item.smallIndustries
            );
            const isParentChecked =
              includesAllValues(
                draftAdvancedFilters.industries.bigIndustries,
                parentBigIndustries
              ) &&
              includesAllValues(
                draftAdvancedFilters.industries.smallIndustries,
                parentSmallIndustries
              );

            return (
              <div
                key={industryParent}
                className="rounded-xl border border-white/10 bg-[#0f172a] p-4"
              >
                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    checked={isParentChecked}
                    onChange={() =>
                      setDraftAdvancedFilters((prev) => {
                        const next = cloneAdvancedFiltersState(prev);

                        if (isParentChecked) {
                          next.industries.bigIndustries = removeArrayValues(
                            next.industries.bigIndustries,
                            parentBigIndustries
                          );
                          next.industries.smallIndustries = removeArrayValues(
                            next.industries.smallIndustries,
                            parentSmallIndustries
                          );
                        } else {
                          next.industries.bigIndustries = addArrayValues(
                            next.industries.bigIndustries,
                            parentBigIndustries
                          );
                          next.industries.smallIndustries = addArrayValues(
                            next.industries.smallIndustries,
                            parentSmallIndustries
                          );
                        }

                        return next;
                      })
                    }
                    className="h-4 w-4 accent-sky-500"
                  />

                  <button
                    type="button"
                    onClick={() =>
                      setExpandedIndustryParents((prev) => ({
                        ...prev,
                        [industryParent]: !prev[industryParent],
                      }))
                    }
                    className="flex-1 text-left text-sm font-semibold text-slate-100"
                  >
                    {isParentOpen ? "▼" : "▶"} {industryParent}
                  </button>
                </div>

                {isParentOpen && (
                  <div className="mt-4 space-y-3">
                    {parentItems.map((item) => {
                      const isOpen = !!expandedIndustries[item.bigIndustry];
                      const isBigIndustryChecked =
                        draftAdvancedFilters.industries.bigIndustries.includes(
                          item.bigIndustry
                        ) &&
                        includesAllValues(
                          draftAdvancedFilters.industries.smallIndustries,
                          item.smallIndustries
                        );

                      return (
                        <div
                          key={item.bigIndustry}
                          className="rounded-xl border border-white/10 bg-white/5 p-3"
                        >
                          <div className="flex items-center gap-2">
                            <input
                              type="checkbox"
                              checked={isBigIndustryChecked}
                              onChange={() =>
                                setDraftAdvancedFilters((prev) => {
                                  const next = cloneAdvancedFiltersState(prev);

                                  if (isBigIndustryChecked) {
                                    next.industries.bigIndustries =
                                      removeArrayValues(
                                        next.industries.bigIndustries,
                                        [item.bigIndustry]
                                      );
                                    next.industries.smallIndustries =
                                      removeArrayValues(
                                        next.industries.smallIndustries,
                                        item.smallIndustries
                                      );
                                  } else {
                                    next.industries.bigIndustries =
                                      addArrayValues(
                                        next.industries.bigIndustries,
                                        [item.bigIndustry]
                                      );
                                    next.industries.smallIndustries =
                                      addArrayValues(
                                        next.industries.smallIndustries,
                                        item.smallIndustries
                                      );
                                  }

                                  return next;
                                })
                              }
                              className="h-4 w-4 accent-sky-500"
                            />

                            <button
                              type="button"
                              onClick={() =>
                                setExpandedIndustries((prev) => ({
                                  ...prev,
                                  [item.bigIndustry]: !prev[item.bigIndustry],
                                }))
                              }
                              className="flex-1 text-left text-sm font-medium text-slate-100"
                            >
                              {isOpen ? "▼" : "▶"} {item.bigIndustry}
                            </button>
                          </div>

                          {isOpen && (
                            <div className="mt-3 ml-4 grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-3">
                              {item.smallIndustries.map((smallIndustry) => (
                                <label
                                  key={`${item.bigIndustry}-${smallIndustry}`}
                                  className="flex items-center gap-2 rounded-lg px-2 py-2 text-sm text-slate-200 hover:bg-white/5"
                                >
                                  <input
                                    type="checkbox"
                                    checked={draftAdvancedFilters.industries.smallIndustries.includes(
                                      smallIndustry
                                    )}
                                    onChange={() =>
                                      setDraftAdvancedFilters((prev) => {
                                        const next = cloneAdvancedFiltersState(
                                          prev
                                        );
                                        next.industries.bigIndustries =
                                          removeArrayValues(
                                            next.industries.bigIndustries,
                                            [item.bigIndustry]
                                          );
                                        next.industries.smallIndustries =
                                          toggleArrayValue(
                                            next.industries.smallIndustries,
                                            smallIndustry
                                          );
                                        return next;
                                      })
                                    }
                                    className="h-4 w-4 accent-sky-500"
                                  />
                                  <span>{smallIndustry}</span>
                                </label>
                              ))}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      );
    }

    if (openAdvancedFilterKey === "established") {
      const years = advancedValueOptions.establishedYears;
      const fromYear = getYearPart(draftAdvancedFilters.established.from);
      const fromMonth = getMonthPart(draftAdvancedFilters.established.from);
      const toYear = getYearPart(draftAdvancedFilters.established.to);
      const toMonth = getMonthPart(draftAdvancedFilters.established.to);

      return (
        <div className="mx-auto w-full max-w-[1100px] rounded-xl border border-white/10 bg-[#0f172a] p-5">
          <div className="mb-4 text-sm font-semibold text-slate-100">
            期間指定
          </div>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <div className="rounded-xl border border-white/10 bg-[#111827] p-5">
              <div className="mb-3 text-sm font-medium text-slate-100">
                開始
              </div>

              <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                <div className="min-w-0">
                  <div className="mb-2 text-xs text-slate-400">年</div>
                  <select
                    value={fromYear}
                    onChange={(e) =>
                      setDraftAdvancedFilters((prev) => {
                        const next = cloneAdvancedFiltersState(prev);
                        next.established.from = buildYearMonthValue(
                          e.target.value,
                          getMonthPart(next.established.from)
                        );
                        return next;
                      })
                    }
                    className="h-11 w-full min-w-0 rounded-xl border border-white/10 bg-[#0b1220] px-3 text-sm text-slate-100 outline-none focus:border-sky-500"
                  >
                    <option value="">選択</option>
                    {years.map((year) => (
                      <option key={`from-year-${year}`} value={year}>
                        {year}年
                      </option>
                    ))}
                  </select>
                </div>

                <div className="min-w-0">
                  <div className="mb-2 text-xs text-slate-400">月</div>
                  <select
                    value={fromMonth}
                    onChange={(e) =>
                      setDraftAdvancedFilters((prev) => {
                        const next = cloneAdvancedFiltersState(prev);
                        next.established.from = buildYearMonthValue(
                          getYearPart(next.established.from),
                          e.target.value
                        );
                        return next;
                      })
                    }
                    className="h-11 w-full min-w-0 rounded-xl border border-white/10 bg-[#0b1220] px-3 text-sm text-slate-100 outline-none focus:border-sky-500"
                  >
                    <option value="">選択</option>
                    {ESTABLISHED_MONTH_OPTIONS.map((month) => (
                      <option
                        key={`from-month-${month.value}`}
                        value={month.value}
                      >
                        {month.label}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
            </div>

            <div className="rounded-xl border border-white/10 bg-[#111827] p-5">
              <div className="mb-3 text-sm font-medium text-slate-100">
                終了
              </div>

              <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                <div className="min-w-0">
                  <div className="mb-2 text-xs text-slate-400">年</div>
                  <select
                    value={toYear}
                    onChange={(e) =>
                      setDraftAdvancedFilters((prev) => {
                        const next = cloneAdvancedFiltersState(prev);
                        next.established.to = buildYearMonthValue(
                          e.target.value,
                          getMonthPart(next.established.to)
                        );
                        return next;
                      })
                    }
                    className="h-11 w-full min-w-0 rounded-xl border border-white/10 bg-[#0b1220] px-3 text-sm text-slate-100 outline-none focus:border-sky-500"
                  >
                    <option value="">選択</option>
                    {years.map((year) => (
                      <option key={`to-year-${year}`} value={year}>
                        {year}年
                      </option>
                    ))}
                  </select>
                </div>

                <div className="min-w-0">
                  <div className="mb-2 text-xs text-slate-400">月</div>
                  <select
                    value={toMonth}
                    onChange={(e) =>
                      setDraftAdvancedFilters((prev) => {
                        const next = cloneAdvancedFiltersState(prev);
                        next.established.to = buildYearMonthValue(
                          getYearPart(next.established.to),
                          e.target.value
                        );
                        return next;
                      })
                    }
                    className="h-11 w-full min-w-0 rounded-xl border border-white/10 bg-[#0b1220] px-3 text-sm text-slate-100 outline-none focus:border-sky-500"
                  >
                    <option value="">選択</option>
                    {ESTABLISHED_MONTH_OPTIONS.map((month) => (
                      <option key={`to-month-${month.value}`} value={month.value}>
                        {month.label}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
            </div>
          </div>
        </div>
      );
    }

    if (openAdvancedFilterKey === "capital") {
      return (
        <div className="rounded-xl border border-white/10 bg-[#0f172a] p-4">
          <div className="mb-3 text-sm font-semibold text-slate-100">
            資本金の範囲指定
          </div>

          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <div>
              <div className="mb-2 text-xs text-slate-400">最小値</div>
              <input
                value={draftAdvancedFilters.capital.min}
                onChange={(e) =>
                  setDraftAdvancedFilters((prev) => {
                    const next = cloneAdvancedFiltersState(prev);
                    next.capital.min = e.target.value;
                    return next;
                  })
                }
                placeholder="100"
                className="h-10 w-full rounded-xl border border-white/10 bg-[#111827] px-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-sky-500"
              />
            </div>

            <div>
              <div className="mb-2 text-xs text-slate-400">最大値</div>
              <input
                value={draftAdvancedFilters.capital.max}
                onChange={(e) =>
                  setDraftAdvancedFilters((prev) => {
                    const next = cloneAdvancedFiltersState(prev);
                    next.capital.max = e.target.value;
                    return next;
                  })
                }
                placeholder="100000"
                className="h-10 w-full rounded-xl border border-white/10 bg-[#111827] px-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-sky-500"
              />
            </div>
          </div>
        </div>
      );
    }

    if (openAdvancedFilterKey === "employeeCount") {
      return (
        <div className="rounded-xl border border-white/10 bg-[#0f172a] p-4">
          <div className="mb-3 text-sm font-semibold text-slate-100">
            従業員数の範囲指定
          </div>

          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <div>
              <div className="mb-2 text-xs text-slate-400">最小値</div>
              <input
                value={draftAdvancedFilters.employeeCount.min}
                onChange={(e) =>
                  setDraftAdvancedFilters((prev) => {
                    const next = cloneAdvancedFiltersState(prev);
                    next.employeeCount.min = e.target.value;
                    return next;
                  })
                }
                placeholder="100"
                className="h-10 w-full rounded-xl border border-white/10 bg-[#111827] px-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-sky-500"
              />
            </div>

            <div>
              <div className="mb-2 text-xs text-slate-400">最大値</div>
              <input
                value={draftAdvancedFilters.employeeCount.max}
                onChange={(e) =>
                  setDraftAdvancedFilters((prev) => {
                    const next = cloneAdvancedFiltersState(prev);
                    next.employeeCount.max = e.target.value;
                    return next;
                  })
                }
                placeholder="100000"
                className="h-10 w-full rounded-xl border border-white/10 bg-[#111827] px-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-sky-500"
              />
            </div>
          </div>
        </div>
      );
    }

    if (openAdvancedFilterKey === "tag") {
      return (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2 2xl:grid-cols-3">
          {advancedValueOptions.tagItems.map((item) => {
            const isOpen = !!expandedTagParents[item.parent];
            const isParentChecked = includesAllValues(
              draftAdvancedFilters.tags.tags,
              item.tags
            );

            return (
              <div
                key={item.parent}
                className="rounded-xl border border-white/10 bg-[#0f172a] p-4"
              >
                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    checked={isParentChecked}
                    onChange={() =>
                      setDraftAdvancedFilters((prev) => {
                        const next = cloneAdvancedFiltersState(prev);
                        next.tags.parents = [];

                        if (isParentChecked) {
                          next.tags.tags = removeArrayValues(
                            next.tags.tags,
                            item.tags
                          );
                        } else {
                          next.tags.tags = addArrayValues(
                            next.tags.tags,
                            item.tags
                          );
                        }

                        return next;
                      })
                    }
                    className="h-4 w-4 accent-sky-500"
                  />

                  <button
                    type="button"
                    onClick={() =>
                      setExpandedTagParents((prev) => ({
                        ...prev,
                        [item.parent]: !prev[item.parent],
                      }))
                    }
                    className="flex-1 text-left text-sm font-semibold text-slate-100"
                  >
                    {isOpen ? "▼" : "▶"} {item.parent}
                  </button>
                </div>

                {isOpen && (
                  <div className="mt-3 ml-4 grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-3">
                    {item.tags.map((tag) => (
                      <label
                        key={`${item.parent}-${tag}`}
                        className="flex items-center gap-2 rounded-lg px-2 py-2 text-sm text-slate-200 hover:bg-white/5"
                      >
                        <input
                          type="checkbox"
                          checked={draftAdvancedFilters.tags.tags.includes(tag)}
                          onChange={() =>
                            setDraftAdvancedFilters((prev) => {
                              const next = cloneAdvancedFiltersState(prev);
                              next.tags.parents = [];
                              next.tags.tags = toggleArrayValue(
                                next.tags.tags,
                                tag
                              );
                              return next;
                            })
                          }
                          className="h-4 w-4 accent-sky-500"
                        />
                        <span>{tag}</span>
                      </label>
                    ))}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      );
    }

    return null;
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
        params.set(
          "advancedFilters",
          JSON.stringify(buildRequestAdvancedFilters(appliedAdvancedFilters))
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

  const activeAdvancedFilterTitle = openAdvancedFilterKey
    ? ADVANCED_FILTER_TITLES[openAdvancedFilterKey]
    : "";

  const activeAdvancedFilterKey = openAdvancedFilterKey;

  const isWideAdvancedFilterModal =
    activeAdvancedFilterKey === "prefecture" ||
    activeAdvancedFilterKey === "industry" ||
    activeAdvancedFilterKey === "tag";

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

            <div className="mb-6 rounded-2xl border border-white/10 bg-white/5 p-4">
              <div className="mb-3 text-sm font-semibold text-slate-200">
                絞り込み
              </div>

              <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
                {ADVANCED_FILTER_BUTTONS.map((button) => {
                  const active = hasActiveAdvancedFilter(
                    button.key,
                    appliedAdvancedFilters
                  );

                  return (
                    <button
                      key={button.key}
                      type="button"
                      onClick={() => handleOpenAdvancedFilter(button.key)}
                      className={`h-12 rounded-xl border px-4 text-sm font-medium transition ${
                        active
                          ? "border-sky-400/40 bg-sky-500/20 text-sky-100 hover:bg-sky-500/30"
                          : "border-white/10 bg-[#0f172a] text-slate-200 hover:bg-white/10"
                      }`}
                    >
                      {button.label}
                    </button>
                  );
                })}
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

        {activeAdvancedFilterKey &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => setOpenAdvancedFilterKey(null)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className={`flex w-full flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl ${
                    isWideAdvancedFilterModal
                      ? "max-w-[calc(100vw-32px)] h-[calc(100dvh-32px)]"
                      : "max-w-[720px] max-h-[calc(100dvh-32px)]"
                  }`}
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      {activeAdvancedFilterTitle} のフィルタ
                    </div>

                    <button
                      type="button"
                      onClick={() => setOpenAdvancedFilterKey(null)}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="flex-1 overflow-y-auto px-4 py-4">
                    {advancedLoading &&
                    activeAdvancedFilterKey !== "capital" &&
                    activeAdvancedFilterKey !== "employeeCount" ? (
                      <div className="rounded-xl border border-white/10 bg-[#0f172a] px-4 py-10 text-center text-sm text-slate-400">
                        読み込み中です...
                      </div>
                    ) : (
                      renderAdvancedFilterContent()
                    )}
                  </div>

                  <div
                    className={`border-t border-white/10 px-4 py-4 ${
                      isWideAdvancedFilterModal
                        ? "flex justify-center gap-3"
                        : "flex gap-2"
                    }`}
                  >
                    <button
                      type="button"
                      onClick={applyAdvancedFilter}
                      className={`h-10 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400 ${
                        isWideAdvancedFilterModal
                          ? "w-[160px] flex-none"
                          : "flex-1"
                      }`}
                    >
                      適用
                    </button>

                    <button
                      type="button"
                      onClick={() =>
                        activeAdvancedFilterKey &&
                        clearAdvancedFilter(activeAdvancedFilterKey)
                      }
                      className={`h-10 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 ${
                        isWideAdvancedFilterModal
                          ? "w-[160px] flex-none"
                          : "flex-1"
                      }`}
                    >
                      クリア
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

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