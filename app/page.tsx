"use client";

import { type ReactNode, useEffect, useMemo, useRef, useState } from "react";
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
  tag: string | null;
  business_type: string | null;
  business_content: string | null;
  industry_category: string | null;
  memo: string | null;
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
  allValues: string[];
  availableValues: string[];
  valueCounts: Record<string, number>;
  availableValueTotal: number;
  availableValueMatchedCount: number;
  totalItemCount: number;
  checkedItemCount: number;
  valueOffset: number;
  valueLimit: number;
  valueLoading: boolean;
  hasMoreValues: boolean;
  valueSearch: string;
};

type AdvancedFilterModalKey =
  | "companyName"
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
  prefectureCount: number;
  cityCounts: Record<string, number>;
};

type IndustryValueItem = {
  industryParent: string;
  bigIndustry: string;
  smallIndustries: string[];
  bigIndustryCount: number;
  smallIndustryCounts: Record<string, number>;
};

type TagValueItem = {
  parent: string;
  tags: string[];
  parentCount: number;
  tagCounts: Record<string, number>;
};

type AdvancedFiltersState = {
  companyName: {
    keyword: string;
  };
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

const CRAWL_CONFIRM_FIELD_OPTIONS = [
  { key: "company", label: "企業名" },
  { key: "zipcode", label: "郵便番号" },
  { key: "address", label: "住所" },
  { key: "website_url", label: "企業URL" },
  { key: "form_url", label: "お問い合わせフォームURL" },
  { key: "phone", label: "電話番号" },
  { key: "fax", label: "FAX番号" },
  { key: "email", label: "メールアドレス" },
  { key: "established_date", label: "設立年月" },
  { key: "representative_name", label: "代表者名" },
  { key: "capital", label: "資本金" },
  { key: "employee_count", label: "従業員数" },
  { key: "business_content", label: "事業内容" },
] as const;

type CrawlFieldKey =
  (typeof CRAWL_CONFIRM_FIELD_OPTIONS)[number]["key"];

function createInitialCrawlFieldSelections(): Record<CrawlFieldKey, boolean> {
  return CRAWL_CONFIRM_FIELD_OPTIONS.reduce((acc, field) => {
    acc[field.key] = true;
    return acc;
  }, {} as Record<CrawlFieldKey, boolean>);
}

function createEmptyCrawlFieldSelections(): Record<CrawlFieldKey, boolean> {
  return CRAWL_CONFIRM_FIELD_OPTIONS.reduce((acc, field) => {
    acc[field.key] = false;
    return acc;
  }, {} as Record<CrawlFieldKey, boolean>);
}

function getSelectedCrawlFields(
  selections: Record<CrawlFieldKey, boolean>
): CrawlFieldKey[] {
  return CRAWL_CONFIRM_FIELD_OPTIONS.filter(
    (field) => selections[field.key]
  ).map((field) => field.key);
}

type CrawlPreviewChange = {
  key: CrawlFieldKey;
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

type CrawlSelectedChangeValue = string | null;
type CrawlSelectedChanges = Record<
  string,
  Partial<Record<CrawlFieldKey, CrawlSelectedChangeValue>>
>;

type ItemInspectionMethodKey = "representative_name_remove_non_name";

type ItemInspectionPreviewChange = {
  rowId: string;
  company: string | null;
  fieldLabel: string;
  beforeValue: string | null;
  afterValue: string | null;
  action: "update" | "delete";
  reason: string;
};

function createInitialItemInspectionMethodSelections(): Record<
  ItemInspectionMethodKey,
  boolean
> {
  return {
    representative_name_remove_non_name: false,
  };
}

function createEmptyItemInspectionMethodSelections(): Record<
  ItemInspectionMethodKey,
  boolean
> {
  return {
    representative_name_remove_non_name: false,
  };
}

type ApiResponse = {
  ok: boolean;
  total?: number;
  page?: number;
  limit?: number | "all";
  totalPages?: number;
  rows?: Row[];
  values?: string[];
  allValues?: string[];
  valueCounts?: Record<string, number>;
  valueTotal?: number;
  valueMatchedCount?: number;
  valueOffset?: number;
  valueLimit?: number;
  hasMoreValues?: boolean;
  totalItemCount?: number;
  checkedItemCount?: number;
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
  processed?: number;
  updated?: number;
  skipped?: number;
  failed?: number;
  deleted?: number;
  preview?: boolean;
  previewRows?: CrawlPreviewRow[];
  inspectionPreviewChanges?: ItemInspectionPreviewChange[];

  previewTotal?: number;
  previewPage?: number;
  previewPageSize?: number;

  jobId?: string | null;
  jobStatus?: "running" | "paused" | "completed" | "error";
  totalTargets?: number;
  currentInspectionValue?: string | null;
  currentInspectionFieldLabel?: string | null;
  currentCompany?: string | null;
  currentWebsiteUrl?: string | null;
  currentFields?: string[];
  progressPercent?: number;
  remainingCount?: number;
  paused?: boolean;
  completed?: boolean;
};

async function readApiResponse(
  res: Response,
  emptyFallback: ApiResponse = { ok: true }
): Promise<ApiResponse> {
  const text = await res.text();

  if (!text) {
    return emptyFallback;
  }

  try {
    return JSON.parse(text) as ApiResponse;
  } catch {
    throw new Error("APIの返却形式がJSONではありません");
  }
}

type CrawlJobStatus = "idle" | "running" | "paused" | "completed" | "error";

type ItemInspectionJobStatus =
  | "idle"
  | "running"
  | "paused"
  | "completed"
  | "error";

function buildImportFileKey(file: File) {
  return `${file.name}__${file.size}__${file.lastModified}`;
}

const CRAWL_PREVIEW_PAGE_SIZE = 20;
const ITEM_INSPECTION_PREVIEW_PAGE_SIZE = 20;

const ACTIVE_CRAWL_JOB_STORAGE_KEY = "master-data-active-crawl-job-id";

function saveActiveCrawlJobId(jobId: string | null) {
  if (typeof window === "undefined") return;

  if (jobId) {
    window.localStorage.setItem(ACTIVE_CRAWL_JOB_STORAGE_KEY, jobId);
  } else {
    window.localStorage.removeItem(ACTIVE_CRAWL_JOB_STORAGE_KEY);
  }
}

function loadActiveCrawlJobId() {
  if (typeof window === "undefined") return null;
  return window.localStorage.getItem(ACTIVE_CRAWL_JOB_STORAGE_KEY);
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
  minmax(140px,1fr)
  minmax(320px,2.2fr)
  minmax(160px,1.1fr)
  minmax(320px,2.2fr)
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

const CSV_TEMPLATE_HEADERS = [
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

const COLUMN_DEFS: { key: FilterKey; label: string }[] = [
  { key: "company", label: "企業名" },
  { key: "zipcode", label: "郵便番号" },
  { key: "address", label: "住所" },
  { key: "big_industry", label: "大業種" },
  { key: "small_industry", label: "小業種" },
  { key: "company_kana", label: "企業名（かな）" },
  { key: "summary", label: "企業概要" },
  { key: "website_url", label: "企業URL" },
  { key: "form_url", label: "お問い合わせフォームURL" },
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
  { key: "tag", label: "タグ" },
  { key: "business_type", label: "業種" },
  { key: "business_content", label: "事業内容" },
  { key: "industry_category", label: "業界" },
  { key: "memo", label: "メモ" },
];

function createInitialItemDeleteSelections(): Record<FilterKey, boolean> {
  return COLUMN_DEFS.reduce((acc, column) => {
    acc[column.key] = true;
    return acc;
  }, {} as Record<FilterKey, boolean>);
}

function createEmptyItemDeleteSelections(): Record<FilterKey, boolean> {
  return COLUMN_DEFS.reduce((acc, column) => {
    acc[column.key] = false;
    return acc;
  }, {} as Record<FilterKey, boolean>);
}

function getSelectedItemDeleteFields(
  selections: Record<FilterKey, boolean>
): FilterKey[] {
  return COLUMN_DEFS.filter((column) => selections[column.key]).map(
    (column) => column.key
  );
}

function createEmptyColumnState(): ColumnFilterState {
  return {
    sortDirection: "",
    conditionType: "",
    conditionValue: "",
    valueFilterEnabled: false,
    selectedValues: [],
    allValues: [],
    availableValues: [],
    valueCounts: {},
    availableValueTotal: 0,
    availableValueMatchedCount: 0,
    totalItemCount: 0,
    checkedItemCount: 0,
    valueOffset: 0,
    valueLimit: 200,
    valueLoading: false,
    hasMoreValues: false,
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
      allValues: [...states[column.key].allValues],
      availableValues: [...states[column.key].availableValues],
      valueCounts: { ...states[column.key].valueCounts },
    };
    return acc;
  }, {} as Record<FilterKey, ColumnFilterState>);
}

function createClearedColumnStates(
  states: Record<FilterKey, ColumnFilterState>
): Record<FilterKey, ColumnFilterState> {
  return COLUMN_DEFS.reduce((acc, column) => {
    acc[column.key] = {
      ...createEmptyColumnState(),
      availableValues: [...states[column.key].availableValues],
      valueCounts: { ...states[column.key].valueCounts },
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
    const selectedValues = state.selectedValues ?? [];

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
      model.selectedValues = selectedValues;
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
  { key: "companyName", label: "企業名" },
  { key: "prefecture", label: "都道府県" },
  { key: "industry", label: "業種" },
  { key: "established", label: "設立" },
  { key: "capital", label: "資本金" },
  { key: "employeeCount", label: "従業員数" },
  { key: "tag", label: "タグ" },
];

const ADVANCED_FILTER_TITLES: Record<AdvancedFilterModalKey, string> = {
  companyName: "企業名",
  prefecture: "都道府県",
  industry: "業種",
  established: "設立",
  capital: "資本金",
  employeeCount: "従業員数",
  tag: "タグ",
};

type SidebarPanelKey = "search" | "list" | "csv" | "inspection" | "theme";

type FilterClearConfirmTarget =
  | { type: "column"; key: FilterKey }
  | { type: "advanced"; key: AdvancedFilterModalKey };

const SIDEBAR_PANEL_TITLES: Record<SidebarPanelKey, string> = {
  search: "絞り込み",
  list: "リスト",
  csv: "CSV取込",
  inspection: "精査",
  theme: "テーマ",
};

const SIDEBAR_MENU_ITEMS: { key: SidebarPanelKey; label: string }[] = [
  { key: "search", label: "検索" },
  { key: "list", label: "リスト" },
  { key: "csv", label: "CSV" },
  { key: "inspection", label: "精査" },
  { key: "theme", label: "テーマ" },
];

function SidebarMenuIcon({
  menuKey,
  className = "h-5 w-5",
}: {
  menuKey: SidebarPanelKey;
  className?: string;
}) {
  if (menuKey === "search") {
    return (
      <svg
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
        className={className}
      >
        <circle cx="11" cy="11" r="6.5" />
        <path d="M16 16L21 21" />
      </svg>
    );
  }

    if (menuKey === "list") {
    return (
      <svg
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
        className={className}
      >
        <circle cx="5" cy="6" r="1.2" />
        <circle cx="5" cy="12" r="1.2" />
        <circle cx="5" cy="18" r="1.2" />
        <path d="M9 6h10" />
        <path d="M9 12h10" />
        <path d="M9 18h10" />
      </svg>
    );
  }

  if (menuKey === "csv") {
    return (
      <svg
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
        className={className}
      >
        <path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z" />
        <path d="M14 3v5h5" />
        <path d="M8 13h8" />
        <path d="M8 17h5" />
      </svg>
    );
  }

  if (menuKey === "theme") {
    return (
      <svg
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
        className={className}
      >
        <path d="M12 3v2.5" />
        <path d="M12 18.5V21" />
        <path d="M3 12h2.5" />
        <path d="M18.5 12H21" />
        <path d="M5.6 5.6l1.8 1.8" />
        <path d="M16.6 16.6l1.8 1.8" />
        <path d="M18.4 5.6l-1.8 1.8" />
        <path d="M7.4 16.6l-1.8 1.8" />
        <circle cx="12" cy="12" r="4.5" />
      </svg>
    );
  }

  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      className={className}
    >
      <path d="M4 6h16" />
      <path d="M4 12h16" />
      <path d="M4 18h10" />
      <circle cx="18" cy="18" r="2" />
    </svg>
  );
}

function MasterDataBrandLogo({
  className = "",
}: {
  className?: string;
}) {
  return (
    <svg
      viewBox="0 0 520 280"
      className={`master-data-brand-logo ${className}`}
      role="img"
      aria-label="MASTER DATA BARI SPECIAL"
    >
      <defs>
        <linearGradient
          id="masterDataBrandGold"
          x1="8%"
          y1="0%"
          x2="92%"
          y2="100%"
        >
          <stop
            offset="0%"
            className="master-data-brand-logo__gold-stop-1"
          />
          <stop
            offset="46%"
            className="master-data-brand-logo__gold-stop-2"
          />
          <stop
            offset="100%"
            className="master-data-brand-logo__gold-stop-3"
          />
        </linearGradient>
      </defs>

      <text
        x="150"
        y="140"
        textAnchor="middle"
        className="master-data-brand-logo__display"
        fill="url(#masterDataBrandGold)"
        fontSize="156"
        fontWeight="600"
        fontStyle="italic"
        letterSpacing="-4"
      >
        M
      </text>

      <text
        x="282"
        y="138"
        textAnchor="middle"
        className="master-data-brand-logo__display-alt"
        fill="url(#masterDataBrandGold)"
        fontSize="122"
        fontWeight="600"
        letterSpacing="0"
      >
        D
      </text>

      <text
        x="374"
        y="138"
        textAnchor="middle"
        className="master-data-brand-logo__display-alt"
        fill="url(#masterDataBrandGold)"
        fontSize="122"
        fontWeight="600"
        letterSpacing="0"
      >
        B
      </text>

      <text
        x="260"
        y="198"
        textAnchor="middle"
        className="master-data-brand-logo__label master-data-brand-logo__wordmark"
        fontSize="35"
        fontWeight="700"
        letterSpacing="5"
      >
        MASTER DATA
      </text>

      <text
        x="260"
        y="229"
        textAnchor="middle"
        className="master-data-brand-logo__sub master-data-brand-logo__wordmark"
        fontSize="20"
        fontWeight="700"
        letterSpacing="3.2"
      >
        BARI SPECIAL
      </text>
    </svg>
  );
}

function createInitialAdvancedFiltersState(): AdvancedFiltersState {
  return {
    companyName: {
      keyword: "",
    },
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
    companyName: {
      keyword: state.companyName.keyword,
    },
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

function buildRequestAdvancedFilters(
  state: AdvancedFiltersState,
  options?: AdvancedValueOptions
) {
  const result: Record<string, unknown> = {};

  const companyKeyword = state.companyName.keyword.trim();
  if (companyKeyword !== "") {
    result.companyName = {
      keyword: companyKeyword,
    };
  }

  const selectedPrefectures = Array.from(
    new Set(state.prefectures.prefectures)
  );
  const selectedCities = Array.from(new Set(state.prefectures.cities));

  const coveredCitySet = new Set(
    (options?.prefectureItems ?? [])
      .filter((item) => selectedPrefectures.includes(item.prefecture))
      .flatMap((item) => item.cities)
  );

  const compactCities =
    coveredCitySet.size === 0
      ? selectedCities
      : selectedCities.filter((city) => !coveredCitySet.has(city));

  if (selectedPrefectures.length > 0 || compactCities.length > 0) {
    result.prefectures = {
      regions: [],
      prefectures: selectedPrefectures,
      cities: compactCities,
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
    case "companyName":
      return state.companyName.keyword.trim() !== "";
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
  children: ReactNode;
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

function isPreviewUrlValue(changeKey: string, value: string | null) {
  if (!value) return false;
  return (
    changeKey === "website_url" ||
    changeKey === "form_url" ||
    /^https?:\/\//i.test(value)
  );
}

function PreviewChangeValue({
  changeKey,
  value,
}: {
  changeKey: string;
  value: string | null;
}) {
  if (!value || value.trim() === "") {
    return <span className="text-slate-500">-</span>;
  }

  if (isPreviewUrlValue(changeKey, value)) {
    return (
      <a
        href={value}
        target="_blank"
        rel="noreferrer"
        className="break-all text-sky-300 underline underline-offset-2 transition hover:text-sky-200"
        title={value}
      >
        {value}
      </a>
    );
  }

  return <span className="whitespace-pre-wrap break-words">{value}</span>;
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
  onLoadPreviousValues,
  onLoadMoreValues,
  onOpenClearConfirm,
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
  onLoadPreviousValues: (key: FilterKey) => void;
  onLoadMoreValues: (key: FilterKey) => void;
  onOpenClearConfirm: (key: FilterKey) => void;
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

  const totalItemCount = filterState.totalItemCount;
  const checkedItemCount = filterState.checkedItemCount;
  const hasPreviousValues = filterState.valueOffset > 0;
  const currentPage =
    Math.floor(filterState.valueOffset / Math.max(filterState.valueLimit, 1)) + 1;
  const totalPages = Math.max(
    1,
    Math.ceil(
      filterState.availableValueTotal / Math.max(filterState.valueLimit, 1)
    )
  );

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
                className="flex h-[calc(100dvh-32px)] w-full max-w-[420px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                onClick={(e) => e.stopPropagation()}
              >
                <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                  <div className="text-sm font-semibold text-slate-100">
                    {label} のフィルタ
                  </div>

                  <div className="flex items-center gap-2">
                    <button
                      type="button"
                      onClick={() => onOpenClearConfirm(filterKey)}
                      className="inline-flex h-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 px-3 text-xs text-slate-300 transition hover:bg-white/10"
                    >
                      フィルタ解除
                    </button>

                    <button
                      type="button"
                      onClick={() => onToggleOpen(filterKey)}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>
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

                    <div className="sticky top-[-16px] z-20 mb-2 space-y-2 bg-[#0b1220]/95 pb-2 backdrop-blur-sm">
                      <div className="grid grid-cols-2 gap-2">
                        <button
                          type="button"
                          onClick={() => onSelectAllVisible(filterKey)}
                          className="h-8 rounded-lg border border-white/10 bg-white/5 px-2 text-xs text-slate-200 transition hover:bg-white/10"
                        >
                          すべて選択
                        </button>
                        <button
                          type="button"
                          onClick={() => onClearVisible(filterKey)}
                          className="h-8 rounded-lg border border-white/10 bg-white/5 px-2 text-xs text-slate-200 transition hover:bg-white/10"
                        >
                          すべて解除
                        </button>
                      </div>

                      <div className="rounded-lg border border-white/10 bg-[#0b1220]/95 p-2">
                        <div className="mb-2 flex items-center justify-between gap-3">
                          <div className="text-xs font-semibold text-slate-300">
                            項目数
                          </div>

                          <div className="flex items-center gap-2">
                            <button
                              type="button"
                              onClick={() => onLoadPreviousValues(filterKey)}
                              disabled={filterState.valueLoading || !hasPreviousValues}
                              className="inline-flex h-7 w-7 items-center justify-center rounded-md border border-white/10 bg-white/5 text-xs text-slate-200 transition hover:bg-white/10 disabled:opacity-40"
                            >
                              ◀
                            </button>

                            <div className="min-w-[56px] text-center text-xs font-semibold text-slate-200">
                              {currentPage}/{totalPages}
                            </div>

                            <button
                              type="button"
                              onClick={() => onLoadMoreValues(filterKey)}
                              disabled={filterState.valueLoading || !filterState.hasMoreValues}
                              className="inline-flex h-7 w-7 items-center justify-center rounded-md border border-white/10 bg-white/5 text-xs text-slate-200 transition hover:bg-white/10 disabled:opacity-40"
                            >
                              ▶
                            </button>
                          </div>
                        </div>

                        <div className="grid grid-cols-2 gap-2">
                          <div className="rounded-lg border border-white/10 bg-white/5 px-3 py-2">
                            <div className="flex items-center justify-between gap-2 text-xs">
                              <span className="text-left text-slate-300">全体</span>
                              <span className="text-right text-sm font-bold text-slate-100">
                                {totalItemCount.toLocaleString()}件
                              </span>
                            </div>
                          </div>

                          <div className="rounded-lg border border-white/10 bg-white/5 px-3 py-2">
                            <div className="flex items-center justify-between gap-2 text-xs">
                              <span className="text-left text-slate-300">チェック</span>
                              <span className="text-right text-sm font-bold text-slate-100">
                                {checkedItemCount.toLocaleString()}件
                              </span>
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>

                    <div className="rounded-xl border border-white/10 bg-[#0f172a] p-2">
                      {visibleValues.length === 0 ? (
                        <div className="px-2 py-6 text-center text-xs text-slate-500">
                          {filterState.valueLoading ? "読み込み中です..." : "候補がありません"}
                        </div>
                      ) : (
                        <List
                          defaultHeight={440}
                          rowComponent={FilterValueRow}
                          rowCount={visibleValues.length}
                          rowHeight={44}
                          rowProps={{
                            visibleValues,
                            filterKey,
                            filterState,
                            onToggleValue,
                          }}
                          style={{ width: "100%" }}
                        />
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

function FilterValueRow({
  index,
  style,
  visibleValues,
  filterKey,
  filterState,
  onToggleValue,
}: RowComponentProps<{
  visibleValues: string[];
  filterKey: FilterKey;
  filterState: ColumnFilterState;
  onToggleValue: (key: FilterKey, value: string) => void;
}>) {
  const value = visibleValues[index];
  const checked =
    !filterState.valueFilterEnabled ||
    filterState.selectedValues.includes(value);

  return (
    <div style={style} className="px-2">
      <label
        className="flex h-full cursor-pointer items-center gap-2 rounded-lg px-2 py-2 text-sm text-slate-100 hover:bg-white/5"
      >
        <input
          type="checkbox"
          checked={checked}
          onChange={() => onToggleValue(filterKey, value)}
          className="h-4 w-4 accent-sky-500"
        />
        <span className="min-w-0 flex-1 truncate">
          {value === "" ? "(空白)" : value}
        </span>
        <span className="shrink-0 text-sm font-bold text-slate-200">
          {(filterState.valueCounts[value] ?? 0).toLocaleString()}
        </span>
      </label>
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
        <Cell title={row.tag || ""}><EmptyValue value={row.tag} /></Cell>
        <Cell title={row.business_type || ""}><EmptyValue value={row.business_type} /></Cell>
        <Cell title={row.business_content || ""} className="whitespace-pre-wrap"><EmptyValue value={row.business_content} /></Cell>
        <Cell title={row.industry_category || ""}><EmptyValue value={row.industry_category} /></Cell>
        <Cell title={row.memo || ""} className="whitespace-pre-wrap"><EmptyValue value={row.memo} /></Cell>
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

  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [checkedImportFiles, setCheckedImportFiles] = useState<
    Record<string, boolean>
  >({});
  const [importing, setImporting] = useState(false);
  const [importMessage, setImportMessage] = useState("");
  const [importError, setImportError] = useState("");

  const [crawlConfirmOpen, setCrawlConfirmOpen] = useState(false);
  const [crawlFieldSelections, setCrawlFieldSelections] = useState<
    Record<CrawlFieldKey, boolean>
  >(() => createInitialCrawlFieldSelections());
  const [crawling, setCrawling] = useState(false);
  const [crawlMessage, setCrawlMessage] = useState("");
  const [crawlError, setCrawlError] = useState("");

  const [crawlScopeOpen, setCrawlScopeOpen] = useState(false);
  const [crawlTargetScope, setCrawlTargetScope] = useState<
    "filtered" | "all" | null
  >(null);

  const [crawlJobId, setCrawlJobId] = useState<string | null>(null);
  const [crawlJobStatus, setCrawlJobStatus] =
    useState<CrawlJobStatus>("idle");
  const [crawlProgressOpen, setCrawlProgressOpen] = useState(false);
  const [crawlTotalTargets, setCrawlTotalTargets] = useState(0);
  const [crawlProcessedCount, setCrawlProcessedCount] = useState(0);
  const [crawlUpdatedCount, setCrawlUpdatedCount] = useState(0);
  const [crawlSkippedCount, setCrawlSkippedCount] = useState(0);
  const [crawlFailedCount, setCrawlFailedCount] = useState(0);
  const [crawlCurrentCompany, setCrawlCurrentCompany] = useState<string | null>(null);
  const [crawlCurrentWebsiteUrl, setCrawlCurrentWebsiteUrl] = useState<string | null>(null);
  const [crawlCurrentFields, setCrawlCurrentFields] = useState<string[]>([]);
  const [crawlRemainingCount, setCrawlRemainingCount] = useState(0);
  const crawlStatusTimerRef = useRef<number | null>(null);

  const [crawlElapsedMs, setCrawlElapsedMs] = useState(0);
  const crawlStartedAtRef = useRef<number | null>(null);
  const crawlElapsedBaseMsRef = useRef(0);
  const crawlElapsedTimerRef = useRef<number | null>(null);

  const [crawlPreviewOpen, setCrawlPreviewOpen] = useState(false);
  const [crawlPreviewRows, setCrawlPreviewRows] = useState<CrawlPreviewRow[]>([]);
  const [crawlPreviewPage, setCrawlPreviewPage] = useState(1);
  const [crawlPreviewTotalCount, setCrawlPreviewTotalCount] = useState(0);
  const [crawlPreviewLoading, setCrawlPreviewLoading] = useState(false);
  const [crawlSelectedChanges, setCrawlSelectedChanges] =
    useState<CrawlSelectedChanges>({});
  const [crawlResumeConfirmOpen, setCrawlResumeConfirmOpen] = useState(false);

  const crawlPreviewTotalPages = useMemo(
    () =>
      Math.max(
        1,
        Math.ceil(crawlPreviewTotalCount / CRAWL_PREVIEW_PAGE_SIZE)
      ),
    [crawlPreviewTotalCount]
  );

  const [listDeleteScopeOpen, setListDeleteScopeOpen] = useState(false);

  const [listDeleteConfirmTarget, setListDeleteConfirmTarget] = useState<
    "filtered" | "all" | null
  >(null);

  const [listDeleting, setListDeleting] = useState(false);
  const [listDeleteMessage, setListDeleteMessage] = useState("");
  const [listDeleteError, setListDeleteError] = useState("");

  const [itemDeleteScopeOpen, setItemDeleteScopeOpen] = useState(false);
  const [itemDeleteFieldOpen, setItemDeleteFieldOpen] = useState(false);
  const [itemDeleteConfirmOpen, setItemDeleteConfirmOpen] = useState(false);

  const [itemDeleteTarget, setItemDeleteTarget] = useState<
    "filtered" | "all" | null
  >(null);

  const [itemDeleteSelections, setItemDeleteSelections] = useState<
    Record<FilterKey, boolean>
  >(() => createInitialItemDeleteSelections());

  const [itemDeleting, setItemDeleting] = useState(false);
  const [itemDeleteMessage, setItemDeleteMessage] = useState("");
  const [itemDeleteError, setItemDeleteError] = useState("");

  const [itemInspectionFieldOpen, setItemInspectionFieldOpen] = useState(false);
  const [itemInspectionMethodOpen, setItemInspectionMethodOpen] = useState(false);

  const [itemInspectionPreviewConfirmOpen, setItemInspectionPreviewConfirmOpen] =
    useState(false);

  const [itemInspectionPreviewChanges, setItemInspectionPreviewChanges] =
    useState<ItemInspectionPreviewChange[]>([]);
    
  const [itemInspectionCheckedPreviewRowIds, setItemInspectionCheckedPreviewRowIds] =
    useState<Record<string, boolean>>({});

  const [itemInspectionPreviewPage, setItemInspectionPreviewPage] = useState(1);

  const itemInspectionPreviewTotalPages = useMemo(
    () =>
      Math.max(
        1,
        Math.ceil(
          itemInspectionPreviewChanges.length /
            ITEM_INSPECTION_PREVIEW_PAGE_SIZE
        )
      ),
    [itemInspectionPreviewChanges.length]
  );

  const pagedItemInspectionPreviewChanges = useMemo(() => {
    const start =
      (itemInspectionPreviewPage - 1) * ITEM_INSPECTION_PREVIEW_PAGE_SIZE;

    return itemInspectionPreviewChanges.slice(
      start,
      start + ITEM_INSPECTION_PREVIEW_PAGE_SIZE
    );
  }, [itemInspectionPreviewChanges, itemInspectionPreviewPage]);

  const [itemInspectionSelections, setItemInspectionSelections] = useState<
    Record<FilterKey, boolean>
  >(() => createInitialItemDeleteSelections());

  const [itemInspectionMethodSelections, setItemInspectionMethodSelections] =
    useState<Record<ItemInspectionMethodKey, boolean>>(
      () => createInitialItemInspectionMethodSelections()
    );

  const [itemInspecting, setItemInspecting] = useState(false);
  const [itemInspectionMessage, setItemInspectionMessage] = useState("");
  const [itemInspectionError, setItemInspectionError] = useState("");

  const [itemInspectionScopeOpen, setItemInspectionScopeOpen] =
    useState(false);
  const [itemInspectionTargetScope, setItemInspectionTargetScope] = useState<
    "filtered" | "all" | null
  >(null);

  const [itemInspectionJobId, setItemInspectionJobId] =
    useState<string | null>(null);
  const [itemInspectionJobStatus, setItemInspectionJobStatus] =
    useState<ItemInspectionJobStatus>("idle");
  const [itemInspectionProgressOpen, setItemInspectionProgressOpen] =
    useState(false);
  const [itemInspectionTotalTargets, setItemInspectionTotalTargets] =
    useState(0);
  const [itemInspectionProcessedCount, setItemInspectionProcessedCount] =
    useState(0);
  const [itemInspectionUpdatedCount, setItemInspectionUpdatedCount] =
    useState(0);
  const [itemInspectionSkippedCount, setItemInspectionSkippedCount] =
    useState(0);
  const [itemInspectionFailedCount, setItemInspectionFailedCount] =
    useState(0);
  const [itemInspectionCurrentCompany, setItemInspectionCurrentCompany] =
    useState<string | null>(null);
  const [itemInspectionCurrentValue, setItemInspectionCurrentValue] =
    useState<string | null>(null);
  const [itemInspectionCurrentFieldLabel, setItemInspectionCurrentFieldLabel] =
    useState<string | null>(null);
  const [itemInspectionRemainingCount, setItemInspectionRemainingCount] =
    useState(0);

  const itemInspectionStatusTimerRef = useRef<number | null>(null);

  const [itemInspectionElapsedMs, setItemInspectionElapsedMs] = useState(0);
  const itemInspectionStartedAtRef = useRef<number | null>(null);
  const itemInspectionElapsedBaseMsRef = useRef(0);
  const itemInspectionElapsedTimerRef = useRef<number | null>(null);

  const [deduplicating, setDeduplicating] = useState(false);
  const [dedupeMessage, setDedupeMessage] = useState("");
  const [dedupeError, setDedupeError] = useState("");
  const [dedupeConfirmOpen, setDedupeConfirmOpen] = useState(false);

  const [dedupeScopeOpen, setDedupeScopeOpen] = useState(false);
  const [dedupeTargetScope, setDedupeTargetScope] = useState<
    "filtered" | "all" | null
  >(null);

  const [importConfirmOpen, setImportConfirmOpen] = useState(false);
  const [importDuplicateConfirmOpen, setImportDuplicateConfirmOpen] = useState(false);
  const [exportScopeOpen, setExportScopeOpen] = useState(false);
  const [exportConfirmOpen, setExportConfirmOpen] = useState(false);
  const [exportDuplicateConfirmOpen, setExportDuplicateConfirmOpen] = useState(false);
  const [exportMode, setExportMode] = useState<"all" | "filtered">("filtered");

  const [singleFilterClearConfirm, setSingleFilterClearConfirm] =
    useState<FilterClearConfirmTarget | null>(null);
  const [allFiltersClearConfirmOpen, setAllFiltersClearConfirmOpen] = useState(false);

  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [openSidebarPanel, setOpenSidebarPanel] =
    useState<SidebarPanelKey | null>(null);

  const [themeMode, setThemeMode] = useState<"dark" | "light">("dark");
  const fetchDataRequestIdRef = useRef(0);

  const buildReadRequestBody = (extra: Record<string, unknown> = {}) => {
    const body: Record<string, unknown> = {
      action: "read",
      filterModels: buildRequestFilterModels(appliedColumnStates),
      advancedFilters: buildRequestAdvancedFilters(
        appliedAdvancedFilters,
        advancedValueOptions
      ),
      ...extra,
    };

    const sortColumn = COLUMN_DEFS.find(
      (column) => appliedColumnStates[column.key].sortDirection !== ""
    );

    if (sortColumn) {
      body.sortKey = sortColumn.key;
      body.sortDirection = appliedColumnStates[sortColumn.key].sortDirection;
    }

    return body;
  };

    const fetchData = async () => {
      const requestId = ++fetchDataRequestIdRef.current;

      setLoading(true);
      setError("");

      try {
        const res = await fetch("/api/master_data", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(
            buildReadRequestBody({
              page,
              limit,
            })
          ),
          cache: "no-store",
        });

        const data = await readApiResponse(res, {
          ok: true,
          total: 0,
          totalPages: 1,
          rows: [],
        });

        if (requestId !== fetchDataRequestIdRef.current) {
          return;
        }

        if (!res.ok || !data.ok) {
          throw new Error(data.error || "データ取得に失敗しました");
        }

        setRows(data.rows || []);
        setTotal(data.total || 0);
        setTotalPages(data.totalPages || 1);
      } catch (e) {
        if (requestId !== fetchDataRequestIdRef.current) {
          return;
        }

        setRows([]);
        setTotal(0);
        setTotalPages(1);
        setError(e instanceof Error ? e.message : "不明なエラー");
      } finally {
        if (requestId === fetchDataRequestIdRef.current) {
          setLoading(false);
        }
      }
    };

    const fetchFilterValues = async (
      key: FilterKey,
      options?: {
        reset?: boolean;
        searchText?: string;
        stateOverride?: ColumnFilterState;
        offset?: number;
      }
    ) => {
      const reset = options?.reset ?? true;
      const currentState = options?.stateOverride ?? draftColumnStates[key];
      const nextSearchText = options?.searchText ?? currentState.valueSearch;
      const currentOffset =
        typeof options?.offset === "number"
          ? Math.max(0, options.offset)
          : reset
          ? 0
          : currentState.valueOffset;
      const currentLimit = currentState.valueLimit || 200;

      setDraftColumnStates((prev) => {
        const next = cloneColumnStates(prev);
        next[key].valueLoading = true;

        if (reset) {
          next[key].availableValues = [];
          next[key].valueCounts = {};
          next[key].availableValueTotal = 0;
          next[key].availableValueMatchedCount = 0;
          next[key].hasMoreValues = false;
          next[key].valueOffset = 0;
        }

        return next;
      });

      try {
        const res = await fetch("/api/master_data", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(
            buildReadRequestBody({
              valuesFor: key,
              valueSearch: nextSearchText,
              valueOffset: currentOffset,
              valueLimit: currentLimit,
              currentValueFilterEnabled: currentState.valueFilterEnabled ? "1" : "0",
              currentSelectedValues: currentState.selectedValues,
            })
          ),
          cache: "no-store",
        });

        const data = await readApiResponse(res);
        if (!res.ok || !data.ok) {
          throw new Error(data.error || "値一覧の取得に失敗しました");
        }

        setDraftColumnStates((prev) => {
          const next = cloneColumnStates(prev);
          const incomingValues = data.values || [];
          const incomingAllValues = Array.isArray(data.allValues)
            ? data.allValues
            : next[key].allValues;
          const initialSelectedValues =
            incomingAllValues.length > 0 ? incomingAllValues : incomingValues;

          next[key].availableValues = incomingValues;
          next[key].allValues = incomingAllValues;
          next[key].valueCounts = data.valueCounts || {};
          next[key].availableValueTotal = data.valueTotal || 0;
          next[key].availableValueMatchedCount = data.valueMatchedCount || 0;
          next[key].totalItemCount = data.totalItemCount || 0;
          next[key].checkedItemCount = data.checkedItemCount || 0;
          next[key].valueOffset = data.valueOffset ?? currentOffset;
          next[key].valueLimit = data.valueLimit || currentLimit;
          next[key].hasMoreValues = data.hasMoreValues ?? false;
          next[key].valueLoading = false;

          if (!currentState.valueFilterEnabled) {
            next[key].selectedValues = initialSelectedValues;
          }

          return next;
        });
      } catch (e) {
        setDraftColumnStates((prev) => {
          const next = cloneColumnStates(prev);
          next[key].valueLoading = false;
          return next;
        });
        console.error(e);
      }
    };

  const loadPreviousFilterValues = async (key: FilterKey) => {
    const state = draftColumnStates[key];

    if (state.valueLoading || state.valueOffset <= 0) {
      return;
    }

    await fetchFilterValues(key, {
      reset: false,
      searchText: state.valueSearch,
      offset: Math.max(0, state.valueOffset - state.valueLimit),
    });
  };

  const loadMoreFilterValues = async (key: FilterKey) => {
    const state = draftColumnStates[key];

    if (state.valueLoading || !state.hasMoreValues) {
      return;
    }

    await fetchFilterValues(key, {
      reset: false,
      searchText: state.valueSearch,
      offset: state.valueOffset + state.valueLimit,
    });
  };

  const fetchAdvancedFilterValues = async (key: AdvancedFilterModalKey) => {
    if (
      key === "companyName" ||
      key === "capital" ||
      key === "employeeCount"
    ) {
      return;
    }

    setAdvancedLoading(true);

    try {
      const res = await fetch("/api/master_data", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(
          buildReadRequestBody({
            advancedValuesFor: key,
          })
        ),
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

  useEffect(() => {
    if (typeof document === "undefined") return;

    document.body.setAttribute("data-app-theme", themeMode);

    return () => {
      document.body.removeAttribute("data-app-theme");
    };
  }, [themeMode]);

  const handleOpenFilter = async (key: FilterKey) => {
    if (openFilterKey === key) {
      setOpenFilterKey(null);
      return;
    }

    setOpenAdvancedFilterKey(null);

    const nextOpenState: ColumnFilterState = {
      ...appliedColumnStates[key],
      selectedValues: appliedColumnStates[key].valueFilterEnabled
        ? [...appliedColumnStates[key].selectedValues]
        : [],
      availableValues:
        draftColumnStates[key].availableValues.length > 0
          ? [...draftColumnStates[key].availableValues]
          : [...appliedColumnStates[key].availableValues],
      valueCounts:
        Object.keys(draftColumnStates[key].valueCounts).length > 0
          ? { ...draftColumnStates[key].valueCounts }
          : { ...appliedColumnStates[key].valueCounts },
      valueOffset: 0,
      valueSearch: "",
    };

    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);
      next[key] = nextOpenState;
      return next;
    });

    setOpenFilterKey(key);
    await fetchFilterValues(key, {
      reset: true,
      searchText: "",
      stateOverride: nextOpenState,
    });
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

    void fetchFilterValues(key, {
      reset: true,
      searchText: value,
    });
  };

  const toggleSelectedValue = (key: FilterKey, value: string) => {
    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);

      const baseSelectedValues = next[key].valueFilterEnabled
        ? [...next[key].selectedValues]
        : next[key].allValues.length > 0
        ? [...next[key].allValues]
        : [...next[key].availableValues];

      const currentChecked = baseSelectedValues.includes(value);

      next[key].valueFilterEnabled = true;
      next[key].selectedValues = currentChecked
        ? baseSelectedValues.filter((item) => item !== value)
        : [...baseSelectedValues, value];

      if (value !== "") {
        const targetCount = next[key].valueCounts[value] ?? 0;
        next[key].checkedItemCount = Math.max(
          0,
          Math.min(
            next[key].totalItemCount,
            next[key].checkedItemCount +
              (currentChecked ? -targetCount : targetCount)
          )
        );
      }

      return next;
    });
  };

  const selectAllVisibleValues = (key: FilterKey) => {
    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);

      const allValues =
        next[key].allValues.length > 0
          ? next[key].allValues
          : next[key].selectedValues.length > 0
          ? next[key].selectedValues
          : [...next[key].availableValues];

      next[key].valueFilterEnabled = true;
      next[key].selectedValues = [...allValues];
      next[key].checkedItemCount = next[key].totalItemCount;

      return next;
    });
  };

  const clearVisibleValues = (key: FilterKey) => {
    setDraftColumnStates((prev) => {
      const next = cloneColumnStates(prev);

      next[key].valueFilterEnabled = true;
      next[key].selectedValues = [];
      next[key].checkedItemCount = 0;

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

      setOpenSidebarPanel(null);
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

    const handleOpenSidebarPanel = (key: SidebarPanelKey) => {
      setOpenFilterKey(null);
      setOpenAdvancedFilterKey(null);

      const nextPanel = openSidebarPanel === key ? null : key;
      setOpenSidebarPanel(nextPanel);

      if (nextPanel === "list") {
        setListDeleteScopeOpen(false);
        setListDeleteError("");
        setListDeleteMessage("");

        setItemDeleteScopeOpen(false);
        setItemDeleteFieldOpen(false);
        setItemDeleteConfirmOpen(false);
        setItemDeleteTarget(null);
        setItemDeleteError("");
        setItemDeleteMessage("");
      } else {
        setListDeleteScopeOpen(false);

        setItemDeleteScopeOpen(false);
        setItemDeleteFieldOpen(false);
        setItemDeleteConfirmOpen(false);
        setItemDeleteTarget(null);
      }
    };

    const clearAdvancedFilter = (key: AdvancedFilterModalKey) => {
      const empty = createInitialAdvancedFiltersState();

      setDraftAdvancedFilters((prev) => {
        const next = cloneAdvancedFiltersState(prev);

        if (key === "companyName") next.companyName = empty.companyName;
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

        if (key === "companyName") next.companyName = empty.companyName;
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

    const clearAllFilters = () => {
      setDraftColumnStates((prev) => createClearedColumnStates(prev));
      setAppliedColumnStates((prev) => createClearedColumnStates(prev));
      setDraftAdvancedFilters(createInitialAdvancedFiltersState());
      setAppliedAdvancedFilters(createInitialAdvancedFiltersState());
      setPage(1);
      setOpenFilterKey(null);
      setOpenAdvancedFilterKey(null);
      setOpenSidebarPanel(null);
    };

    const handleConfirmSingleFilterClear = () => {
      if (!singleFilterClearConfirm) return;

      if (singleFilterClearConfirm.type === "column") {
        clearColumnFilter(singleFilterClearConfirm.key);
      } else {
        clearAdvancedFilter(singleFilterClearConfirm.key);
      }

      setSingleFilterClearConfirm(null);
    };

    const handleConfirmAllFiltersClear = () => {
      clearAllFilters();
      setAllFiltersClearConfirmOpen(false);
    };

  const renderAdvancedFilterContent = () => {
    if (openAdvancedFilterKey === "companyName") {
      return (
        <div className="rounded-xl border border-white/10 bg-[#0f172a] p-4">
          <div className="mb-3 text-sm font-semibold text-slate-100">
            企業名の部分一致検索
          </div>

          <input
            value={draftAdvancedFilters.companyName.keyword}
            onChange={(e) =>
              setDraftAdvancedFilters((prev) => {
                const next = cloneAdvancedFiltersState(prev);
                next.companyName.keyword = e.target.value;
                return next;
              })
            }
            placeholder="例：株式会社"
            className="h-11 w-full rounded-xl border border-white/10 bg-[#111827] px-3 text-sm text-slate-100 outline-none placeholder:text-slate-500 focus:border-sky-500"
          />

          <div className="mt-3 text-xs text-slate-400">
            入力した文字を含む企業名のみ表示します
          </div>
        </div>
      );
    }


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
            const regionCount = regionItems.reduce(
              (sum, item) => sum + item.prefectureCount,
              0
            );
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
                    <span className="flex items-center justify-between gap-3">
                      <span>
                        {isRegionOpen ? "▼" : "▶"} {region}
                      </span>
                      <span className="text-sm font-bold text-slate-200">
                        {regionCount.toLocaleString()}
                      </span>
                    </span>
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
                              <span className="flex items-center justify-between gap-3">
                                <span>
                                  {isPrefectureOpen ? "▼" : "▶"} {item.prefecture}
                                </span>
                                <span className="text-sm font-bold text-slate-200">
                                  {item.prefectureCount.toLocaleString()}
                                </span>
                              </span>
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
                                  <span className="min-w-0 flex-1 truncate">{city}</span>
                                  <span className="shrink-0 text-sm font-bold text-slate-200">
                                    {(item.cityCounts[city] ?? 0).toLocaleString()}
                                  </span>
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
            const parentCount = parentItems.reduce(
              (sum, item) => sum + item.bigIndustryCount,
              0
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
                    <span className="flex items-center justify-between gap-3">
                      <span>
                        {isParentOpen ? "▼" : "▶"} {industryParent}
                      </span>
                      <span className="text-sm font-bold text-slate-200">
                        {parentCount.toLocaleString()}
                      </span>
                    </span>
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
                              <span className="flex items-center justify-between gap-3">
                                <span>
                                  {isOpen ? "▼" : "▶"} {item.bigIndustry}
                                </span>
                                <span className="text-sm font-bold text-slate-200">
                                  {item.bigIndustryCount.toLocaleString()}
                                </span>
                              </span>
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
                                  <span className="min-w-0 flex-1 truncate">{smallIndustry}</span>
                                  <span className="shrink-0 text-sm font-bold text-slate-200">
                                    {(item.smallIndustryCounts[smallIndustry] ?? 0).toLocaleString()}
                                  </span>
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
                    <span className="flex items-center justify-between gap-3">
                      <span>
                        {isOpen ? "▼" : "▶"} {item.parent}
                      </span>
                      <span className="text-sm font-bold text-slate-200">
                        {item.parentCount.toLocaleString()}
                      </span>
                    </span>
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
                        <span className="min-w-0 flex-1 truncate">{tag}</span>
                        <span className="shrink-0 text-sm font-bold text-slate-200">
                          {(item.tagCounts[tag] ?? 0).toLocaleString()}
                        </span>
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

  const renderSidebarPanelContent = () => {
    if (openSidebarPanel === "search") {
      return (
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          {ADVANCED_FILTER_BUTTONS.map((button) => {
            const active = hasActiveAdvancedFilter(
              button.key,
              appliedAdvancedFilters
            );

            return (
              <button
                key={button.key}
                type="button"
                onClick={() => {
                  setOpenSidebarPanel(null);
                  handleOpenAdvancedFilter(button.key);
                }}
                className={`h-11 rounded-xl border px-3 text-sm font-medium transition ${
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
      );
    }

    if (openSidebarPanel === "list") {
      return (
        <div className="space-y-4">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
            <button
              type="button"
              onClick={() => setListDeleteScopeOpen(true)}
              disabled={listDeleting}
              className="h-11 rounded-xl bg-rose-600 px-5 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
            >
              リスト削除
            </button>

            <button
              type="button"
              onClick={() => {
                setItemDeleteSelections(createEmptyItemDeleteSelections());
                setItemDeleteTarget(null);
                setItemDeleteError("");
                setItemDeleteMessage("");
                setItemDeleteScopeOpen(true);
              }}
              disabled={itemDeleting}
              className="h-11 rounded-xl bg-cyan-600 px-5 text-sm font-medium text-white transition hover:bg-cyan-500 disabled:opacity-50"
            >
              項目削除
            </button>

            <button
              type="button"
              onClick={() => {
                setDedupeTargetScope(null);
                setDedupeScopeOpen(true);
              }}
              disabled={deduplicating}
              className="h-11 rounded-xl bg-violet-600 px-5 text-sm font-medium text-white transition hover:bg-violet-500 disabled:opacity-50"
            >
              {deduplicating ? "重複削除中..." : "重複削除"}
            </button>
          </div>

          {listDeleteMessage && (
            <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200">
              {listDeleteMessage}
            </div>
          )}

          {listDeleteError && (
            <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
              {listDeleteError}
            </div>
          )}

          {itemDeleteMessage && (
            <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200">
              {itemDeleteMessage}
            </div>
          )}

          {itemDeleteError && (
            <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
              {itemDeleteError}
            </div>
          )}

          {dedupeMessage && (
            <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200">
              {dedupeMessage}
            </div>
          )}

          {dedupeError && (
            <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
              {dedupeError}
            </div>
          )}
        </div>
      );
    }

    if (openSidebarPanel === "csv") {
      return (
        <div className="space-y-4">
          <input
            type="file"
            accept=".csv,text/csv"
            multiple
            onChange={(e) => {
              const newFiles = Array.from(e.target.files ?? []);
              setSelectedFiles((prev) => [...prev, ...newFiles]);
              setCheckedImportFiles((prev) => ({
                ...prev,
                ...Object.fromEntries(
                  newFiles.map((file) => [buildImportFileKey(file), true])
                ),
              }));
              e.currentTarget.value = "";
            }}
            className="block w-full rounded-xl border border-white/10 bg-[#0f172a] px-4 py-3 text-sm text-slate-200 file:mr-4 file:rounded-lg file:border-0 file:bg-sky-500 file:px-4 file:py-2 file:text-sm file:font-medium file:text-white hover:file:bg-sky-400"
          />

          {selectedFiles.length > 0 && (
            <div className="space-y-2">
              {selectedFiles.map((file, index) => (
                <div
                  key={`${file.name}-${file.size}-${file.lastModified}-${index}`}
                  className="flex items-center justify-between gap-3 rounded-xl border border-white/10 bg-[#0f172a] px-4 py-3"
                >
                  <span
                    className="min-w-0 flex-1 truncate text-sm text-slate-200"
                    title={file.name}
                  >
                    {file.name}
                  </span>

                  <button
                    type="button"
                    onClick={() => {
                      const fileKey = buildImportFileKey(file);

                      setSelectedFiles((prev) =>
                        prev.filter((_, fileIndex) => fileIndex !== index)
                      );

                      setCheckedImportFiles((prev) => {
                        const next = { ...prev };
                        delete next[fileKey];
                        return next;
                      });
                    }}
                    className="inline-flex h-7 w-7 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                  >
                    ×
                  </button>
                </div>
              ))}
            </div>
          )}

          <div className="flex flex-col items-center justify-center gap-3 sm:flex-row sm:flex-wrap">
            <button
              onClick={handleImportClick}
              disabled={importing}
              className="h-11 min-w-[160px] rounded-xl bg-emerald-500 px-5 text-sm font-medium text-white transition hover:bg-emerald-400 disabled:opacity-50"
            >
              {importing ? "投入中..." : "CSVを投入"}
            </button>

            <button
              onClick={handleExportClick}
              className="h-11 min-w-[160px] rounded-xl bg-sky-500 px-5 text-sm font-medium text-white transition hover:bg-sky-400"
            >
              CSVを抽出
            </button>

            <button
              onClick={handleDownloadTemplate}
              className="h-11 min-w-[160px] rounded-xl bg-indigo-500 px-5 text-sm font-medium text-white transition hover:bg-indigo-400"
            >
              CSVテンプレート
            </button>
          </div>

          {importMessage && (
            <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200">
              {importMessage}
            </div>
          )}

          {importError && (
            <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
              {importError}
            </div>
          )}
        </div>
      );
    }

    if (openSidebarPanel === "inspection") {
      return (
        <div className="space-y-4">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <button
              type="button"
              onClick={() => {
                setCrawlFieldSelections(createEmptyCrawlFieldSelections());
                setCrawlTargetScope(null);
                setCrawlScopeOpen(true);
              }}
              disabled={crawling}
              className="h-11 rounded-xl bg-amber-600 px-5 text-sm font-medium text-white transition hover:bg-amber-500 disabled:opacity-50"
            >
              {crawling ? "クローリング中..." : "クローリング"}
            </button>

            <button
              type="button"
              onClick={() => {
                setItemInspectionSelections(createEmptyItemDeleteSelections());
                setItemInspectionMethodSelections(
                  createEmptyItemInspectionMethodSelections()
                );
                setItemInspectionPreviewChanges([]);
                setItemInspectionCheckedPreviewRowIds({});
                setItemInspectionMessage("");
                setItemInspectionError("");
                setItemInspectionMethodOpen(false);
                setItemInspectionPreviewConfirmOpen(false);
                setItemInspectionTargetScope(null);
                setItemInspectionScopeOpen(true);
              }}
              disabled={itemInspecting}
              className="h-11 rounded-xl bg-cyan-600 px-5 text-sm font-medium text-white transition hover:bg-cyan-500 disabled:opacity-50"
            >
              {itemInspecting ? "項目精査中..." : "項目精査"}
            </button>
          </div>

          {crawlMessage && (
            <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200">
              {crawlMessage}
            </div>
          )}

          {crawlError && (
            <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
              {crawlError}
            </div>
          )}

          {itemInspectionMessage && (
            <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200">
              {itemInspectionMessage}
            </div>
          )}

          {itemInspectionError && (
            <div className="rounded-xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
              {itemInspectionError}
            </div>
          )}
        </div>
      );
    }

    if (openSidebarPanel === "theme") {
      return (
        <div className="space-y-3">
          <button
            type="button"
            onClick={() => {
              setThemeMode("light");
              setOpenSidebarPanel(null);
            }}
            className={`h-11 w-full rounded-xl border px-3 text-sm font-medium transition ${
              themeMode === "light"
                ? "border-sky-400/40 bg-sky-500/20 text-sky-100 hover:bg-sky-500/30"
                : "border-white/10 bg-[#0f172a] text-slate-200 hover:bg-white/10"
            }`}
          >
            ライト
          </button>

          <button
            type="button"
            onClick={() => {
              setThemeMode("dark");
              setOpenSidebarPanel(null);
            }}
            className={`h-11 w-full rounded-xl border px-3 text-sm font-medium transition ${
              themeMode === "dark"
                ? "border-sky-400/40 bg-sky-500/20 text-sky-100 hover:bg-sky-500/30"
                : "border-white/10 bg-[#0f172a] text-slate-200 hover:bg-white/10"
            }`}
          >
            ダーク
          </button>
        </div>
      );
    }

    return null;
  };

const handleExportClick = () => {
  setImportError("");
  setImportMessage("");
  setExportConfirmOpen(false);
  setExportDuplicateConfirmOpen(false);
  setExportScopeOpen(true);
};

const handleImportClick = () => {
  if (selectedFiles.length === 0) {
    setImportError("CSVファイルを選択してください");
    setImportMessage("");
    return;
  }

  setImportDuplicateConfirmOpen(false);
  setImportConfirmOpen(true);
};

const handleImport = async (shouldDeduplicate: boolean) => {
  if (selectedFiles.length === 0) {
    setImportError("CSVファイルを選択してください");
    setImportMessage("");
    return;
  }

  const checkedFiles = selectedFiles.filter(
    (file) => checkedImportFiles[buildImportFileKey(file)] !== false
  );

  if (checkedFiles.length === 0) {
    setImportError("投入するCSVを1つ以上チェックしてください");
    setImportMessage("");
    return;
  }

  setImporting(true);
  setImportConfirmOpen(false);
  setImportDuplicateConfirmOpen(false);
  setImportError("");
  setImportMessage("");

  try {
    const formData = new FormData();

    checkedFiles.forEach((file) => {
      formData.append("files", file);
    });

    formData.append("skipDuplicateCheck", shouldDeduplicate ? "0" : "1");

    const res = await fetch("/api/master_data", {
      method: "POST",
      body: formData,
    });

    const data = await readApiResponse(res);

    if (!res.ok || !data.ok) {
      throw new Error(data.error || "CSV取込に失敗しました");
    }

    setImportMessage(data.message || "CSVを取り込みました");
    setSelectedFiles([]);
    setCheckedImportFiles({});
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

const handleExport = async (shouldDeduplicate: boolean) => {
  setImportError("");
  setImportMessage("");
  setExportConfirmOpen(false);
  setExportDuplicateConfirmOpen(false);

  try {
    const showSaveFilePicker = (
      window as Window & {
        showSaveFilePicker?: (options?: {
          suggestedName?: string;
          types?: Array<{
            description?: string;
            accept: Record<string, string[]>;
          }>;
        }) => Promise<{
          createWritable: () => Promise<{
            write: (data: Blob) => Promise<void>;
            close: () => Promise<void>;
          }>;
        }>;
      }
    ).showSaveFilePicker;

    if (!showSaveFilePicker) {
      throw new Error("この環境では保存場所の選択に対応していません");
    }

    const tempFileName = "master_data.csv";

    const fileHandle = await showSaveFilePicker({
      suggestedName: tempFileName,
      types: [
        {
          description: "CSVファイル",
          accept: {
            "text/csv": [".csv"],
          },
        },
      ],
    });

    const params = new URLSearchParams();
    params.set("exportScope", exportMode);
    params.set("dedupeByCompany", shouldDeduplicate ? "1" : "0");

    if (exportMode === "filtered") {
      params.set(
        "filterModels",
        JSON.stringify(buildRequestFilterModels(appliedColumnStates))
      );
      params.set(
        "advancedFilters",
        JSON.stringify(
          buildRequestAdvancedFilters(
            appliedAdvancedFilters,
            advancedValueOptions
          )
        )
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
    }

    const res = await fetch(`/api/master_data/export?${params.toString()}`, {
      cache: "no-store",
    });

    if (!res.ok) {
      const data = await readApiResponse(res);
      throw new Error(data.error || "CSV抽出に失敗しました");
    }

    const blob = await res.blob();
    const writable = await fileHandle.createWritable();
    await writable.write(blob);
    await writable.close();

    setImportMessage("CSVを保存しました");
  } catch (e) {
    if (e instanceof DOMException && e.name === "AbortError") {
      return;
    }

    setImportError(
      e instanceof Error ? e.message : "CSV抽出に失敗しました"
    );
  }
};

const handleDownloadTemplate = async () => {
  setImportError("");
  setImportMessage("");

  try {
    const headerLine = CSV_TEMPLATE_HEADERS.map((value) =>
      `"${String(value).replace(/"/g, `""`)}"`
    ).join(",");

    const csv = "\uFEFF" + headerLine + "\r\n";
    const csvBlob = new Blob([csv], { type: "text/csv;charset=utf-8;" });

    const showSaveFilePicker = (
      window as Window & {
        showSaveFilePicker?: (options?: {
          suggestedName?: string;
          types?: Array<{
            description?: string;
            accept: Record<string, string[]>;
          }>;
        }) => Promise<{
          createWritable: () => Promise<{
            write: (data: Blob) => Promise<void>;
            close: () => Promise<void>;
          }>;
        }>;
      }
    ).showSaveFilePicker;

    if (!showSaveFilePicker) {
      throw new Error(
        "この環境では保存場所の選択に対応していません"
      );
    }

    const fileHandle = await showSaveFilePicker({
      suggestedName: "CSVテンプレート_マスタデータ.csv",
      types: [
        {
          description: "CSVファイル",
          accept: {
            "text/csv": [".csv"],
          },
        },
      ],
    });

    const writable = await fileHandle.createWritable();
    await writable.write(csvBlob);
    await writable.close();

    setImportMessage("CSVテンプレートを保存しました");
  } catch (e) {
    if (e instanceof DOMException && e.name === "AbortError") {
      return;
    }

    setImportError(
      e instanceof Error
        ? e.message
        : "CSVテンプレートの保存に失敗しました"
    );
  }
};

  const getDefaultCrawlPreviewValue = (change: CrawlPreviewChange) => {
    return change.candidates.length > 1
      ? null
      : change.after ?? change.candidates[0] ?? null;
  };

  const getCrawlPreviewSelectedValue = (
    overrides: CrawlSelectedChanges,
    previewRowId: string,
    change: CrawlPreviewChange
  ) => {
    const rowOverride = overrides[previewRowId];

    if (
      rowOverride &&
      Object.prototype.hasOwnProperty.call(rowOverride, change.key)
    ) {
      return rowOverride[change.key] ?? null;
    }

    return getDefaultCrawlPreviewValue(change);
  };

  const toggleCrawlPreviewReflect = (
    previewRowId: string,
    change: CrawlPreviewChange,
    fallbackValue: string | null
  ) => {
    const defaultValue = getDefaultCrawlPreviewValue(change);
    const targetValue = fallbackValue ?? defaultValue;

    setCrawlSelectedChanges((prev) => {
      const currentEffectiveValue = getCrawlPreviewSelectedValue(
        prev,
        previewRowId,
        change
      );
      const nextEffectiveValue =
        currentEffectiveValue !== null ? null : targetValue;

      const currentRow = { ...(prev[previewRowId] ?? {}) };
      const next = { ...prev };

      if (nextEffectiveValue === defaultValue) {
        delete currentRow[change.key];
      } else {
        currentRow[change.key] = nextEffectiveValue;
      }

      if (Object.keys(currentRow).length === 0) {
        delete next[previewRowId];
      } else {
        next[previewRowId] = currentRow;
      }

      return next;
    });
  };

  const toggleCrawlPreviewCandidate = (
    previewRowId: string,
    change: CrawlPreviewChange,
    candidate: string
  ) => {
    const defaultValue = getDefaultCrawlPreviewValue(change);

    setCrawlSelectedChanges((prev) => {
      const currentEffectiveValue = getCrawlPreviewSelectedValue(
        prev,
        previewRowId,
        change
      );
      const nextEffectiveValue =
        currentEffectiveValue === candidate ? defaultValue : candidate;

      const currentRow = { ...(prev[previewRowId] ?? {}) };
      const next = { ...prev };

      if (nextEffectiveValue === defaultValue) {
        delete currentRow[change.key];
      } else {
        currentRow[change.key] = nextEffectiveValue;
      }

      if (Object.keys(currentRow).length === 0) {
        delete next[previewRowId];
      } else {
        next[previewRowId] = currentRow;
      }

      return next;
    });
  };

  const fetchCrawlPreviewPage = async (jobId: string, nextPage: number) => {
    setCrawlPreviewLoading(true);

    try {
      const res = await fetch("/api/master_data/crawl", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "get_job_status",
          jobId,
          previewPage: nextPage,
          previewPageSize: CRAWL_PREVIEW_PAGE_SIZE,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "クローリング結果確認の取得に失敗しました");
      }

      applyCrawlStatus(data);
      setCrawlPreviewRows(data.previewRows || []);
      setCrawlPreviewTotalCount(data.previewTotal ?? 0);
      setCrawlPreviewPage(data.previewPage ?? nextPage);
    } catch (e) {
      setCrawlError(
        e instanceof Error
          ? e.message
          : "クローリング結果確認の取得でエラーが発生しました"
      );
    } finally {
      setCrawlPreviewLoading(false);
    }
  };

  const handleCrawlPreviewPageChange = async (nextPage: number) => {
    if (!crawlJobId) return;
    if (nextPage < 1 || nextPage > crawlPreviewTotalPages) return;

    await fetchCrawlPreviewPage(crawlJobId, nextPage);
  };

  const clearCrawlElapsedTimer = () => {
    if (crawlElapsedTimerRef.current !== null) {
      window.clearInterval(crawlElapsedTimerRef.current);
      crawlElapsedTimerRef.current = null;
    }
  };

  const updateCrawlElapsedMs = () => {
    const runningMs =
      crawlStartedAtRef.current !== null
        ? Date.now() - crawlStartedAtRef.current
        : 0;

    setCrawlElapsedMs(crawlElapsedBaseMsRef.current + runningMs);
  };

  const startCrawlElapsedTracking = (reset = false) => {
    if (reset) {
      crawlElapsedBaseMsRef.current = 0;
      setCrawlElapsedMs(0);
    }

    crawlStartedAtRef.current = Date.now();
    updateCrawlElapsedMs();
    clearCrawlElapsedTimer();
    crawlElapsedTimerRef.current = window.setInterval(updateCrawlElapsedMs, 1000);
  };

  const stopCrawlElapsedTracking = () => {
    if (crawlStartedAtRef.current !== null) {
      crawlElapsedBaseMsRef.current += Date.now() - crawlStartedAtRef.current;
      crawlStartedAtRef.current = null;
    }

    updateCrawlElapsedMs();
    clearCrawlElapsedTimer();
  };

  const formatCrawlDuration = (ms: number) => {
    const safeMs = Math.max(0, ms);

    if (safeMs < 60_000) {
      const seconds = safeMs / 1000;
      return `${seconds < 10 ? seconds.toFixed(1) : Math.round(seconds)}秒`;
    }

    const totalSeconds = Math.floor(safeMs / 1000);
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    const seconds = totalSeconds % 60;

    if (hours > 0) {
      return `${hours}時間${String(minutes).padStart(2, "0")}分${String(
        seconds
      ).padStart(2, "0")}秒`;
    }

    return `${minutes}分${String(seconds).padStart(2, "0")}秒`;
  };

  const crawlAverageDivisor = Math.max(
    crawlProcessedCount,
    crawlJobStatus === "running" && crawlCurrentCompany ? 1 : 0
  );

  const averageCrawlMs =
    crawlAverageDivisor > 0 ? Math.round(crawlElapsedMs / crawlAverageDivisor) : 0;

  const clearCrawlStatusPolling = () => {
    if (crawlStatusTimerRef.current !== null) {
      window.clearInterval(crawlStatusTimerRef.current);
      crawlStatusTimerRef.current = null;
    }
  };

  const applyCrawlStatus = (data: ApiResponse) => {
    setCrawlJobStatus((data.jobStatus as CrawlJobStatus) ?? "idle");
    setCrawlTotalTargets(data.totalTargets ?? 0);
    setCrawlProcessedCount(data.processed ?? 0);
    setCrawlUpdatedCount(data.updated ?? 0);
    setCrawlSkippedCount(data.skipped ?? 0);
    setCrawlFailedCount(data.failed ?? 0);
    setCrawlCurrentCompany(data.currentCompany ?? null);
    setCrawlCurrentWebsiteUrl(data.currentWebsiteUrl ?? null);
    setCrawlCurrentFields(data.currentFields ?? []);
    setCrawlRemainingCount(data.remainingCount ?? 0);
  };

  const startCrawlStatusPolling = (jobId: string) => {
    clearCrawlStatusPolling();

    const poll = async () => {
      try {
        const res = await fetch("/api/master_data/crawl", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            action: "get_job_status",
            jobId,
            previewPage: 1,
            previewPageSize: CRAWL_PREVIEW_PAGE_SIZE,
          }),
        });

        const data = await readApiResponse(res);

        if (!res.ok || !data.ok) {
          throw new Error(data.error || "クローリング進捗の取得に失敗しました");
        }

        applyCrawlStatus(data);

        if (data.jobStatus === "paused") {
          const previewRows = data.previewRows || [];
          const previewTotal = data.previewTotal ?? 0;

          stopCrawlElapsedTracking();
          clearCrawlStatusPolling();
          setCrawling(false);
          setCrawlProgressOpen(false);
          setCrawlPreviewRows(previewRows);
          setCrawlPreviewTotalCount(previewTotal);
          setCrawlPreviewPage(data.previewPage ?? 1);
          setCrawlSelectedChanges({});
          setCrawlPreviewOpen(previewTotal > 0);
          setCrawlResumeConfirmOpen(previewTotal === 0);
          setCrawlMessage("クローリングを中止しました");
          saveActiveCrawlJobId(jobId);
          return;
        }

        if (data.jobStatus === "completed") {
          const previewRows = data.previewRows || [];
          const previewTotal = data.previewTotal ?? 0;

          stopCrawlElapsedTracking();
          clearCrawlStatusPolling();
          setCrawling(false);
          setCrawlProgressOpen(false);

          if (previewTotal === 0) {
            setCrawlPreviewRows([]);
            setCrawlPreviewTotalCount(0);
            setCrawlPreviewPage(1);
            setCrawlPreviewOpen(false);
            setCrawlMessage("保存候補はありませんでした");
            setCrawlSelectedChanges({});
            setCrawlJobId(null);
            saveActiveCrawlJobId(null);
            setCrawlJobStatus("completed");
          } else {
            setCrawlPreviewRows(previewRows);
            setCrawlPreviewTotalCount(previewTotal);
            setCrawlPreviewPage(data.previewPage ?? 1);
            setCrawlPreviewOpen(true);
            saveActiveCrawlJobId(jobId);
          }

          return;
        }

        if (data.jobStatus === "error") {
          stopCrawlElapsedTracking();
          clearCrawlStatusPolling();
          setCrawling(false);
          setCrawlProgressOpen(false);
          setCrawlError(data.error || "クローリング中にエラーが発生しました");
        }
      } catch (e) {
        stopCrawlElapsedTracking();
        clearCrawlStatusPolling();
        setCrawling(false);
        setCrawlProgressOpen(false);

        const message =
          e instanceof Error ? e.message : "クローリング進捗取得でエラーが発生しました";

        if (message.includes("クローリングジョブが見つかりません")) {
          setCrawlJobId(null);
          saveActiveCrawlJobId(null);
        }

        setCrawlError(message);
      }
    };

    void poll();
    crawlStatusTimerRef.current = window.setInterval(() => {
      void poll();
    }, 1200);
  };

  const restoreSavedCrawlJob = async () => {
    const storedJobId = loadActiveCrawlJobId();
    if (!storedJobId) return;

    try {
      const res = await fetch("/api/master_data/crawl", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "get_job_status",
          jobId: storedJobId,
          previewPage: 1,
          previewPageSize: CRAWL_PREVIEW_PAGE_SIZE,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "保存済みクローリング状態の復元に失敗しました");
      }

      setCrawlJobId(storedJobId);
      applyCrawlStatus(data);
      setCrawlMessage("");
      setCrawlError("");
      setCrawlSelectedChanges({});

      const previewRows = data.previewRows || [];
      const previewTotal = data.previewTotal ?? 0;
      const remainingCount = data.remainingCount ?? 0;

      if (data.jobStatus === "running") {
        setCrawling(true);
        setCrawlProgressOpen(true);
        setCrawlPreviewOpen(false);
        setCrawlResumeConfirmOpen(false);
        setCrawlPreviewRows([]);
        setCrawlPreviewTotalCount(0);
        setCrawlPreviewPage(1);
        startCrawlElapsedTracking(true);
        startCrawlStatusPolling(storedJobId);
        return;
      }

      stopCrawlElapsedTracking();
      clearCrawlStatusPolling();
      setCrawling(false);
      setCrawlProgressOpen(false);

      setCrawlPreviewRows(previewRows);
      setCrawlPreviewTotalCount(previewTotal);
      setCrawlPreviewPage(data.previewPage ?? 1);
      setCrawlPreviewOpen(previewTotal > 0);
      setCrawlResumeConfirmOpen(
        data.jobStatus === "paused" && previewTotal === 0 && remainingCount > 0
      );

      if (data.jobStatus === "completed" && previewTotal === 0 && remainingCount === 0) {
        setCrawlJobId(null);
        saveActiveCrawlJobId(null);
      }
    } catch {
      setCrawlJobId(null);
      saveActiveCrawlJobId(null);
    }
  };

  useEffect(() => {
    void restoreSavedCrawlJob();
  }, []);

  useEffect(() => {
    return () => {
      clearCrawlStatusPolling();
      clearCrawlElapsedTimer();
      clearItemInspectionStatusPolling();
      clearItemInspectionElapsedTimer();
    };
  }, []);

  const handleCancelCrawl = async () => {
    if (!crawlJobId) return;

    try {
      const res = await fetch("/api/master_data/crawl", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "cancel_job",
          jobId: crawlJobId,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "中止に失敗しました");
      }

      clearCrawlStatusPolling();
      stopCrawlElapsedTracking();
      setCrawling(false);
      setCrawlProgressOpen(false);
      setCrawlPreviewOpen(false);
      setCrawlResumeConfirmOpen(false);
      setCrawlPreviewRows([]);
      setCrawlPreviewTotalCount(0);
      setCrawlPreviewPage(1);
      setCrawlSelectedChanges({});
      setCrawlJobId(null);
      saveActiveCrawlJobId(null);
      setCrawlJobStatus("idle");
      setCrawlCurrentCompany(null);
      setCrawlCurrentWebsiteUrl(null);
      setCrawlCurrentFields([]);
      setCrawlRemainingCount(0);
      setCrawlMessage("クローリングを中止しました");
      setCrawlError("");
    } catch (e) {
      setCrawlError(
        e instanceof Error ? e.message : "中止処理でエラーが発生しました"
      );
    }
  };

  const handlePauseCrawl = async () => {
    if (!crawlJobId) return;

    try {
      const res = await fetch("/api/master_data/crawl", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "pause_job",
          jobId: crawlJobId,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "中断に失敗しました");
      }

      setCrawlMessage("中断指示を受け付けました");
    } catch (e) {
      setCrawlError(
        e instanceof Error ? e.message : "中断処理でエラーが発生しました"
      );
    }
  };

  const handleResumeCrawl = async () => {
    if (!crawlJobId) return;

    setCrawlResumeConfirmOpen(false);
    setCrawlPreviewOpen(false);
    setCrawlPreviewRows([]);
    setCrawlPreviewTotalCount(0);
    setCrawlMessage("");
    stopCrawlElapsedTracking();
    setCrawlError("");
    setCrawling(true);
    setCrawlProgressOpen(true);
    startCrawlElapsedTracking(false);

    try {
      const res = await fetch("/api/master_data/crawl", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "resume_job",
          jobId: crawlJobId,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "再開に失敗しました");
      }

      saveActiveCrawlJobId(crawlJobId);
      applyCrawlStatus(data);
      startCrawlStatusPolling(crawlJobId);
    } catch (e) {
      setCrawling(false);
      setCrawlProgressOpen(false);
      setCrawlError(
        e instanceof Error ? e.message : "再開処理でエラーが発生しました"
      );
    }
  };

  const handleCrawl = async () => {
    if (!crawlTargetScope) return;

    setCrawling(true);
    setCrawlConfirmOpen(false);
    setCrawlPreviewOpen(false);
    setCrawlResumeConfirmOpen(false);
    setCrawlPreviewRows([]);
    setCrawlPreviewTotalCount(0);
    setCrawlPreviewPage(1);
    setCrawlSelectedChanges({});
    setCrawlMessage("");
    stopCrawlElapsedTracking();
    setCrawlError("");
    setCrawlJobId(null);
    saveActiveCrawlJobId(null);
    setCrawlJobStatus("running");
    setCrawlTotalTargets(0);
    setCrawlProcessedCount(0);
    setCrawlUpdatedCount(0);
    setCrawlSkippedCount(0);
    setCrawlFailedCount(0);
    setCrawlCurrentCompany(null);
    setCrawlCurrentWebsiteUrl(null);
    setCrawlCurrentFields([]);
    setCrawlRemainingCount(0);
    setCrawlProgressOpen(true);
    startCrawlElapsedTracking(true);

    try {
      const body: Record<string, unknown> = {
        action: "start_preview_job",
        targetScope: crawlTargetScope,
        selectedFields: getSelectedCrawlFields(crawlFieldSelections),
      };

      if (crawlTargetScope === "filtered") {
        body.filterModels = buildRequestFilterModels(appliedColumnStates);
        body.advancedFilters = buildRequestAdvancedFilters(
          appliedAdvancedFilters,
          advancedValueOptions
        );
      }

      const sortColumn = COLUMN_DEFS.find(
        (column) => appliedColumnStates[column.key].sortDirection !== ""
      );

      if (sortColumn) {
        body.sortKey = sortColumn.key;
        body.sortDirection = appliedColumnStates[sortColumn.key].sortDirection;
      }

      const res = await fetch("/api/master_data/crawl", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "クローリング開始に失敗しました");
      }

      if (!data.jobId) {
        stopCrawlElapsedTracking();
        setCrawling(false);
        setCrawlProgressOpen(false);
        setCrawlMessage(data.message || "対象がありませんでした");
        saveActiveCrawlJobId(null);
        return;
      }

      setCrawlJobId(data.jobId);
      saveActiveCrawlJobId(data.jobId);
      applyCrawlStatus(data);
      startCrawlStatusPolling(data.jobId);
    } catch (e) {
      setCrawling(false);
      setCrawlProgressOpen(false);
      setCrawlError(
        e instanceof Error ? e.message : "クローリング開始でエラーが発生しました"
      );
    }
  };

  const handleCrawlSave = async () => {
    if (!crawlJobId) {
      setCrawlError("保存対象のジョブが見つかりません");
      return;
    }

    setCrawling(true);
    setCrawlMessage("");
    setCrawlError("");

    try {
      const res = await fetch("/api/master_data/crawl", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "save_partial",
          jobId: crawlJobId,
          selectedChanges: crawlSelectedChanges,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "クローリング保存に失敗しました");
      }

      setCrawlPreviewOpen(false);
      setCrawlPreviewRows([]);
      setCrawlPreviewTotalCount(0);
      setCrawlPreviewPage(1);
      setCrawlSelectedChanges({});
      setCrawlMessage(
        data.message ||
          `処理対象 ${data.processed ?? 0} 件 / 更新 ${data.updated ?? 0} 件 / スキップ ${data.skipped ?? 0} 件 / 失敗 ${data.failed ?? 0} 件`
      );

      setCrawlRemainingCount(data.remainingCount ?? 0);

      await fetchData();

      const remainingCount = data.remainingCount ?? 0;
      const previewTotal = data.previewTotal ?? 0;

      if (previewTotal > 0) {
        setCrawlPreviewRows(data.previewRows || []);
        setCrawlPreviewTotalCount(previewTotal);
        setCrawlPreviewPage(data.previewPage ?? 1);
        setCrawlPreviewOpen(true);
        setCrawlResumeConfirmOpen(false);
        setCrawlJobStatus("paused");
      } else if (remainingCount > 0) {
        setCrawlResumeConfirmOpen(true);
      } else {
        setCrawlJobId(null);
        saveActiveCrawlJobId(null);
        setCrawlJobStatus("completed");
      }
    } catch (e) {
      setCrawlError(
        e instanceof Error ? e.message : "クローリング保存でエラーが発生しました"
      );
    } finally {
      setCrawling(false);
    }
  };

  const handleListDelete = async () => {
    if (!listDeleteConfirmTarget) return;

    const deleteTarget = listDeleteConfirmTarget;

    setListDeleting(true);
    setListDeleteConfirmTarget(null);
    setListDeleteError("");
    setListDeleteMessage("");

    try {
      const params = new URLSearchParams();
      params.set("deleteMode", "list");
      params.set("deleteScope", deleteTarget);

      if (deleteTarget === "filtered") {
        params.set(
          "filterModels",
          JSON.stringify(buildRequestFilterModels(appliedColumnStates))
        );
        params.set(
          "advancedFilters",
          JSON.stringify(
            buildRequestAdvancedFilters(
              appliedAdvancedFilters,
              advancedValueOptions
            )
          )
        );
      }

      const res = await fetch(`/api/master_data?${params.toString()}`, {
        method: "DELETE",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "リスト削除に失敗しました");
      }

      setListDeleteScopeOpen(false);
      setListDeleteMessage(data.message || "リストを削除しました");
      setPage(1);
      await fetchData();
    } catch (e) {
      setListDeleteError(
        e instanceof Error ? e.message : "リスト削除でエラーが発生しました"
      );
    } finally {
      setListDeleting(false);
    }
  };

  const handleItemDelete = async () => {
    if (!itemDeleteTarget) return;

    const deleteTarget = itemDeleteTarget;
    const selectedFields = getSelectedItemDeleteFields(itemDeleteSelections);

    setItemDeleting(true);
    setItemDeleteConfirmOpen(false);
    setItemDeleteError("");
    setItemDeleteMessage("");

    try {
      const params = new URLSearchParams();
      params.set("deleteMode", "item");
      params.set("deleteScope", deleteTarget);
      params.set("selectedFields", JSON.stringify(selectedFields));

      if (deleteTarget === "filtered") {
        params.set(
          "filterModels",
          JSON.stringify(buildRequestFilterModels(appliedColumnStates))
        );
        params.set(
          "advancedFilters",
          JSON.stringify(
            buildRequestAdvancedFilters(
              appliedAdvancedFilters,
              advancedValueOptions
            )
          )
        );
      }

      const res = await fetch(`/api/master_data?${params.toString()}`, {
        method: "DELETE",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "項目削除に失敗しました");
      }

      setItemDeleteScopeOpen(false);
      setItemDeleteFieldOpen(false);
      setItemDeleteTarget(null);
      setItemDeleteMessage(data.message || "項目を削除しました");
      setPage(1);
      await fetchData();
    } catch (e) {
      setItemDeleteError(
        e instanceof Error ? e.message : "項目削除でエラーが発生しました"
      );
    } finally {
      setItemDeleting(false);
    }
  };

  const clearItemInspectionStatusPolling = () => {
    if (itemInspectionStatusTimerRef.current !== null) {
      window.clearInterval(itemInspectionStatusTimerRef.current);
      itemInspectionStatusTimerRef.current = null;
    }
  };

  const clearItemInspectionElapsedTimer = () => {
    if (itemInspectionElapsedTimerRef.current !== null) {
      window.clearInterval(itemInspectionElapsedTimerRef.current);
      itemInspectionElapsedTimerRef.current = null;
    }
  };

  const updateItemInspectionElapsedMs = () => {
    const runningMs =
      itemInspectionStartedAtRef.current !== null
        ? Date.now() - itemInspectionStartedAtRef.current
        : 0;

    setItemInspectionElapsedMs(
      itemInspectionElapsedBaseMsRef.current + runningMs
    );
  };

  const startItemInspectionElapsedTracking = (reset = false) => {
    if (reset) {
      itemInspectionElapsedBaseMsRef.current = 0;
      setItemInspectionElapsedMs(0);
    }

    itemInspectionStartedAtRef.current = Date.now();
    updateItemInspectionElapsedMs();
    clearItemInspectionElapsedTimer();

    itemInspectionElapsedTimerRef.current = window.setInterval(
      updateItemInspectionElapsedMs,
      1000
    );
  };

  const stopItemInspectionElapsedTracking = () => {
    if (itemInspectionStartedAtRef.current !== null) {
      itemInspectionElapsedBaseMsRef.current +=
        Date.now() - itemInspectionStartedAtRef.current;
      itemInspectionStartedAtRef.current = null;
    }

    updateItemInspectionElapsedMs();
    clearItemInspectionElapsedTimer();
  };

  const applyItemInspectionStatus = (data: ApiResponse) => {
    setItemInspectionJobStatus(
      (data.jobStatus as ItemInspectionJobStatus) ?? "idle"
    );
    setItemInspectionTotalTargets(data.totalTargets ?? 0);
    setItemInspectionProcessedCount(data.processed ?? 0);
    setItemInspectionUpdatedCount(data.updated ?? 0);
    setItemInspectionSkippedCount(data.skipped ?? 0);
    setItemInspectionFailedCount(data.failed ?? 0);
    setItemInspectionCurrentCompany(data.currentCompany ?? null);
    setItemInspectionCurrentValue(data.currentInspectionValue ?? null);
    setItemInspectionCurrentFieldLabel(
      data.currentInspectionFieldLabel ?? null
    );
    setItemInspectionRemainingCount(data.remainingCount ?? 0);
  };

  const openItemInspectionPreviewConfirmFromStatus = (
    data: ApiResponse
  ) => {
    const previewChanges = data.inspectionPreviewChanges || [];

    setItemInspectionPreviewChanges(previewChanges);
    setItemInspectionCheckedPreviewRowIds(
      Object.fromEntries(previewChanges.map((row) => [row.rowId, true]))
    );

    setItemInspectionPreviewPage(1);

    if (previewChanges.length > 0) {
      setItemInspectionPreviewConfirmOpen(true);
    } else {
      setItemInspectionPreviewConfirmOpen(false);
    }
  };

  const startItemInspectionStatusPolling = (jobId: string) => {
    clearItemInspectionStatusPolling();

    const poll = async () => {
      try {
        const res = await fetch("/api/master_data/item_inspection", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            action: "get_job_status",
            jobId,
          }),
        });

        const data = await readApiResponse(res);

        if (!res.ok || !data.ok) {
          throw new Error(data.error || "項目精査進捗の取得に失敗しました");
        }

        applyItemInspectionStatus(data);

        if (data.jobStatus === "paused") {
          clearItemInspectionStatusPolling();
          stopItemInspectionElapsedTracking();
          setItemInspecting(false);
          setItemInspectionProgressOpen(false);
          openItemInspectionPreviewConfirmFromStatus(data);
          setItemInspectionMessage("項目精査を中断しました");
          await fetchData();
          return;
        }

        if (data.jobStatus === "completed") {
          clearItemInspectionStatusPolling();
          stopItemInspectionElapsedTracking();
          setItemInspecting(false);
          setItemInspectionProgressOpen(false);
          openItemInspectionPreviewConfirmFromStatus(data);
          setItemInspectionMessage(data.message || "項目精査が完了しました");
          await fetchData();
          return;
        }

        if (data.jobStatus === "error") {
          clearItemInspectionStatusPolling();
          stopItemInspectionElapsedTracking();
          setItemInspecting(false);
          setItemInspectionProgressOpen(false);
          throw new Error(data.error || "項目精査中にエラーが発生しました");
        }
      } catch (e) {
        clearItemInspectionStatusPolling();
        stopItemInspectionElapsedTracking();
        setItemInspecting(false);
        setItemInspectionProgressOpen(false);
        setItemInspectionError(
          e instanceof Error
            ? e.message
            : "項目精査進捗取得でエラーが発生しました"
        );
      }
    };

    void poll();

    itemInspectionStatusTimerRef.current = window.setInterval(() => {
      void poll();
    }, 1200);
  };

  const handleCancelItemInspection = async () => {
    if (!itemInspectionJobId) return;

    try {
      const res = await fetch("/api/master_data/item_inspection", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "cancel_job",
          jobId: itemInspectionJobId,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "中止に失敗しました");
      }

      clearItemInspectionStatusPolling();
      stopItemInspectionElapsedTracking();
      setItemInspecting(false);
      setItemInspectionProgressOpen(false);
      setItemInspectionJobId(null);
      setItemInspectionJobStatus("idle");
      setItemInspectionTotalTargets(0);
      setItemInspectionProcessedCount(0);
      setItemInspectionUpdatedCount(0);
      setItemInspectionSkippedCount(0);
      setItemInspectionFailedCount(0);
      setItemInspectionCurrentCompany(null);
      setItemInspectionCurrentValue(null);
      setItemInspectionCurrentFieldLabel(null);
      setItemInspectionRemainingCount(0);
      setItemInspectionMessage("項目精査を中止しました");
      setItemInspectionError("");
    } catch (e) {
      setItemInspectionError(
        e instanceof Error ? e.message : "中止処理でエラーが発生しました"
      );
    }
  };

  const handlePauseItemInspection = async () => {
    if (!itemInspectionJobId) return;

    try {
      const res = await fetch("/api/master_data/item_inspection", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "pause_job",
          jobId: itemInspectionJobId,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "中断に失敗しました");
      }

      setItemInspectionMessage("中断指示を受け付けました");
    } catch (e) {
      setItemInspectionError(
        e instanceof Error ? e.message : "中断処理でエラーが発生しました"
      );
    }
  };

  const handleRunItemInspection = async () => {
    if (!itemInspectionTargetScope) return;

    if (
      !(
        selectedItemInspectionFields.length === 1 &&
        selectedItemInspectionFields[0] === "representative_name"
      )
    ) {
      return;
    }

    if (!itemInspectionMethodSelections.representative_name_remove_non_name) {
      return;
    }

    setItemInspecting(true);
    setItemInspectionMethodOpen(false);
    setItemInspectionPreviewConfirmOpen(false);
    setItemInspectionError("");
    setItemInspectionMessage("");
    setItemInspectionPreviewChanges([]);
    setItemInspectionCheckedPreviewRowIds({});
    setItemInspectionJobId(null);
    setItemInspectionJobStatus("running");
    setItemInspectionTotalTargets(0);
    setItemInspectionProcessedCount(0);
    setItemInspectionUpdatedCount(0);
    setItemInspectionSkippedCount(0);
    setItemInspectionFailedCount(0);
    setItemInspectionCurrentCompany(null);
    setItemInspectionCurrentValue(null);
    setItemInspectionCurrentFieldLabel("代表者名");
    setItemInspectionRemainingCount(0);
    setItemInspectionProgressOpen(true);
    startItemInspectionElapsedTracking(true);

    try {
      const body: Record<string, unknown> = {
        action: "start_job",
        inspectionScope: itemInspectionTargetScope,
        selectedFields: selectedItemInspectionFields,
        methodSelections: itemInspectionMethodSelections,
      };

      if (itemInspectionTargetScope === "filtered") {
        body.filterModels = buildRequestFilterModels(appliedColumnStates);
        body.advancedFilters = buildRequestAdvancedFilters(
          appliedAdvancedFilters,
          advancedValueOptions
        );
      }

      const res = await fetch("/api/master_data/item_inspection", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "項目精査開始に失敗しました");
      }

      if (!data.jobId) {
        clearItemInspectionStatusPolling();
        stopItemInspectionElapsedTracking();
        setItemInspecting(false);
        setItemInspectionProgressOpen(false);
        setItemInspectionMessage(data.message || "対象がありませんでした");
        return;
      }

      setItemInspectionJobId(data.jobId);
      applyItemInspectionStatus(data);
      startItemInspectionStatusPolling(data.jobId);
    } catch (e) {
      clearItemInspectionStatusPolling();
      stopItemInspectionElapsedTracking();
      setItemInspecting(false);
      setItemInspectionProgressOpen(false);
      setItemInspectionError(
        e instanceof Error ? e.message : "項目精査開始でエラーが発生しました"
      );
    }
  };

  const handleApplyItemInspectionChanges = async () => {
    const targetChanges = itemInspectionPreviewChanges.filter(
      (row) => itemInspectionCheckedPreviewRowIds[row.rowId] !== false
    );

    if (targetChanges.length === 0) {
      return;
    }

    setItemInspecting(true);
    setItemInspectionError("");
    setItemInspectionMessage("");

    try {
      const res = await fetch("/api/master_data/item_inspection", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "apply_preview_changes",
          changes: targetChanges,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "精査結果の反映に失敗しました");
      }

      setItemInspectionPreviewConfirmOpen(false);
      setItemInspectionPreviewChanges([]);
      setItemInspectionCheckedPreviewRowIds({});
      setItemInspectionMessage(data.message || "精査結果を反映しました");

      await fetchData();
    } catch (e) {
      setItemInspectionError(
        e instanceof Error
          ? e.message
          : "精査結果の反映でエラーが発生しました"
      );
    } finally {
      setItemInspecting(false);
    }
  };

  const handleDeduplicate = async () => {
    if (!dedupeTargetScope) return;

    setDeduplicating(true);
    setDedupeMessage("");
    setDedupeError("");

    try {
      const params = new URLSearchParams();
      params.set("deleteMode", "dedupe");
      params.set("deleteScope", dedupeTargetScope);

      if (dedupeTargetScope === "filtered") {
        params.set(
          "filterModels",
          JSON.stringify(buildRequestFilterModels(appliedColumnStates))
        );
        params.set(
          "advancedFilters",
          JSON.stringify(
            buildRequestAdvancedFilters(
              appliedAdvancedFilters,
              advancedValueOptions
            )
          )
        );
      }

      const res = await fetch(`/api/master_data?${params.toString()}`, {
        method: "DELETE",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "重複削除に失敗しました");
      }

      setDedupeMessage(
        data.message ||
          `${data.deleted?.toLocaleString() ?? 0}件の重複データを削除しました`
      );

      setPage(1);
      await fetchData();
    } catch (e) {
      setDedupeError(
        e instanceof Error ? e.message : "重複削除でエラーが発生しました"
      );
    } finally {
      setDeduplicating(false);
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

  const usingVirtual = true;

  const activeSidebarPanelTitle = openSidebarPanel
    ? SIDEBAR_PANEL_TITLES[openSidebarPanel]
    : "";

  const sidebarPanelMaxWidthClass =
    openSidebarPanel === "csv"
      ? "max-w-[920px]"
      : openSidebarPanel === "search"
      ? "max-w-[720px]"
      : openSidebarPanel === "theme"
      ? "max-w-[420px]"
      : "max-w-[520px]";

  const activeAdvancedFilterTitle = openAdvancedFilterKey
    ? ADVANCED_FILTER_TITLES[openAdvancedFilterKey]
    : "";

  const activeAdvancedFilterKey = openAdvancedFilterKey;

  const singleFilterClearLabel = singleFilterClearConfirm
    ? singleFilterClearConfirm.type === "column"
      ? COLUMN_DEFS.find((column) => column.key === singleFilterClearConfirm.key)?.label ?? ""
      : ADVANCED_FILTER_TITLES[singleFilterClearConfirm.key]
    : "";

  const isWideAdvancedFilterModal =
    activeAdvancedFilterKey === "prefecture" ||
    activeAdvancedFilterKey === "industry" ||
    activeAdvancedFilterKey === "tag";

  const selectedCrawlFields = getSelectedCrawlFields(crawlFieldSelections);
  const hasSelectedCrawlFields = selectedCrawlFields.length > 0;

  const selectedItemDeleteFields = getSelectedItemDeleteFields(
    itemDeleteSelections
  );
  const hasSelectedItemDeleteFields = selectedItemDeleteFields.length > 0;

  const selectedItemDeleteLabels = COLUMN_DEFS.filter(
    (column) => itemDeleteSelections[column.key]
  ).map((column) => column.label);

  const selectedItemInspectionFields = getSelectedItemDeleteFields(
    itemInspectionSelections
  );

  const selectedItemInspectionLabels = COLUMN_DEFS.filter(
    (column) => itemInspectionSelections[column.key]
  ).map((column) => column.label);

  const hasSelectedItemInspectionFields =
    selectedItemInspectionFields.length > 0;

  const hasSelectedItemInspectionMethods =
    itemInspectionMethodSelections.representative_name_remove_non_name;

  const itemInspectionProcessingCount =
    itemInspectionJobStatus === "running" && itemInspectionCurrentCompany
      ? 1
      : 0;

  const itemInspectionAverageDivisor = Math.max(
    itemInspectionProcessedCount,
    itemInspectionProcessingCount
  );

  const averageItemInspectionMs =
    itemInspectionAverageDivisor > 0
      ? Math.round(itemInspectionElapsedMs / itemInspectionAverageDivisor)
      : 0;

  const itemInspectionProgressPercent =
    itemInspectionTotalTargets === 0
      ? 0
      : Math.round(
          (itemInspectionProcessedCount / itemInspectionTotalTargets) * 100
        );

  const isRepresentativeNameOnlyInspection =
    selectedItemInspectionFields.length === 1 &&
    selectedItemInspectionFields[0] === "representative_name";

  const renderedTableBody = useMemo(() => {
    if (rows.length === 0 && !loading) {
      return (
        <div className="px-4 py-12 text-center text-slate-500">
          データがありません
        </div>
      );
    }

    if (usingVirtual) {
      return (
        <List
          defaultHeight={650}
          rowComponent={VirtualRow}
          rowCount={rows.length}
          rowHeight={58}
          rowProps={{ rows }}
          style={{ width: "100%" }}
        />
      );
    }

    return (
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
            <Cell title={row.tag || ""}><EmptyValue value={row.tag} /></Cell>
            <Cell title={row.business_type || ""}><EmptyValue value={row.business_type} /></Cell>
            <Cell title={row.business_content || ""} className="whitespace-pre-wrap"><EmptyValue value={row.business_content} /></Cell>
            <Cell title={row.industry_category || ""}><EmptyValue value={row.industry_category} /></Cell>
            <Cell title={row.memo || ""} className="whitespace-pre-wrap"><EmptyValue value={row.memo} /></Cell>
          </div>
        ))}
      </div>
    );
  }, [loading, rows]);

  return (
    <main
      data-theme={themeMode}
      className="h-[100dvh] overflow-hidden bg-transparent text-slate-100"
    >
      <div
        className={`mx-auto flex h-full max-w-[1880px] flex-col pr-6 py-6 transition-all duration-300 ${
          sidebarOpen ? "pl-[276px]" : "pl-[148px]"
        }`}
      >

        <aside
          className={`fixed left-6 top-6 z-40 transition-all duration-300 ${
            sidebarOpen ? "w-[220px]" : "w-[92px]"
          }`}
        >
          <div className="rounded-[28px] border border-white/10 bg-[#08101d]/90 p-2 shadow-[0_24px_60px_rgba(0,0,0,0.35)] backdrop-blur-2xl">
            <div className="rounded-[24px] border border-white/10 bg-[#0b1326]/85 p-3">
              <div
                className={`mb-3 flex items-center ${
                  sidebarOpen ? "justify-between" : "justify-center"
                }`}
              >
                {sidebarOpen && (
                  <div className="px-2 text-sm font-semibold text-slate-200">
                    メニュー
                  </div>
                )}

                <button
                  type="button"
                  onClick={() => setSidebarOpen((prev) => !prev)}
                  className="inline-flex h-10 w-10 items-center justify-center rounded-xl border border-white/10 bg-white/5 text-slate-200 transition hover:bg-white/10"
                >
                  {sidebarOpen ? "←" : "→"}
                </button>
              </div>

              <div className="space-y-2">
                {SIDEBAR_MENU_ITEMS.map((item) => {
                  const isActive = openSidebarPanel === item.key;

                  return (
                    <div key={item.key} className="space-y-2">
                      <button
                        type="button"
                        onClick={() => handleOpenSidebarPanel(item.key)}
                        className={`flex w-full items-center gap-3 rounded-2xl border px-3 py-3 text-left text-sm font-medium transition ${
                          isActive
                            ? "border-sky-400/40 bg-sky-500/20 text-sky-100"
                            : "border-white/10 bg-white/5 text-slate-200 hover:bg-white/10"
                        } ${sidebarOpen ? "justify-start" : "justify-center"}`}
                      >
                        <span className="inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-[#0f172a]">
                          <SidebarMenuIcon menuKey={item.key} />
                        </span>

                        {sidebarOpen && (
                          <span className="truncate">{item.label}</span>
                        )}
                      </button>

                      {item.key === "search" && (
                        <button
                          type="button"
                          onClick={() => {
                            setOpenSidebarPanel(null);
                            setOpenFilterKey(null);
                            setOpenAdvancedFilterKey(null);
                            setAllFiltersClearConfirmOpen(true);
                          }}
                          className={`flex w-full items-center gap-3 rounded-2xl border border-white/10 bg-white/5 px-3 py-3 text-left text-sm font-medium text-slate-200 transition hover:bg-white/10 ${
                            sidebarOpen ? "justify-start" : "justify-center"
                          }`}
                        >
                          <span className="inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-[#0f172a] text-base">
                            ↺
                          </span>

                          {sidebarOpen && (
                            <span className="truncate">フィルタ解除</span>
                          )}
                        </button>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </aside>

        <div
          ref={topPanelRef}
          className="sticky top-0 z-30 mb-6 rounded-[28px] border border-white/10 bg-[#08101d]/80 p-2 shadow-[0_24px_60px_rgba(0,0,0,0.35)] backdrop-blur-2xl"
        >
          <div className="rounded-[24px] border border-white/10 bg-[#0b1326]/85 p-6">
            <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
              <div className="flex items-center gap-6">
                <MasterDataBrandLogo className="h-auto w-[280px] shrink-0 md:w-[340px]" />

                <h1 className="master-data-brand-title text-[40px] leading-none md:text-[54px]">
                  マスタデータ
                </h1>
              </div>

              <div className="grid grid-cols-2 gap-3 xl:grid-cols-4">
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
          </div>

        {openSidebarPanel &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => setOpenSidebarPanel(null)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className={`flex w-full ${sidebarPanelMaxWidthClass} flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl`}
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      {activeSidebarPanelTitle}
                    </div>

                    <button
                      type="button"
                      onClick={() => setOpenSidebarPanel(null)}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="px-4 py-4">
                    {renderSidebarPanelContent()}
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

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

                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() =>
                          activeAdvancedFilterKey &&
                          setSingleFilterClearConfirm({
                            type: "advanced",
                            key: activeAdvancedFilterKey,
                          })
                        }
                        className="inline-flex h-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 px-3 text-xs text-slate-300 transition hover:bg-white/10"
                      >
                        フィルタ解除
                      </button>

                      <button
                        type="button"
                        onClick={() => setOpenAdvancedFilterKey(null)}
                        className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                      >
                        ×
                      </button>
                    </div>
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

        {itemInspectionFieldOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => {
                if (itemInspecting) return;
                setItemInspectionFieldOpen(false);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      項目精査確認
                    </div>

                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() =>
                          setItemInspectionSelections(
                            createInitialItemDeleteSelections()
                          )
                        }
                        disabled={itemInspecting}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        全選択
                      </button>

                      <button
                        type="button"
                        onClick={() =>
                          setItemInspectionSelections(
                            createEmptyItemDeleteSelections()
                          )
                        }
                        disabled={itemInspecting}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        選択解除
                      </button>
                    </div>
                  </div>

                  <div className="px-4 py-6">
                    <div className="mb-4 text-sm leading-7 text-slate-300">
                      チェックした項目のみ精査します。
                      <br />
                      <span className="text-xs text-slate-400">
                        対象:
                        {itemInspectionTargetScope === "all"
                          ? "全てのリスト"
                          : "絞り込みリストのみ"}
                      </span>
                    </div>

                    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-4">
                      {COLUMN_DEFS.map((field) => (
                        <label
                          key={field.key}
                          className="flex items-center gap-3 rounded-xl border border-white/10 bg-white/5 px-3 py-3 text-sm text-slate-200"
                        >
                          <input
                            type="checkbox"
                            checked={itemInspectionSelections[field.key]}
                            onChange={() =>
                              setItemInspectionSelections((prev) => ({
                                ...prev,
                                [field.key]: !prev[field.key],
                              }))
                            }
                            className="h-4 w-4 accent-cyan-500"
                          />
                          <span>{field.label}</span>
                        </label>
                      ))}
                    </div>
                  </div>

                  <div className="flex justify-center gap-3 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setItemInspectionFieldOpen(false)}
                      disabled={itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={() => {
                        if (!isRepresentativeNameOnlyInspection) {
                          return;
                        }

                        setItemInspectionFieldOpen(false);
                        setItemInspectionMethodOpen(true);
                      }}
                      disabled={itemInspecting || !hasSelectedItemInspectionFields}
                      className={`h-10 w-[120px] flex-none rounded-xl px-3 text-sm font-medium text-white transition disabled:cursor-not-allowed disabled:opacity-100 ${
                        !hasSelectedItemInspectionFields
                          ? "bg-cyan-500/30 text-white/60"
                          : "bg-cyan-500 hover:bg-cyan-400"
                      }`}
                    >
                      はい
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {itemInspectionMethodOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => {
                if (itemInspecting) return;
                setItemInspectionMethodOpen(false);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[720px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      項目精査 方法確認
                    </div>

                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() =>
                          setItemInspectionMethodSelections(
                            createInitialItemInspectionMethodSelections()
                          )
                        }
                        disabled={itemInspecting}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        全選択
                      </button>

                      <button
                        type="button"
                        onClick={() =>
                          setItemInspectionMethodSelections(
                            createEmptyItemInspectionMethodSelections()
                          )
                        }
                        disabled={itemInspecting}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        選択解除
                      </button>
                    </div>
                  </div>

                  <div className="px-4 py-6">
                    <div className="grid grid-cols-1 gap-2">
                      <label className="flex items-center gap-3 rounded-xl border border-white/10 bg-white/5 px-3 py-3 text-sm text-slate-200">
                        <input
                          type="checkbox"
                          checked={
                            itemInspectionMethodSelections.representative_name_remove_non_name
                          }
                          onChange={() =>
                            setItemInspectionMethodSelections((prev) => ({
                              ...prev,
                              representative_name_remove_non_name:
                                !prev.representative_name_remove_non_name,
                            }))
                          }
                          className="h-4 w-4 accent-cyan-500"
                        />
                        <span>氏名以外を削除</span>
                      </label>
                    </div>
                  </div>

                  <div className="flex justify-center gap-3 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setItemInspectionMethodOpen(false)}
                      disabled={itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleRunItemInspection}
                      disabled={
                        itemInspecting || !hasSelectedItemInspectionMethods
                      }
                      className={`h-10 w-[120px] flex-none rounded-xl px-3 text-sm font-medium text-white transition disabled:cursor-not-allowed disabled:opacity-100 ${
                        !hasSelectedItemInspectionMethods
                          ? "bg-cyan-500/30 text-white/60"
                          : "bg-cyan-500 hover:bg-cyan-400"
                      }`}
                    >
                      {itemInspecting ? "精査中..." : "はい"}
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {itemInspectionProgressOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6">
              <div className="flex min-h-full items-center justify-center">
                <div className="flex w-full max-w-[720px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl">
                  <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                    項目精査 進行状況
                  </div>

                  <div className="space-y-4 px-4 py-5">
                    <div className="rounded-xl border border-white/10 bg-[#0f172a] p-4">
                      <div className="mb-2 text-xs text-slate-400">
                        現在処理中の企業
                      </div>
                      <div className="text-sm font-semibold text-slate-100">
                        {itemInspectionCurrentCompany || "待機中"}
                      </div>

                      <div className="mt-3 text-xs text-slate-400">
                        現在処理中の
                        {itemInspectionCurrentFieldLabel || "項目"}
                      </div>
                      <div className="mt-1 break-all text-sm text-sky-300">
                        {itemInspectionCurrentValue || "-"}
                      </div>

                      <div className="mt-3 text-xs text-slate-400">
                        精査対象項目
                      </div>
                      <div className="mt-1 text-sm text-slate-200">
                        {selectedItemInspectionLabels.length > 0
                          ? selectedItemInspectionLabels.join(" / ")
                          : "-"}
                      </div>
                    </div>

                    <div className="rounded-xl border border-white/10 bg-[#0f172a] p-4">
                      <div className="flex items-center justify-between gap-3 text-sm">
                        <span className="text-slate-300">進捗</span>

                        <div className="flex items-center gap-4">
                          <div className="text-right text-xs leading-5 text-slate-400">
                            <div>
                              平均{" "}
                              {itemInspectionProcessedCount > 0
                                ? formatCrawlDuration(averageItemInspectionMs)
                                : "-"}{" "}
                              / 件
                            </div>
                            <div>
                              経過 {formatCrawlDuration(itemInspectionElapsedMs)}
                            </div>
                          </div>

                          <span className="font-semibold text-slate-100">
                            {itemInspectionProgressPercent}%
                          </span>
                        </div>
                      </div>

                      <div className="mt-3 h-3 overflow-hidden rounded-full bg-white/10">
                        <div
                          className="h-full rounded-full bg-cyan-500 transition-all"
                          style={{
                            width: `${itemInspectionProgressPercent}%`,
                          }}
                        />
                      </div>
                    </div>

                    <div className="grid grid-cols-2 gap-3">
                      <div className="rounded-xl border border-white/10 bg-[#0f172a] p-4">
                        <div className="text-xs text-slate-400">完了</div>
                        <div className="mt-1 text-lg font-semibold text-slate-100">
                          {itemInspectionProcessedCount.toLocaleString()}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-[#0f172a] p-4">
                        <div className="text-xs text-slate-400">処理中</div>
                        <div className="mt-1 text-lg font-semibold text-slate-100">
                          {itemInspectionProcessingCount.toLocaleString()}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-[#0f172a] p-4">
                        <div className="text-xs text-slate-400">対象総数</div>
                        <div className="mt-1 text-lg font-semibold text-slate-100">
                          {itemInspectionTotalTargets.toLocaleString()}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-[#0f172a] p-4">
                        <div className="text-xs text-slate-400">精査件数</div>
                        <div className="mt-1 text-lg font-semibold text-slate-100">
                          {itemInspectionUpdatedCount.toLocaleString()}
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="flex justify-center gap-3 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={handleCancelItemInspection}
                      disabled={!itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
                    >
                      中止
                    </button>

                    <button
                      type="button"
                      onClick={handlePauseItemInspection}
                      disabled={!itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl bg-cyan-500 px-3 text-sm font-medium text-white transition hover:bg-cyan-400 disabled:opacity-50"
                    >
                      中断
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {itemInspectionPreviewConfirmOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => {
                if (itemInspecting) return;
                setItemInspectionPreviewConfirmOpen(false);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      項目精査確認
                    </div>

                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() =>
                          setItemInspectionCheckedPreviewRowIds(
                            Object.fromEntries(
                              itemInspectionPreviewChanges.map((row) => [
                                row.rowId,
                                true,
                              ])
                            )
                          )
                        }
                        disabled={itemInspecting}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        全選択
                      </button>

                      <button
                        type="button"
                        onClick={() => setItemInspectionCheckedPreviewRowIds({})}
                        disabled={itemInspecting}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        選択解除
                      </button>
                    </div>
                  </div>

                  <div className="px-4 py-6">
                    <div className="mb-4 text-sm leading-7 text-slate-300">
                      チェックした項目のみ反映します。
                    </div>

                    {itemInspectionPreviewChanges.length === 0 ? (
                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-8 text-center text-sm text-slate-400">
                        反映対象はありません
                      </div>
                    ) : (
                      <>
                        <div className="mb-4 border-b border-white/10 pb-4">
                          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                            <div className="text-xs text-slate-400">
                              削除候補 {itemInspectionPreviewChanges.length.toLocaleString()}件中{" "}
                              {(itemInspectionPreviewPage - 1) *
                                ITEM_INSPECTION_PREVIEW_PAGE_SIZE +
                                1}
                              〜
                              {Math.min(
                                itemInspectionPreviewPage *
                                  ITEM_INSPECTION_PREVIEW_PAGE_SIZE,
                                itemInspectionPreviewChanges.length
                              )}
                              件を表示
                            </div>

                            <div className="flex items-center gap-2">
                              <button
                                type="button"
                                onClick={() =>
                                  setItemInspectionPreviewPage((prev) =>
                                    Math.max(prev - 1, 1)
                                  )
                                }
                                disabled={
                                  itemInspectionPreviewPage === 1 || itemInspecting
                                }
                                className="h-8 rounded-lg border border-white/10 bg-white/5 px-3 text-xs text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                              >
                                前へ
                              </button>

                              <div className="text-xs text-slate-300">
                                {itemInspectionPreviewPage} /{" "}
                                {itemInspectionPreviewTotalPages}
                              </div>

                              <button
                                type="button"
                                onClick={() =>
                                  setItemInspectionPreviewPage((prev) =>
                                    Math.min(
                                      prev + 1,
                                      itemInspectionPreviewTotalPages
                                    )
                                  )
                                }
                                disabled={
                                  itemInspectionPreviewPage >=
                                    itemInspectionPreviewTotalPages ||
                                  itemInspecting
                                }
                                className="h-8 rounded-lg border border-white/10 bg-white/5 px-3 text-xs text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                              >
                                次へ
                              </button>
                            </div>
                          </div>
                        </div>

                        <div className="grid max-h-[60vh] grid-cols-1 gap-2 overflow-y-auto">
                          {pagedItemInspectionPreviewChanges.map((row) => (
                            <label
                              key={row.rowId}
                              className="flex items-start gap-3 rounded-xl border border-white/10 bg-white/5 px-3 py-3 text-sm text-slate-200"
                            >
                              <input
                                type="checkbox"
                                checked={
                                  itemInspectionCheckedPreviewRowIds[row.rowId] !==
                                  false
                                }
                                onChange={() =>
                                  setItemInspectionCheckedPreviewRowIds((prev) => ({
                                    ...prev,
                                    [row.rowId]: prev[row.rowId] === false,
                                  }))
                                }
                                className="mt-1 h-4 w-4 accent-cyan-500"
                              />

                              <div className="min-w-0 flex-1">
                                <div className="truncate font-medium text-slate-100">
                                  {row.company || "(企業名なし)"}
                                </div>

                                <div className="mt-1 text-xs text-slate-400">
                                  項目：{row.fieldLabel}
                                </div>

                                <div className="mt-1 break-words text-xs text-slate-400">
                                  現在値：{row.beforeValue || "-"}
                                </div>

                                <div className="mt-1 break-words text-xs text-emerald-200">
                                  反映後：
                                  {row.action === "delete"
                                    ? "(削除)"
                                    : row.afterValue || "-"}
                                </div>

                                <div className="mt-1 text-xs text-cyan-200">
                                  {row.reason}
                                </div>
                              </div>

                              <div className="shrink-0 rounded-lg border border-white/10 bg-[#0f172a] px-2 py-1 text-xs text-slate-200">
                                {row.action === "delete" ? "削除" : "更新"}
                              </div>
                            </label>
                          ))}
                        </div>
                      </>
                    )}
                  </div>

                  <div className="flex justify-center gap-3 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setItemInspectionPreviewConfirmOpen(false)}
                      disabled={itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleApplyItemInspectionChanges}
                      disabled={itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl bg-cyan-500 px-3 text-sm font-medium text-white transition hover:bg-cyan-400 disabled:opacity-50"
                    >
                      {itemInspecting ? "反映中..." : "はい"}
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {crawlScopeOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => {
                setCrawlScopeOpen(false);
                setCrawlTargetScope(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      クローリング 対象選択
                    </div>

                    <button
                      type="button"
                      onClick={() => {
                        setCrawlScopeOpen(false);
                        setCrawlTargetScope(null);
                      }}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="px-4 py-4">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <button
                        type="button"
                        onClick={() => {
                          setCrawlScopeOpen(false);
                          setCrawlTargetScope("all");
                          setCrawlConfirmOpen(true);
                        }}
                        className="h-11 rounded-xl bg-amber-600 px-4 text-sm font-medium text-white transition hover:bg-amber-500"
                      >
                        全てのリスト
                      </button>

                      <button
                        type="button"
                        onClick={() => {
                          setCrawlScopeOpen(false);
                          setCrawlTargetScope("filtered");
                          setCrawlConfirmOpen(true);
                        }}
                        className="h-11 rounded-xl bg-amber-500 px-4 text-sm font-medium text-white transition hover:bg-amber-400"
                      >
                        絞り込みリストのみ
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {itemInspectionScopeOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => {
                setItemInspectionScopeOpen(false);
                setItemInspectionTargetScope(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      項目精査 対象選択
                    </div>

                    <button
                      type="button"
                      onClick={() => {
                        setItemInspectionScopeOpen(false);
                        setItemInspectionTargetScope(null);
                      }}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="px-4 py-4">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <button
                        type="button"
                        onClick={() => {
                          setItemInspectionScopeOpen(false);
                          setItemInspectionTargetScope("all");
                          setItemInspectionFieldOpen(true);
                        }}
                        className="h-11 rounded-xl bg-cyan-600 px-4 text-sm font-medium text-white transition hover:bg-cyan-500"
                      >
                        全てのリスト
                      </button>

                      <button
                        type="button"
                        onClick={() => {
                          setItemInspectionScopeOpen(false);
                          setItemInspectionTargetScope("filtered");
                          setItemInspectionFieldOpen(true);
                        }}
                        className="h-11 rounded-xl bg-cyan-500 px-4 text-sm font-medium text-white transition hover:bg-cyan-400"
                      >
                        絞り込みリストのみ
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {dedupeScopeOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => {
                setDedupeScopeOpen(false);
                setDedupeTargetScope(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      重複削除 対象選択
                    </div>

                    <button
                      type="button"
                      onClick={() => {
                        setDedupeScopeOpen(false);
                        setDedupeTargetScope(null);
                      }}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="px-4 py-4">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <button
                        type="button"
                        onClick={() => {
                          setDedupeScopeOpen(false);
                          setDedupeTargetScope("all");
                          setDedupeConfirmOpen(true);
                        }}
                        className="h-11 rounded-xl bg-violet-600 px-4 text-sm font-medium text-white transition hover:bg-violet-500"
                      >
                        全てのリスト
                      </button>

                      <button
                        type="button"
                        onClick={() => {
                          setDedupeScopeOpen(false);
                          setDedupeTargetScope("filtered");
                          setDedupeConfirmOpen(true);
                        }}
                        className="h-11 rounded-xl bg-violet-500 px-4 text-sm font-medium text-white transition hover:bg-violet-400"
                      >
                        絞り込みリストのみ
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {itemDeleteScopeOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => {
                setItemDeleteScopeOpen(false);
                setItemDeleteTarget(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      項目削除 対象選択
                    </div>

                    <button
                      type="button"
                      onClick={() => {
                        setItemDeleteScopeOpen(false);
                        setItemDeleteTarget(null);
                      }}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="px-4 py-4">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <button
                        type="button"
                        onClick={() => {
                          setItemDeleteScopeOpen(false);
                          setItemDeleteTarget("all");
                          setItemDeleteFieldOpen(true);
                        }}
                        className="h-11 rounded-xl bg-cyan-600 px-4 text-sm font-medium text-white transition hover:bg-cyan-500"
                      >
                        全てのリスト
                      </button>

                      <button
                        type="button"
                        onClick={() => {
                          setItemDeleteScopeOpen(false);
                          setItemDeleteTarget("filtered");
                          setItemDeleteFieldOpen(true);
                        }}
                        className="h-11 rounded-xl bg-cyan-500 px-4 text-sm font-medium text-white transition hover:bg-cyan-400"
                      >
                        絞り込みリストのみ
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {itemDeleteFieldOpen &&
          itemDeleteTarget &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => {
                if (itemDeleting) return;
                setItemDeleteFieldOpen(false);
                setItemDeleteTarget(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      項目削除確認
                    </div>

                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() =>
                          setItemDeleteSelections(createInitialItemDeleteSelections())
                        }
                        disabled={itemDeleting}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        全選択
                      </button>

                      <button
                        type="button"
                        onClick={() =>
                          setItemDeleteSelections(createEmptyItemDeleteSelections())
                        }
                        disabled={itemDeleting}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        選択解除
                      </button>
                    </div>
                  </div>

                  <div className="px-4 py-6">
                    <div className="mb-4 text-sm leading-7 text-slate-300">
                      チェックした項目のみ削除します。
                      <br />
                      <span className="text-xs text-slate-400">
                        対象:
                        {itemDeleteTarget === "all"
                          ? "全てのリスト"
                          : "絞り込みリストのみ"}
                      </span>
                    </div>

                    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-4">
                      {COLUMN_DEFS.map((field) => (
                        <label
                          key={field.key}
                          className="flex items-center gap-3 rounded-xl border border-white/10 bg-white/5 px-3 py-3 text-sm text-slate-200"
                        >
                          <input
                            type="checkbox"
                            checked={itemDeleteSelections[field.key]}
                            onChange={() =>
                              setItemDeleteSelections((prev) => ({
                                ...prev,
                                [field.key]: !prev[field.key],
                              }))
                            }
                            className="h-4 w-4 accent-cyan-500"
                          />
                          <span>{field.label}</span>
                        </label>
                      ))}
                    </div>
                  </div>

                  <div className="flex justify-center gap-3 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => {
                        setItemDeleteFieldOpen(false);
                        setItemDeleteTarget(null);
                      }}
                      disabled={itemDeleting}
                      className="h-10 w-[120px] flex-none rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={() => {
                        setItemDeleteFieldOpen(false);
                        setItemDeleteConfirmOpen(true);
                      }}
                      disabled={itemDeleting || !hasSelectedItemDeleteFields}
                      className={`h-10 w-[120px] flex-none rounded-xl px-3 text-sm font-medium text-white transition disabled:cursor-not-allowed disabled:opacity-100 ${
                        !hasSelectedItemDeleteFields
                          ? "bg-cyan-500/30 text-white/60"
                          : "bg-cyan-500 hover:bg-cyan-400"
                      }`}
                    >
                      はい
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {itemDeleteConfirmOpen &&
          itemDeleteTarget &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => {
                if (itemDeleting) return;
                setItemDeleteConfirmOpen(false);
                setItemDeleteTarget(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                    項目削除確認
                  </div>

                  <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                    本当に項目を削除しますか？
                    <br />
                    <span className="text-xs text-slate-400">
                      対象:
                      {itemDeleteTarget === "all"
                        ? "全てのリスト"
                        : "絞り込みリストのみ"}
                    </span>

                    <div className="mt-4">
                      <div className="mb-2 text-xs font-semibold text-slate-400">
                        選択項目
                      </div>

                      <div className="flex flex-wrap gap-2">
                        {selectedItemDeleteLabels.map((label) => (
                          <span
                            key={label}
                            className="inline-flex items-center rounded-lg border border-amber-500/20 bg-amber-500/10 px-3 py-1 text-xs text-amber-200"
                          >
                            {label}
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>

                  <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => {
                        setItemDeleteConfirmOpen(false);
                        setItemDeleteTarget(null);
                      }}
                      disabled={itemDeleting}
                      className="h-10 flex-1 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleItemDelete}
                      disabled={itemDeleting}
                      className="h-10 flex-1 rounded-xl bg-cyan-500 px-3 text-sm font-medium text-white transition hover:bg-cyan-400 disabled:opacity-50"
                    >
                      {itemDeleting ? "削除中..." : "はい"}
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {listDeleteScopeOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => setListDeleteScopeOpen(false)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      リスト削除 対象選択
                    </div>

                    <button
                      type="button"
                      onClick={() => setListDeleteScopeOpen(false)}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="px-4 py-4">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <button
                        type="button"
                        onClick={() => {
                          setListDeleteScopeOpen(false);
                          setListDeleteConfirmTarget("all");
                        }}
                        className="h-11 rounded-xl bg-rose-600 px-4 text-sm font-medium text-white transition hover:bg-rose-500"
                      >
                        全てのリスト
                      </button>

                      <button
                        type="button"
                        onClick={() => {
                          setListDeleteScopeOpen(false);
                          setListDeleteConfirmTarget("filtered");
                        }}
                        className="h-11 rounded-xl bg-rose-500 px-4 text-sm font-medium text-white transition hover:bg-rose-400"
                      >
                        絞り込みリストのみ
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {listDeleteConfirmTarget &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => !listDeleting && setListDeleteConfirmTarget(null)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                    リスト削除確認
                  </div>

                  <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                    本当にリストを削除しますか？
                    <br />
                    <span className="text-xs text-slate-400">
                      対象:
                      {listDeleteConfirmTarget === "all"
                        ? "全てのリスト"
                        : "絞り込みリストのみ"}
                    </span>
                  </div>

                  <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setListDeleteConfirmTarget(null)}
                      disabled={listDeleting}
                      className="h-10 flex-1 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleListDelete}
                      disabled={listDeleting}
                      className="h-10 flex-1 rounded-xl bg-rose-500 px-3 text-sm font-medium text-white transition hover:bg-rose-400 disabled:opacity-50"
                    >
                      {listDeleting ? "削除中..." : "はい"}
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {singleFilterClearConfirm &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => setSingleFilterClearConfirm(null)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                    フィルタ解除確認
                  </div>

                  <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                    {singleFilterClearLabel} のフィルタを解除しますか？
                  </div>

                  <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setSingleFilterClearConfirm(null)}
                      className="h-10 flex-1 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleConfirmSingleFilterClear}
                      className="h-10 flex-1 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400"
                    >
                      はい
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {crawlProgressOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6">
              <div className="flex min-h-full items-center justify-center">
                <div className="flex w-full max-w-[720px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl">
                  <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                    クローリング進行状況
                  </div>

                  <div className="space-y-4 px-4 py-5">
                    <div className="rounded-xl border border-white/10 bg-[#0f172a] p-4">
                      <div className="mb-2 text-xs text-slate-400">現在処理中の企業</div>
                      <div className="text-sm font-semibold text-slate-100">
                        {crawlCurrentCompany || "待機中"}
                      </div>

                      <div className="mt-3 text-xs text-slate-400">現在処理中のURL</div>
                      <div className="mt-1 break-all text-xs text-sky-300">
                        {crawlCurrentWebsiteUrl || "-"}
                      </div>

                      <div className="mt-3 text-xs text-slate-400">取得対象項目</div>
                      <div className="mt-1 text-sm text-slate-200">
                        {crawlCurrentFields.length > 0
                          ? crawlCurrentFields.join(" / ")
                          : "-"}
                      </div>
                    </div>

                    <div className="rounded-xl border border-white/10 bg-[#0f172a] p-4">
                      <div className="flex items-center justify-between gap-3 text-sm">
                        <span className="text-slate-300">進捗</span>

                        <div className="flex items-center gap-4">
                          <div className="text-right text-xs leading-5 text-slate-400">
                            <div>
                              平均 {crawlProcessedCount > 0 ? formatCrawlDuration(averageCrawlMs) : "-"} / 件
                            </div>
                            <div>
                              経過 {formatCrawlDuration(crawlElapsedMs)}
                            </div>
                          </div>

                          <span className="font-semibold text-slate-100">
                            {crawlTotalTargets === 0
                              ? 0
                              : Math.round((crawlProcessedCount / crawlTotalTargets) * 100)}
                            %
                          </span>
                        </div>
                      </div>

                      <div className="mt-3 h-3 overflow-hidden rounded-full bg-white/10">
                        <div
                          className="h-full rounded-full bg-amber-500 transition-all"
                          style={{
                            width: `${
                              crawlTotalTargets === 0
                                ? 0
                                : Math.round((crawlProcessedCount / crawlTotalTargets) * 100)
                            }%`,
                          }}
                        />
                      </div>

                      <div className="mt-4 grid grid-cols-2 gap-3 md:grid-cols-4">
                        <div className="rounded-lg bg-white/5 px-3 py-3">
                          <div className="text-xs text-slate-400">完了</div>
                          <div className="mt-1 text-lg font-semibold text-slate-100">
                            {crawlProcessedCount}
                          </div>
                        </div>

                        <div className="rounded-lg bg-white/5 px-3 py-3">
                          <div className="text-xs text-slate-400">処理中</div>
                          <div className="mt-1 text-lg font-semibold text-slate-100">
                            {crawlJobStatus === "running" && crawlCurrentCompany ? 1 : 0}
                          </div>
                        </div>

                        <div className="rounded-lg bg-white/5 px-3 py-3">
                          <div className="text-xs text-slate-400">対象総数</div>
                          <div className="mt-1 text-lg font-semibold text-slate-100">
                            {crawlTotalTargets}
                          </div>
                        </div>

                        <div className="rounded-lg bg-white/5 px-3 py-3">
                          <div className="text-xs text-slate-400">候補件数</div>
                          <div className="mt-1 text-lg font-semibold text-slate-100">
                            {crawlUpdatedCount}
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="flex justify-center gap-3 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={handleCancelCrawl}
                      disabled={!crawlJobId || crawling === false}
                      className="h-10 w-[120px] flex-none rounded-xl border border-rose-500/30 bg-rose-500/10 px-3 text-sm font-medium text-rose-200 transition hover:bg-rose-500/20 disabled:opacity-50"
                    >
                      中止
                    </button>

                    <button
                      type="button"
                      onClick={handlePauseCrawl}
                      disabled={!crawlJobId || crawlJobStatus !== "running" || crawling === false}
                      className="h-10 w-[120px] flex-none rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                    >
                      中断
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {crawlConfirmOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => !crawling && setCrawlConfirmOpen(false)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      クローリング確認
                    </div>

                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() => setCrawlFieldSelections(createInitialCrawlFieldSelections())}
                        disabled={crawling}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        全選択
                      </button>

                      <button
                        type="button"
                        onClick={() => setCrawlFieldSelections(createEmptyCrawlFieldSelections())}
                        disabled={crawling}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        選択解除
                      </button>
                    </div>
                  </div>

                  <div className="px-4 py-6">
                    <div className="mb-4 text-sm leading-7 text-slate-300">
                      チェックした項目のみクローリングします。
                      <br />
                      保存前にクローリング結果を一覧で表示し、内容を確認してから保存できます。
                      <br />
                      <span className="text-xs text-slate-400">
                      対象:
                      {crawlTargetScope === "all"
                        ? "全てのリスト"
                        : "絞り込みリストのみ"}
                    </span>
                    </div>

                    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-4">
                      {CRAWL_CONFIRM_FIELD_OPTIONS.map((field) => (
                        <label
                          key={field.key}
                          className="flex items-center gap-3 rounded-xl border border-white/10 bg-white/5 px-3 py-3 text-sm text-slate-200"
                        >
                          <input
                            type="checkbox"
                            checked={crawlFieldSelections[field.key]}
                            onChange={() =>
                              setCrawlFieldSelections((prev) => ({
                                ...prev,
                                [field.key]: !prev[field.key],
                              }))
                            }
                            className="h-4 w-4 accent-amber-500"
                          />
                          <span>{field.label}</span>
                        </label>
                      ))}
                    </div>
                  </div>

                  <div className="flex justify-center gap-3 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setCrawlConfirmOpen(false)}
                      disabled={crawling}
                      className="h-10 w-[120px] flex-none rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleCrawl}
                      disabled={crawling || !hasSelectedCrawlFields}
                      className={`h-10 w-[120px] flex-none rounded-xl px-3 text-sm font-medium text-white transition disabled:cursor-not-allowed disabled:opacity-100 ${
                        !hasSelectedCrawlFields
                          ? "bg-amber-500/30 text-white/60"
                          : "bg-amber-500 hover:bg-amber-400"
                      }`}
                    >
                      {crawling ? "実行中..." : "はい"}
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

          {crawlPreviewOpen &&
            typeof document !== "undefined" &&
            createPortal(
              <div className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6">
                <div className="flex min-h-full items-center justify-center">
                  <div
                    className="flex w-full max-w-[1100px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                      クローリング結果確認
                    </div>

                    <div className="border-b border-white/10 px-4 py-4">
                      <div
                        className={`grid gap-3 ${
                          crawlJobStatus === "paused"
                            ? "grid-cols-2 md:grid-cols-4"
                            : "grid-cols-1 md:grid-cols-3"
                        }`}
                      >
                        <div className="rounded-xl border border-white/10 bg-[#0f172a] px-4 py-3">
                          <div className="text-xs text-slate-400">完了件数</div>
                          <div className="mt-1 text-lg font-semibold text-slate-100">
                            {crawlProcessedCount.toLocaleString()}件
                          </div>
                        </div>

                        <div className="rounded-xl border border-white/10 bg-[#0f172a] px-4 py-3">
                          <div className="text-xs text-slate-400">1件あたりの平均取得時間</div>
                          <div className="mt-1 text-lg font-semibold text-slate-100">
                            {formatCrawlDuration(averageCrawlMs)}
                          </div>
                        </div>

                        <div className="rounded-xl border border-white/10 bg-[#0f172a] px-4 py-3">
                          <div className="text-xs text-slate-400">全体の取得の総時間</div>
                          <div className="mt-1 text-lg font-semibold text-slate-100">
                            {formatCrawlDuration(crawlElapsedMs)}
                          </div>
                        </div>

                        {crawlJobStatus === "paused" && (
                          <div className="rounded-xl border border-white/10 bg-[#0f172a] px-4 py-3">
                            <div className="text-xs text-slate-400">残り件数</div>
                            <div className="mt-1 text-lg font-semibold text-slate-100">
                              {crawlRemainingCount.toLocaleString()}件
                            </div>
                          </div>
                        )}
                      </div>
                    </div>

                    <div className="border-b border-white/10 px-4 py-3">
                      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                        <div className="text-xs text-slate-400">
                          保存候補 {crawlPreviewTotalCount.toLocaleString()}件中{" "}
                          {crawlPreviewTotalCount === 0
                            ? 0
                            : (crawlPreviewPage - 1) * CRAWL_PREVIEW_PAGE_SIZE + 1}
                          〜
                          {Math.min(
                            crawlPreviewPage * CRAWL_PREVIEW_PAGE_SIZE,
                            crawlPreviewTotalCount
                          )}
                          件を表示
                        </div>

                        <div className="flex items-center gap-2">
                          <button
                            type="button"
                            onClick={() =>
                              void handleCrawlPreviewPageChange(crawlPreviewPage - 1)
                            }
                            disabled={crawlPreviewPage === 1 || crawlPreviewLoading}
                            className="h-8 rounded-lg border border-white/10 bg-white/5 px-3 text-xs text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                          >
                            前へ
                          </button>

                          <div className="text-xs text-slate-300">
                            {crawlPreviewPage} / {crawlPreviewTotalPages}
                            {crawlPreviewLoading ? " 読込中..." : ""}
                          </div>

                          <button
                            type="button"
                            onClick={() =>
                              void handleCrawlPreviewPageChange(crawlPreviewPage + 1)
                            }
                            disabled={
                              crawlPreviewPage >= crawlPreviewTotalPages ||
                              crawlPreviewLoading
                            }
                            className="h-8 rounded-lg border border-white/10 bg-white/5 px-3 text-xs text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                          >
                            次へ
                          </button>
                        </div>
                      </div>
                    </div>

                    <div className="max-h-[70vh] overflow-y-auto px-4 py-4 space-y-4">
                      {crawlPreviewLoading ? (
                        <div className="px-4 py-12 text-center text-slate-400">
                          クローリング結果確認を読み込み中です...
                        </div>
                      ) : crawlPreviewRows.length === 0 ? (
                        <div className="px-4 py-12 text-center text-slate-500">
                          保存候補はありません
                        </div>
                      ) : (
                        crawlPreviewRows.map((row, rowIndex) => (
                          <div
                            key={row.preview_row_id || `${row.row_id}-${rowIndex}`}
                            className="rounded-xl border border-white/10 bg-[#0f172a] p-4"
                          >
                            <div className="text-sm font-semibold text-slate-100">
                              {row.company || "(企業名なし)"}
                            </div>

                            <div className="mt-1 text-xs break-all">
                              {row.website_url ? (
                                <a
                                  href={row.website_url}
                                  target="_blank"
                                  rel="noreferrer"
                                  className="text-sky-300 underline underline-offset-2 transition hover:text-sky-200"
                                  title={row.website_url}
                                >
                                  {row.website_url}
                                </a>
                              ) : (
                                <span className="text-slate-500">-</span>
                              )}
                            </div>

                            <div className="mt-4 space-y-2">
                              <div className="grid gap-2 md:grid-cols-[72px_160px_1fr_1fr]">
                                <div className="rounded-lg bg-white/5 px-3 py-2 text-center text-xs font-semibold text-slate-300">
                                  反映
                                </div>
                                <div className="rounded-lg bg-white/5 px-3 py-2 text-xs font-semibold text-slate-300">
                                  項目
                                </div>
                                <div className="rounded-lg bg-white/5 px-3 py-2 text-xs font-semibold text-slate-300">
                                  変更前
                                </div>
                                <div className="rounded-lg bg-white/5 px-3 py-2 text-xs font-semibold text-slate-300">
                                  変更候補
                                </div>
                              </div>

                              {row.changes.map((change, changeIndex) => {
                                const selectedValue = getCrawlPreviewSelectedValue(
                                  crawlSelectedChanges,
                                  row.preview_row_id,
                                  change
                                );

                                const checked = selectedValue !== null;
                                const displayValue =
                                  change.after ?? change.candidates[0] ?? null;
                                const hasMultipleCandidates =
                                  change.candidates.length > 1;

                                return (
                                  <div
                                    key={`${row.preview_row_id}-${change.key}-${changeIndex}`}
                                    className="grid gap-2 md:grid-cols-[72px_160px_1fr_1fr]"
                                  >
                                    <div className="flex items-center justify-center rounded-lg bg-white/5 px-2 py-2">
                                      <input
                                        type="checkbox"
                                        checked={checked}
                                        onChange={() =>
                                          toggleCrawlPreviewReflect(
                                            row.preview_row_id,
                                            change,
                                            displayValue
                                          )
                                        }
                                        className="h-4 w-4 accent-amber-500"
                                      />
                                    </div>

                                    <div className="rounded-lg bg-white/5 px-3 py-2 text-xs font-medium text-slate-200">
                                      {change.label}
                                    </div>

                                    <div className="rounded-lg border border-rose-500/20 bg-rose-500/10 px-3 py-2 text-xs text-rose-100">
                                      <PreviewChangeValue
                                        changeKey={change.key}
                                        value={change.before}
                                      />
                                    </div>

                                    <div className="rounded-lg border border-emerald-500/20 bg-emerald-500/10 px-3 py-2 text-xs text-emerald-100">
                                      {hasMultipleCandidates ? (
                                        <div className="space-y-2">
                                          {change.candidates.map(
                                            (candidate, candidateIndex) => {
                                              const candidateChecked =
                                                selectedValue === candidate;

                                              return (
                                                <label
                                                  key={`${row.preview_row_id}-${change.key}-${candidateIndex}`}
                                                  className="flex items-start gap-2"
                                                >
                                                  <input
                                                    type="checkbox"
                                                    checked={candidateChecked}
                                                    onChange={() =>
                                                      toggleCrawlPreviewCandidate(
                                                        row.preview_row_id,
                                                        change,
                                                        candidate
                                                      )
                                                    }
                                                    className="mt-0.5 h-4 w-4 accent-amber-500"
                                                  />
                                                  <span className="break-all">
                                                    {candidate}
                                                  </span>
                                                </label>
                                              );
                                            }
                                          )}
                                        </div>
                                      ) : (
                                        <PreviewChangeValue
                                          changeKey={change.key}
                                          value={displayValue}
                                        />
                                      )}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>
                          </div>
                        ))
                      )}
                    </div>

                    <div className="flex justify-center gap-3 border-t border-white/10 px-4 py-4">
                      <button
                        type="button"
                        onClick={() => setCrawlPreviewOpen(false)}
                        disabled={crawling}
                        className="h-10 w-[120px] flex-none rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        中止
                      </button>

                      {crawlJobStatus === "paused" && crawlJobId && (
                        <button
                          type="button"
                          onClick={handleResumeCrawl}
                          disabled={crawling}
                          className="h-10 w-[120px] flex-none rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400 disabled:opacity-50"
                        >
                          再開
                        </button>
                      )}

                      <button
                        type="button"
                        onClick={handleCrawlSave}
                        disabled={crawling}
                        className="h-10 w-[120px] flex-none rounded-xl bg-amber-500 px-3 text-sm font-medium text-white transition hover:bg-amber-400 disabled:opacity-50"
                      >
                        {crawling ? "保存中..." : "保存"}
                      </button>
                    </div>
                  </div>
                </div>
              </div>,
              document.body
            )}

        {crawlResumeConfirmOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6">
              <div className="flex min-h-full items-center justify-center">
                <div className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl">
                  <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                    再開確認
                  </div>

                  <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                    途中までのクローリング結果を保存しました。
                    <br />
                    途中から再開しますか？
                  </div>

                  <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => {
                        setCrawlResumeConfirmOpen(false);
                        setCrawlJobId(null);
                        saveActiveCrawlJobId(null);
                        setCrawlJobStatus("idle");
                        setCrawlRemainingCount(0);
                      }}
                      className="h-10 flex-1 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleResumeCrawl}
                      className="h-10 flex-1 rounded-xl bg-amber-500 px-3 text-sm font-medium text-white transition hover:bg-amber-400"
                    >
                      はい
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {dedupeConfirmOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => !deduplicating && setDedupeConfirmOpen(false)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                    重複削除確認
                  </div>

                  <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                    企業名の完全一致で重複データを削除します。
                    <br />
                    本当に重複削除しますか？
                    <br />
                    <span className="text-xs text-slate-400">
                      対象:
                      {dedupeTargetScope === "all"
                        ? "全てのリスト"
                        : "絞り込みリストのみ"}
                    </span>
                  </div>

                  <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setDedupeConfirmOpen(false)}
                      disabled={deduplicating}
                      className="h-10 flex-1 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleDeduplicate}
                      disabled={deduplicating}
                      className="h-10 flex-1 rounded-xl bg-violet-500 px-3 text-sm font-medium text-white transition hover:bg-violet-400 disabled:opacity-50"
                    >
                      {deduplicating ? "実行中..." : "はい"}
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {importConfirmOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => !importing && setImportConfirmOpen(false)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      CSV投入確認
                    </div>

                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() =>
                          setCheckedImportFiles(
                            Object.fromEntries(
                              selectedFiles.map((file) => [buildImportFileKey(file), true])
                            )
                          )
                        }
                        disabled={importing}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        全選択
                      </button>

                      <button
                        type="button"
                        onClick={() =>
                          setCheckedImportFiles(
                            Object.fromEntries(
                              selectedFiles.map((file) => [buildImportFileKey(file), false])
                            )
                          )
                        }
                        disabled={importing}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        選択解除
                      </button>
                    </div>
                  </div>

                  <div className="px-4 py-6">
                    <div className="mb-4 text-sm leading-7 text-slate-300">
                      チェックしたCSVのみ投入します。
                      <br />
                      本当にCSVを投入しますか？
                    </div>

                    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                      {selectedFiles.map((file, index) => {
                        const fileKey = buildImportFileKey(file);

                        return (
                          <label
                            key={`${file.name}-${file.size}-${file.lastModified}-${index}`}
                            className="flex items-center gap-3 rounded-xl border border-white/10 bg-white/5 px-3 py-3 text-sm text-slate-200"
                          >
                            <input
                              type="checkbox"
                              checked={checkedImportFiles[fileKey] !== false}
                              onChange={() =>
                                setCheckedImportFiles((prev) => ({
                                  ...prev,
                                  [fileKey]: !(prev[fileKey] !== false),
                                }))
                              }
                              className="h-4 w-4 accent-emerald-500"
                            />
                            <span className="min-w-0 flex-1 truncate" title={file.name}>
                              {file.name}
                            </span>
                          </label>
                        );
                      })}
                    </div>
                  </div>

                  <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setImportConfirmOpen(false)}
                      disabled={importing}
                      className="h-10 flex-1 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={() => {
                        setImportConfirmOpen(false);
                        setImportDuplicateConfirmOpen(true);
                      }}
                      disabled={importing}
                      className="h-10 flex-1 rounded-xl bg-emerald-500 px-3 text-sm font-medium text-white transition hover:bg-emerald-400 disabled:opacity-50"
                    >
                      {importing ? "投入中..." : "はい"}
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {importDuplicateConfirmOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
              onClick={() => !importing && setImportDuplicateConfirmOpen(false)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                    CSV投入 重複削除確認
                  </div>

                  <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                    投入するCSV内で、企業名の完全一致を重複削除しますか？
                  </div>

                  <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => handleImport(false)}
                      disabled={importing}
                      className="h-10 flex-1 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                    >
                      重複削除しない
                    </button>

                    <button
                      type="button"
                      onClick={() => handleImport(true)}
                      disabled={importing}
                      className="h-10 flex-1 rounded-xl bg-emerald-500 px-3 text-sm font-medium text-white transition hover:bg-emerald-400 disabled:opacity-50"
                    >
                      {importing ? "投入中..." : "重複削除する"}
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

          {exportScopeOpen &&
            typeof document !== "undefined" &&
            createPortal(
              <div
                className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
                onClick={() => setExportScopeOpen(false)}
              >
                <div className="flex min-h-full items-center justify-center">
                  <div
                    className="flex w-full max-w-[640px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                      <div className="text-sm font-semibold text-slate-100">
                        CSV抽出対象選択
                      </div>

                      <button
                        type="button"
                        onClick={() => setExportScopeOpen(false)}
                        className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                      >
                        ×
                      </button>
                    </div>

                    <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                      CSVを抽出する対象を選択してください。
                    </div>

                    <div className="grid gap-2 border-t border-white/10 px-4 py-4 sm:grid-cols-2">
                      <button
                        type="button"
                        onClick={() => {
                          setExportMode("all");
                          setExportScopeOpen(false);
                          setExportConfirmOpen(true);
                        }}
                        className="h-10 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400"
                      >
                        全てのリスト
                      </button>

                      <button
                        type="button"
                        onClick={() => {
                          setExportMode("filtered");
                          setExportScopeOpen(false);
                          setExportConfirmOpen(true);
                        }}
                        className="h-10 rounded-xl bg-emerald-500 px-3 text-sm font-medium text-white transition hover:bg-emerald-400"
                      >
                        絞り込みリストのみ
                      </button>
                    </div>
                  </div>
                </div>
              </div>,
              document.body
            )}

          {exportConfirmOpen &&
            typeof document !== "undefined" &&
            createPortal(
              <div
                className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
                onClick={() => setExportConfirmOpen(false)}
              >
                <div className="flex min-h-full items-center justify-center">
                  <div
                    className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                      CSV抽出確認
                    </div>

                    <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                      本当にCSVを抽出しますか？
                      <br />
                      <span className="text-xs text-slate-400">
                        対象: {exportMode === "all"
                          ? "全てのリスト"
                          : "絞り込みリストのみ"}
                      </span>
                    </div>

                    <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                      <button
                        type="button"
                        onClick={() => setExportConfirmOpen(false)}
                        className="h-10 flex-1 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10"
                      >
                        いいえ
                      </button>

                      <button
                        type="button"
                        onClick={() => {
                          setExportConfirmOpen(false);
                          setExportDuplicateConfirmOpen(true);
                        }}
                        className="h-10 flex-1 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400"
                      >
                        はい
                      </button>
                    </div>
                  </div>
                </div>
              </div>,
              document.body
            )}

          {exportDuplicateConfirmOpen &&
            typeof document !== "undefined" &&
            createPortal(
              <div
                className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
                onClick={() => setExportDuplicateConfirmOpen(false)}
              >
                <div className="flex min-h-full items-center justify-center">
                  <div
                    className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                      CSV抽出 重複削除確認
                    </div>

                    <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                      抽出するCSV内で、企業名の完全一致を重複削除しますか？
                      <br />
                      <span className="text-xs text-slate-400">
                        対象: {exportMode === "all"
                          ? "全てのリスト"
                          : "絞り込みリストのみ"}
                      </span>
                    </div>

                    <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                      <button
                        type="button"
                        onClick={() => handleExport(false)}
                        className="h-10 flex-1 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10"
                      >
                        重複削除しない
                      </button>

                      <button
                        type="button"
                        onClick={() => handleExport(true)}
                        className="h-10 flex-1 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400"
                      >
                        重複削除する
                      </button>
                    </div>
                  </div>
                </div>
              </div>,
              document.body
            )}

            {allFiltersClearConfirmOpen &&
              typeof document !== "undefined" &&
              createPortal(
                <div
                  className="fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-4 sm:p-6"
                  onClick={() => setAllFiltersClearConfirmOpen(false)}
                >
                  <div className="flex min-h-full items-center justify-center">
                    <div
                      className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                        全フィルタ解除確認
                      </div>

                      <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                        現在適用中のフィルタをすべて解除しますか？
                      </div>

                      <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                        <button
                          type="button"
                          onClick={() => setAllFiltersClearConfirmOpen(false)}
                          className="h-10 flex-1 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10"
                        >
                          いいえ
                        </button>

                        <button
                          type="button"
                          onClick={handleConfirmAllFiltersClear}
                          className="h-10 flex-1 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400"
                        >
                          はい
                        </button>
                      </div>
                    </div>
                  </div>
                </div>,
                document.body
              )}

        {error && (
          <div className="mb-4 rounded-2xl border border-rose-500/20 bg-rose-500/10 px-4 py-3 text-sm text-rose-200">
            エラー: {error}
          </div>
        )}
        </div>

        <div className="flex min-h-0 flex-1 flex-col rounded-[24px] border border-white/10 bg-[#0b1326]/90 shadow-[0_24px_60px_rgba(0,0,0,0.35)]">
          <div className="flex-1 overflow-auto">
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
                    onLoadPreviousValues={loadPreviousFilterValues}
                    onLoadMoreValues={loadMoreFilterValues}
                    onOpenClearConfirm={(key) =>
                      setSingleFilterClearConfirm({ type: "column", key })
                    }
                    onApply={applyColumnFilter}
                    onClear={clearColumnFilter}
                  />
                ))}
              </div>

              {renderedTableBody}
            </div>
          </div>
        </div>

        <div className="mt-5 shrink-0 flex flex-wrap items-center justify-center gap-2">
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

      {/* テーマ：ライト */}
      <style jsx global>{`
        main[data-theme="light"] {
          background: #f8fafc;
          color: #0f172a;
        }

        body[data-app-theme="light"] {
          color: #0f172a;
        }

        main[data-theme="light"] [class*="bg-[#08101d]"],
        body[data-app-theme="light"] [class*="bg-[#08101d]"] {
          background: rgba(255, 255, 255, 0.96) !important;
        }

        main[data-theme="light"] [class*="bg-[#0b1326]"],
        body[data-app-theme="light"] [class*="bg-[#0b1326]"],
        main[data-theme="light"] [class*="bg-[#0b1220]"],
        body[data-app-theme="light"] [class*="bg-[#0b1220]"],
        main[data-theme="light"] [class*="bg-[#0f172a]"],
        body[data-app-theme="light"] [class*="bg-[#0f172a]"],
        main[data-theme="light"] [class*="bg-[#111827]"],
        body[data-app-theme="light"] [class*="bg-[#111827]"],
        main[data-theme="light"] [class*="bg-[#162033]"],
        body[data-app-theme="light"] [class*="bg-[#162033]"] {
          background: #ffffff !important;
        }

        main[data-theme="light"] [class*="bg-slate-950/70"],
        body[data-app-theme="light"] [class*="bg-slate-950/70"] {
          background: rgba(15, 23, 42, 0.18) !important;
        }

        main[data-theme="light"] [class*="bg-white/5"],
        body[data-app-theme="light"] [class*="bg-white/5"] {
          background: rgba(15, 23, 42, 0.04) !important;
        }

        main[data-theme="light"] [class*="border-white/10"],
        body[data-app-theme="light"] [class*="border-white/10"] {
          border-color: rgba(15, 23, 42, 0.12) !important;
        }

        main[data-theme="light"] [class*="border-white/5"],
        body[data-app-theme="light"] [class*="border-white/5"] {
          border-color: rgba(15, 23, 42, 0.08) !important;
        }

        main[data-theme="light"] [class*="text-white"],
        body[data-app-theme="light"] [class*="text-white"],
        main[data-theme="light"] [class*="text-slate-100"],
        body[data-app-theme="light"] [class*="text-slate-100"],
        main[data-theme="light"] [class*="text-slate-200"],
        body[data-app-theme="light"] [class*="text-slate-200"] {
          color: #0f172a !important;
        }

        main[data-theme="light"] [class*="text-slate-300"],
        body[data-app-theme="light"] [class*="text-slate-300"] {
          color: #334155 !important;
        }

        main[data-theme="light"] [class*="text-slate-400"],
        body[data-app-theme="light"] [class*="text-slate-400"],
        main[data-theme="light"] [class*="text-slate-500"],
        body[data-app-theme="light"] [class*="text-slate-500"] {
          color: #64748b !important;
        }

        main[data-theme="light"] button[class*="bg-sky-500"],
        body[data-app-theme="light"] button[class*="bg-sky-500"],
        main[data-theme="light"] button[class*="bg-emerald-500"],
        body[data-app-theme="light"] button[class*="bg-emerald-500"],
        main[data-theme="light"] button[class*="bg-amber-500"],
        body[data-app-theme="light"] button[class*="bg-amber-500"],
        main[data-theme="light"] button[class*="bg-rose-500"],
        body[data-app-theme="light"] button[class*="bg-rose-500"] {
          color: #ffffff !important;
        }

        main[data-theme="light"] button[class*="bg-sky-500/20"],
        body[data-app-theme="light"] button[class*="bg-sky-500/20"],
        main[data-theme="light"] button[class*="bg-sky-500/20"] *,
        body[data-app-theme="light"] button[class*="bg-sky-500/20"] * {
          color: #0f172a !important;
        }

        main[data-theme="light"] [class*="text-sky-100"],
        body[data-app-theme="light"] [class*="text-sky-100"] {
          color: #0f172a !important;
        }

        main[data-theme="light"] input[type="checkbox"],
        body[data-app-theme="light"] input[type="checkbox"] {
          accent-color: #2563eb;
        }

        main[data-theme="light"] select,
        body[data-app-theme="light"] select,
        main[data-theme="light"] option,
        body[data-app-theme="light"] option {
          color: #0f172a !important;
          background: #ffffff !important;
        }

        main[data-theme="light"] [class*="hover:bg-white/10"]:hover,
        body[data-app-theme="light"] [class*="hover:bg-white/10"]:hover,
        main[data-theme="light"] [class*="hover:bg-white/5"]:hover,
        body[data-app-theme="light"] [class*="hover:bg-white/5"]:hover {
          background: rgba(15, 23, 42, 0.08) !important;
        }

        main[data-theme="dark"] .master-data-brand-logo,
        body[data-app-theme="dark"] .master-data-brand-logo {
          --mdb-gold-1: #f8e7c5;
          --mdb-gold-2: #ddb879;
          --mdb-gold-3: #b98542;
          --mdb-gold-text: #f1d4a4;
          filter:
            drop-shadow(0 18px 36px rgba(0, 0, 0, 0.34))
            drop-shadow(0 4px 14px rgba(247, 227, 191, 0.18));
        }

        main[data-theme="light"] .master-data-brand-logo,
        body[data-app-theme="light"] .master-data-brand-logo {
          --mdb-gold-1: #d8ad64;
          --mdb-gold-2: #b27c35;
          --mdb-gold-3: #7e5320;
          --mdb-gold-text: #8f6327;
          filter:
            drop-shadow(0 14px 28px rgba(15, 23, 42, 0.18))
            drop-shadow(0 2px 10px rgba(255, 247, 230, 0.96));
        }

        .master-data-brand-logo__gold-stop-1 {
          stop-color: var(--mdb-gold-1);
        }

        .master-data-brand-logo__gold-stop-2 {
          stop-color: var(--mdb-gold-2);
        }

        .master-data-brand-logo__gold-stop-3 {
          stop-color: var(--mdb-gold-3);
        }

        .master-data-brand-logo__label,
        .master-data-brand-logo__sub {
          fill: var(--mdb-gold-text);
        }

        .master-data-brand-logo__display {
          font-family:
            "Cormorant Garamond",
            "Bodoni Moda",
            "Didot",
            "Times New Roman",
            serif;
          text-rendering: geometricPrecision;
        }

        .master-data-brand-logo__display-alt {
          font-family:
            "Cormorant Garamond",
            "Playfair Display",
            "Bodoni Moda",
            "Didot",
            "Times New Roman",
            serif;
          text-rendering: geometricPrecision;
        }

        .master-data-brand-logo__wordmark {
          font-family:
            "Playfair Display",
            "Cormorant Garamond",
            "Bodoni Moda",
            "Didot",
            "Times New Roman",
            serif;
          text-rendering: geometricPrecision;
        }

        .master-data-brand-title {
          font-family:
            "Cormorant Garamond",
            "Bodoni Moda",
            "Didot",
            "Yu Mincho",
            "Hiragino Mincho ProN",
            "MS PMincho",
            serif;
          font-weight: 600;
          letter-spacing: 0.08em;
          line-height: 0.95;
          white-space: nowrap;
        }

        main[data-theme="dark"] .master-data-brand-title,
        body[data-app-theme="dark"] .master-data-brand-title {
          color: #f1d4a4 !important;
          text-shadow:
            0 14px 32px rgba(0, 0, 0, 0.30),
            0 2px 10px rgba(247, 227, 191, 0.12);
        }

        main[data-theme="light"] .master-data-brand-title,
        body[data-app-theme="light"] .master-data-brand-title {
          color: #8f6327 !important;
          text-shadow:
            0 10px 24px rgba(15, 23, 42, 0.12),
            0 1px 0 rgba(255, 255, 255, 0.70);
        }
      `}</style>

    </main>
  );
}