"use client";

import { type FormEvent, type ReactNode, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
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
  permit_number: string | null;
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

type DedupeMatchMethod = "exact";

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
  { key: "worker_dispatch_license", label: "労働者派遣" },
  { key: "paid_job_placement_license", label: "有料職業紹介" },
] as const;

type CrawlFieldKey =
  (typeof CRAWL_CONFIRM_FIELD_OPTIONS)[number]["key"];

type CrawlPreviewFieldKey = CrawlFieldKey | "permit_number";

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
  key: CrawlPreviewFieldKey;
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
  source_row: Row | null;
  changes: CrawlPreviewChange[];
};

type CrawlSelectedChangeValue = string | null;
type CrawlSelectedChanges = Record<
  string,
  Partial<Record<CrawlPreviewFieldKey, CrawlSelectedChangeValue>>
>;

type ItemInspectionMethodKey =
  | "representative_name_remove_non_name"
  | "representative_name_inspect_name";

type ItemInspectionPreviewChange = {
  rowId: string;
  company: string | null;
  fieldLabel: string;
  beforeValue: string | null;
  afterValue: string | null;
  action: "update" | "delete" | "review" | "none";
  reason: string;
  source_row: Row | null;
};

function createInitialItemInspectionMethodSelections(): Record<
  ItemInspectionMethodKey,
  boolean
> {
  return {
    representative_name_remove_non_name: false,
    representative_name_inspect_name: false,
  };
}

function createEmptyItemInspectionMethodSelections(): Record<
  ItemInspectionMethodKey,
  boolean
> {
  return {
    representative_name_remove_non_name: false,
    representative_name_inspect_name: false,
  };
}

type MasterDataLoginRole = "スーパー管理者" | "管理者" | "従業員";

type MasterDataLoginUser = {
  id: string;
  password: string;
  name: string;
  organization: string;
  role: MasterDataLoginRole;
};

type MasterDataLoginHistoryItem = {
  loggedAt: string;
  ipAddress: string;
  browser: string;
};

type CrawlFieldPermissionKey = `inspection.crawlField.${CrawlFieldKey}`;
type ItemInspectionFieldPermissionKey =
  `inspection.itemInspectionField.${FilterKey}`;

type MasterDataPermissionKey =
  | "search.companyName"
  | "search.prefecture"
  | "search.industry"
  | "search.established"
  | "search.capital"
  | "search.employeeCount"
  | "search.tag"
  | "search.columnFilters"
  | "list.add"
  | "list.delete"
  | "list.itemDelete"
  | "list.dedupe"
  | "csv.import"
  | "csv.export"
  | "csv.template"
  | "inspection.crawl"
  | "inspection.itemInspection"
  | CrawlFieldPermissionKey
  | ItemInspectionFieldPermissionKey;

type MasterDataPermissions = Partial<
  Record<MasterDataPermissionKey, boolean>
>;

type MasterDataPermissionEmployee = {
  id: string;
  name: string;
  role: MasterDataLoginRole;
  organization: string;
  permissions: MasterDataPermissions;
  allowedFilters: Record<string, unknown>;
};

type PermissionListScopeFilters = {
  filterModels: Record<string, unknown>;
  advancedFilters: Record<string, unknown>;
  sortKey?: FilterKey;
  sortDirection?: SortDirection;
};

const CRAWL_PERMISSION_FIELD_ITEMS: {
  key: MasterDataPermissionKey;
  label: string;
}[] = CRAWL_CONFIRM_FIELD_OPTIONS.map((field) => ({
  key: `inspection.crawlField.${field.key}` as MasterDataPermissionKey,
  label: field.label,
}));

const ITEM_INSPECTION_PERMISSION_FIELD_ITEMS: {
  key: MasterDataPermissionKey;
  label: string;
}[] = [
  { key: "inspection.itemInspectionField.company", label: "企業名" },
  { key: "inspection.itemInspectionField.zipcode", label: "郵便番号" },
  { key: "inspection.itemInspectionField.address", label: "住所" },
  { key: "inspection.itemInspectionField.big_industry", label: "大業種" },
  { key: "inspection.itemInspectionField.small_industry", label: "小業種" },
  { key: "inspection.itemInspectionField.company_kana", label: "企業名（かな）" },
  { key: "inspection.itemInspectionField.summary", label: "企業概要" },
  { key: "inspection.itemInspectionField.website_url", label: "企業URL" },
  { key: "inspection.itemInspectionField.form_url", label: "お問い合わせフォームURL" },
  { key: "inspection.itemInspectionField.phone", label: "電話番号" },
  { key: "inspection.itemInspectionField.fax", label: "FAX番号" },
  { key: "inspection.itemInspectionField.email", label: "メールアドレス" },
  { key: "inspection.itemInspectionField.established_date", label: "設立年月" },
  { key: "inspection.itemInspectionField.representative_name", label: "代表者名" },
  { key: "inspection.itemInspectionField.representative_title", label: "代表者役職" },
  { key: "inspection.itemInspectionField.capital", label: "資本金" },
  { key: "inspection.itemInspectionField.employee_count", label: "従業員数" },
  { key: "inspection.itemInspectionField.employee_count_year", label: "従業員数年度" },
  { key: "inspection.itemInspectionField.previous_sales", label: "前年売上高" },
  { key: "inspection.itemInspectionField.latest_sales", label: "直近売上高" },
  { key: "inspection.itemInspectionField.closing_month", label: "決算月" },
  { key: "inspection.itemInspectionField.office_count", label: "事業所数" },
  { key: "inspection.itemInspectionField.tag", label: "タグ" },
  { key: "inspection.itemInspectionField.business_type", label: "業種" },
  { key: "inspection.itemInspectionField.business_content", label: "事業内容" },
  { key: "inspection.itemInspectionField.industry_category", label: "業界" },
  { key: "inspection.itemInspectionField.permit_number", label: "許可番号" },
  { key: "inspection.itemInspectionField.memo", label: "メモ" },
];

const LIST_DATA_PERMISSION_ITEMS: {
  key: MasterDataPermissionKey;
  label: string;
}[] = [
  { key: "search.columnFilters", label: "リスト内フィルタ" },
];

const MASTER_DATA_PERMISSION_GROUPS: {
  title: string;
  items: { key: MasterDataPermissionKey; label: string }[];
}[] = [
  {
    title: "検索",
    items: [
      { key: "search.companyName", label: "企業名" },
      { key: "search.prefecture", label: "都道府県" },
      { key: "search.industry", label: "業種" },
      { key: "search.established", label: "設立" },
      { key: "search.capital", label: "資本金" },
      { key: "search.employeeCount", label: "従業員数" },
      { key: "search.tag", label: "タグ" },
    ],
  },
  {
    title: "リスト",
    items: [
      { key: "list.add", label: "リスト追加" },
      { key: "list.delete", label: "リスト削除" },
      { key: "list.itemDelete", label: "項目削除" },
      { key: "list.dedupe", label: "重複削除" },
    ],
  },
  {
    title: "CSV",
    items: [
      { key: "csv.import", label: "CSV投入" },
      { key: "csv.export", label: "CSV抽出" },
      { key: "csv.template", label: "CSVテンプレート" },
    ],
  },
  {
    title: "精査",
    items: [
      { key: "inspection.crawl", label: "クローリング" },
      { key: "inspection.itemInspection", label: "項目精査" },
    ],
  },
  {
    title: "クローリング項目",
    items: CRAWL_PERMISSION_FIELD_ITEMS,
  },
  {
    title: "項目精査 項目",
    items: ITEM_INSPECTION_PERMISSION_FIELD_ITEMS,
  },
];

function createPermissionValues(checked: boolean): MasterDataPermissions {
  return [
    ...LIST_DATA_PERMISSION_ITEMS.map((item) => item.key),
    ...MASTER_DATA_PERMISSION_GROUPS.flatMap((group) =>
      group.items.map((item) => item.key)
    ),
  ].reduce((acc, permissionKey) => {
    acc[permissionKey] = checked;
    return acc;
  }, {} as MasterDataPermissions);
}

const CRAWL_FIELD_PERMISSION_KEYS = CRAWL_PERMISSION_FIELD_ITEMS.map(
  (item) => item.key
);

const ITEM_INSPECTION_FIELD_PERMISSION_KEYS =
  ITEM_INSPECTION_PERMISSION_FIELD_ITEMS.map((item) => item.key);

function hasAnyCheckedPermission(
  permissions: MasterDataPermissions,
  permissionKeys: MasterDataPermissionKey[]
) {
  return permissionKeys.some((permissionKey) => permissions[permissionKey] !== false);
}

function syncInspectionParentPermissions(
  permissions: MasterDataPermissions
): MasterDataPermissions {
  const next = { ...permissions };

  next["inspection.crawl"] = hasAnyCheckedPermission(
    next,
    CRAWL_FIELD_PERMISSION_KEYS
  );

  next["inspection.itemInspection"] = hasAnyCheckedPermission(
    next,
    ITEM_INSPECTION_FIELD_PERMISSION_KEYS
  );

  return next;
}

type ApiResponse = {
  ok: boolean;
  loginUser?: MasterDataLoginUser | null;
  loginHistoryEvent?: MasterDataLoginHistoryItem;
  loginHistory?: MasterDataLoginHistoryItem[];
  employees?: MasterDataPermissionEmployee[];
  employee?: MasterDataPermissionEmployee;
  permissions?: MasterDataPermissions;
  allowedFilters?: Record<string, unknown>;
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
  elapsedMs?: number;
  elapsedSeconds?: number;
  averageSecondsPerItem?: number;

  mynaviTotalPages?: number;
  mynaviSelectedPageCount?: number;
  mynaviTotalUrls?: number;
  mynaviProcessedCount?: number;
  mynaviSuccessCount?: number;
  mynaviFailedCount?: number;
  mynaviCurrentPageNumber?: number;
  mynaviCsvFileName?: string;
  mynaviGradYear?: string;
  mynaviPhase?: "idle" | "collect_urls" | "scrape_details" | "completed" | "error";
  mynaviJobMode?: "count_pages" | "scrape";
  mynaviCurrentField?: string | null;
  mynaviCurrentCompany?: string | null;
  mynaviCurrentCompanyIndex?: number;
  mynaviDetectedTotalPages?: number;
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

type MynaviJobStatus = "idle" | "running" | "paused" | "completed" | "error";

type MynaviJobMode = "count_pages" | "scrape";

type MasterDataLoginStatus = "checking" | "logged_in" | "logged_out";

type MasterDataNoticeTone = "working" | "success" | "error";

type MasterDataNoticeItem = {
  key: string;
  tone: MasterDataNoticeTone;
  title: string;
  message?: ReactNode;
  loading?: boolean;
};

const MYNAVI_FIELD_LABELS: Record<string, string> = {
  count_open: "一覧ページを開いています",
  count_parse: "ページ数を解析しています",
  prepare: "取得開始準備中",
  collect_urls: "URL一覧を取得しています",
  outline: "会社概要",
  recruit_results: "採用実績",
  contact: "問合せ先",
  sanitize: "整形",
  completed: "完了",
};

function getMynaviFieldLabel(value: string | null) {
  if (!value || value.trim() === "") {
    return "-";
  }

  return MYNAVI_FIELD_LABELS[value] ?? value;
}

function getMynaviPhaseLabel(
  mode: MynaviJobMode,
  phase:
    | "idle"
    | "count_pages"
    | "collect_urls"
    | "scrape_details"
    | "completed"
    | "error"
) {
  if (mode === "count_pages") {
    return "ページ数確認";
  }

  if (phase === "collect_urls") {
    return "URL取得";
  }

  if (phase === "scrape_details") {
    return "全項目取得";
  }

  if (phase === "completed") {
    return "完了";
  }

  if (phase === "error") {
    return "エラー";
  }

  return "準備中";
}

function buildImportFileKey(file: File) {
  return `${file.name}__${file.size}__${file.lastModified}`;
}

const CRAWL_PREVIEW_PAGE_SIZE = 20;
const ITEM_INSPECTION_PREVIEW_PAGE_SIZE = 20;

const ACTIVE_CRAWL_JOB_STORAGE_KEY = "master-data-active-crawl-job-id";

const MASTER_DATA_LOGIN_SESSION_KEY = "master-data-login-session";
const MASTER_DATA_LOGIN_USER_KEY = "master-data-login-user";

const MASTER_DATA_LOGIN_EXPIRES_AT_KEY = "master-data-login-expires-at";
const MASTER_DATA_LOGIN_SESSION_MS = 30 * 60 * 1000;

function getMasterDataLoginExpiresAt() {
  if (typeof window === "undefined") return 0;

  return Number(
    window.sessionStorage.getItem(MASTER_DATA_LOGIN_EXPIRES_AT_KEY)
  );
}

function refreshMasterDataLoginExpiresAt() {
  if (typeof window === "undefined") return;

  window.sessionStorage.setItem(
    MASTER_DATA_LOGIN_EXPIRES_AT_KEY,
    String(Date.now() + MASTER_DATA_LOGIN_SESSION_MS)
  );
}

function clearMasterDataLoginSessionStorage() {
  if (typeof window === "undefined") return;

  window.sessionStorage.removeItem(MASTER_DATA_LOGIN_SESSION_KEY);
  window.sessionStorage.removeItem(MASTER_DATA_LOGIN_EXPIRES_AT_KEY);
  window.sessionStorage.removeItem(MASTER_DATA_LOGIN_USER_KEY);
}

const LOCAL_CRAWL_WORKER_STATUS_URL = "http://127.0.0.1:39281/status";

async function fetchMasterDataLoginHistory(userId: string | undefined) {
  if (!userId) return [];

  try {
    const res = await fetch(
      `/api/master_data/login?userId=${encodeURIComponent(userId)}`,
      {
        method: "GET",
        cache: "no-store",
      }
    );

    const data = await readApiResponse(res);

    if (!res.ok || !data.ok) {
      throw new Error(data.error || "ログイン履歴の取得に失敗しました");
    }

    return data.loginHistory || [];
  } catch {
    return [];
  }
}

function splitMasterDataLoginName(name: string | undefined) {
  const trimmedName = (name ?? "").trim();

  if (trimmedName === "") {
    return {
      lastName: "-",
      firstName: "-",
    };
  }

  const parts = trimmedName.split(/[ 　]+/).filter(Boolean);

  if (parts.length >= 2) {
    return {
      lastName: parts[0],
      firstName: parts.slice(1).join(" "),
    };
  }

  return {
    lastName: trimmedName,
    firstName: "-",
  };
}

function isMasterDataManagerRole(role: MasterDataLoginRole | undefined) {
  return role === "スーパー管理者" || role === "管理者";
}

function getMasterDataRoleBadgeClass(role: MasterDataLoginRole | undefined) {
  if (role === "スーパー管理者") {
    return "border-violet-300/30 bg-violet-400/10 text-violet-100";
  }

  if (role === "従業員") {
    return "border-amber-300/30 bg-amber-400/10 text-amber-100";
  }

  return "border-sky-300/30 bg-sky-400/10 text-sky-100";
}

function getMasterDataAccountCardClass(role: MasterDataLoginRole | undefined) {
  if (role === "スーパー管理者") {
    return "border border-violet-300/20 bg-gradient-to-br from-violet-500/18 via-white/5 to-[#0b1220]";
  }

  if (role === "従業員") {
    return "border border-amber-300/20 bg-gradient-to-br from-amber-500/18 via-white/5 to-[#0b1220]";
  }

  return "border border-sky-300/20 bg-gradient-to-br from-sky-500/18 via-white/5 to-[#0b1220]";
}

function getMasterDataAccountIconClass(role: MasterDataLoginRole | undefined) {
  if (role === "スーパー管理者") {
    return "border-violet-300/25 bg-violet-400/10 text-violet-100";
  }

  if (role === "従業員") {
    return "border-amber-300/25 bg-amber-400/10 text-amber-100";
  }

  return "border-sky-300/25 bg-sky-400/10 text-sky-100";
}

function formatMasterDataLoginDate(value: string) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value || "-";
  }

  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const hh = String(date.getHours()).padStart(2, "0");
  const mi = String(date.getMinutes()).padStart(2, "0");
  const ss = String(date.getSeconds()).padStart(2, "0");

  return `${yyyy}/${mm}/${dd} ${hh}:${mi}:${ss}`;
}

function isLocalAppRuntime() {
  if (typeof window === "undefined") return false;

  return (
    window.location.hostname === "localhost" ||
    window.location.hostname === "127.0.0.1"
  );
}

type LocalCrawlWorkerStatus = {
  ok: boolean;
  workerId: string;
  workerName: string;
  apiBaseUrl: string;
  running: boolean;
  currentJobId: string | null;
  currentCompany: string | null;
  lastMessage: string;
  lastError: string | null;
  startedAt: string;
};

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

const CRAWL_ELAPSED_STORAGE_PREFIX = "master-data-crawl-elapsed-ms:";
const CRAWL_ELAPSED_LAST_SEEN_STORAGE_PREFIX =
  "master-data-crawl-elapsed-last-seen-at:";
const CRAWL_ELAPSED_RUNNING_STORAGE_PREFIX =
  "master-data-crawl-elapsed-was-running:";

function saveCrawlElapsedMs(jobId: string | null, elapsedMs: number) {
  if (typeof window === "undefined" || !jobId) return;

  window.localStorage.setItem(
    `${CRAWL_ELAPSED_STORAGE_PREFIX}${jobId}`,
    String(Math.max(Math.floor(elapsedMs), 0))
  );
}

function saveCrawlElapsedSnapshot(
  jobId: string | null,
  elapsedMs: number,
  isRunning: boolean
) {
  if (typeof window === "undefined" || !jobId) return;

  saveCrawlElapsedMs(jobId, elapsedMs);

  window.localStorage.setItem(
    `${CRAWL_ELAPSED_LAST_SEEN_STORAGE_PREFIX}${jobId}`,
    String(Date.now())
  );

  window.localStorage.setItem(
    `${CRAWL_ELAPSED_RUNNING_STORAGE_PREFIX}${jobId}`,
    isRunning ? "1" : "0"
  );
}

function loadCrawlElapsedMs(jobId: string | null) {
  if (typeof window === "undefined" || !jobId) return 0;

  const value = Number(
    window.localStorage.getItem(`${CRAWL_ELAPSED_STORAGE_PREFIX}${jobId}`)
  );

  return Number.isFinite(value) && value > 0 ? Math.floor(value) : 0;
}

function loadCrawlElapsedSnapshotMs(jobId: string | null) {
  if (typeof window === "undefined" || !jobId) return 0;

  const savedElapsedMs = loadCrawlElapsedMs(jobId);
  const wasRunning =
    window.localStorage.getItem(
      `${CRAWL_ELAPSED_RUNNING_STORAGE_PREFIX}${jobId}`
    ) === "1";

  const lastSeenAt = Number(
    window.localStorage.getItem(
      `${CRAWL_ELAPSED_LAST_SEEN_STORAGE_PREFIX}${jobId}`
    )
  );

  if (!wasRunning || !Number.isFinite(lastSeenAt) || lastSeenAt <= 0) {
    return savedElapsedMs;
  }

  return Math.max(
    savedElapsedMs + Math.max(Date.now() - lastSeenAt, 0),
    savedElapsedMs,
    0
  );
}

function deleteCrawlElapsedMs(jobId: string | null) {
  if (typeof window === "undefined" || !jobId) return;

  window.localStorage.removeItem(`${CRAWL_ELAPSED_STORAGE_PREFIX}${jobId}`);
  window.localStorage.removeItem(
    `${CRAWL_ELAPSED_LAST_SEEN_STORAGE_PREFIX}${jobId}`
  );
  window.localStorage.removeItem(
    `${CRAWL_ELAPSED_RUNNING_STORAGE_PREFIX}${jobId}`
  );
}

const GRID_TEMPLATE = `
  minmax(220px,2fr)
  minmax(140px,0.8fr)
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
  minmax(170px,1fr)
  minmax(160px,1.1fr)
  minmax(160px,1.1fr)
  minmax(120px,0.9fr)
  minmax(140px,0.9fr)
  minmax(140px,1fr)
  minmax(140px,1fr)
  minmax(320px,2.2fr)
  minmax(160px,1.1fr)
  minmax(180px,1.2fr)
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
  "許可番号",
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
  { key: "permit_number", label: "許可番号" },
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

const ADVANCED_FILTER_PANEL_META: Record<
  AdvancedFilterModalKey,
  { icon: string; description: string }
> = {
  companyName: {
    icon: "Aa",
    description: "企業名に含まれる文字で絞り込みます",
  },
  prefecture: {
    icon: "📍",
    description: "都道府県や市区町村から探します",
  },
  industry: {
    icon: "🏭",
    description: "大業種・小業種で分類します",
  },
  established: {
    icon: "📅",
    description: "設立年月の範囲で絞り込みます",
  },
  capital: {
    icon: "¥",
    description: "資本金の金額範囲で絞り込みます",
  },
  employeeCount: {
    icon: "👥",
    description: "従業員数の範囲で絞り込みます",
  },
  tag: {
    icon: "＃",
    description: "登録済みタグでリストを探します",
  },
};

type SidebarPanelKey =
  | "search"
  | "list"
  | "csv"
  | "inspection"
  | "theme"
  | "settings";

type FilterClearConfirmTarget =
  | { type: "column"; key: FilterKey }
  | { type: "advanced"; key: AdvancedFilterModalKey };

type InspectionCancelConfirmTarget =
  | "crawlProgress"
  | "crawlPreview"
  | "itemInspectionProgress"
  | "itemInspectionPreview";

const SIDEBAR_PANEL_TITLES: Record<SidebarPanelKey, string> = {
  search: "絞り込み",
  list: "リスト",
  csv: "CSV取込",
  inspection: "精査",
  theme: "テーマ",
  settings: "設定",
};

const SIDEBAR_MENU_ITEMS: { key: SidebarPanelKey; label: string }[] = [
  { key: "search", label: "検索" },
  { key: "list", label: "リスト" },
  { key: "csv", label: "CSV" },
  { key: "inspection", label: "精査" },
  { key: "theme", label: "テーマ" },
  { key: "settings", label: "設定" },
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

    if (menuKey === "settings") {
    return (
      <svg
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
        className={className}
      >
        <path d="M12 15.5A3.5 3.5 0 1 0 12 8a3.5 3.5 0 0 0 0 7.5z" />
        <path d="M19.4 15a1.8 1.8 0 0 0 .36 1.98l.06.06a2.1 2.1 0 0 1-2.97 2.97l-.06-.06a1.8 1.8 0 0 0-1.98-.36 1.8 1.8 0 0 0-1.1 1.66V21a2.1 2.1 0 0 1-4.2 0v-.09a1.8 1.8 0 0 0-1.1-1.66 1.8 1.8 0 0 0-1.98.36l-.06.06a2.1 2.1 0 0 1-2.97-2.97l.06-.06A1.8 1.8 0 0 0 3.6 15a1.8 1.8 0 0 0-1.66-1.1H1.85a2.1 2.1 0 0 1 0-4.2h.09A1.8 1.8 0 0 0 3.6 8.6a1.8 1.8 0 0 0-.36-1.98l-.06-.06a2.1 2.1 0 1 1 2.97-2.97l.06.06a1.8 1.8 0 0 0 1.98.36h.01A1.8 1.8 0 0 0 9.3 2.35V2.1a2.1 2.1 0 0 1 4.2 0v.25a1.8 1.8 0 0 0 1.1 1.66h.01a1.8 1.8 0 0 0 1.98-.36l.06-.06a2.1 2.1 0 1 1 2.97 2.97l-.06.06a1.8 1.8 0 0 0-.36 1.98v.01a1.8 1.8 0 0 0 1.66 1.1h.25a2.1 2.1 0 0 1 0 4.2h-.25A1.8 1.8 0 0 0 19.4 15z" />
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
      preserveAspectRatio="xMidYMid slice"
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

function MasterDataLoginWordmark({
  className = "",
}: {
  className?: string;
}) {
  return (
    <svg
      viewBox="0 0 520 120"
      className={`master-data-brand-logo ${className}`}
      role="img"
      aria-label="MASTER DATA"
    >
      <text
        x="260"
        y="78"
        textAnchor="middle"
        className="master-data-brand-logo__wordmark"
        fill="#6f471b"
        fontSize="58"
        fontWeight="800"
        letterSpacing="7"
        style={{
          filter:
            "drop-shadow(0 4px 5px rgba(15, 23, 42, 0.22)) drop-shadow(0 1px 0 rgba(255, 255, 255, 0.9))",
        }}
      >
        MASTER DATA
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

function getSingleLineFitFontSize(value: string) {
  const length = Array.from(value.replace(/\s/g, "")).length;

  if (length >= 22) return "0.7em";
  if (length >= 18) return "0.76em";
  if (length >= 14) return "0.84em";
  if (length >= 11) return "0.92em";

  return undefined;
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
      className={`app-table-cell px-4 py-3 text-sm text-slate-100 ${className}`}
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

type SelectionOptionTone =
  | "sky"
  | "emerald"
  | "amber"
  | "cyan"
  | "violet"
  | "rose";

const SELECTION_OPTION_TONE_CLASSES: Record<
  SelectionOptionTone,
  {
    card: string;
    icon: string;
    badge: string;
  }
> = {
  sky: {
    card: "border-sky-300/20 bg-gradient-to-br from-sky-500/18 via-[#0f172a] to-[#0b1220] hover:border-sky-300/40 hover:bg-sky-500/10",
    icon: "border-sky-300/25 bg-sky-400/10 text-sky-100",
    badge: "border-sky-300/25 bg-sky-400/10 text-sky-100",
  },
  emerald: {
    card: "border-emerald-300/20 bg-gradient-to-br from-emerald-500/18 via-[#0f172a] to-[#0b1220] hover:border-emerald-300/40 hover:bg-emerald-500/10",
    icon: "border-emerald-300/25 bg-emerald-400/10 text-emerald-100",
    badge: "border-emerald-300/25 bg-emerald-400/10 text-emerald-100",
  },
  amber: {
    card: "border-amber-300/20 bg-gradient-to-br from-amber-500/18 via-[#0f172a] to-[#0b1220] hover:border-amber-300/40 hover:bg-amber-500/10",
    icon: "border-amber-300/25 bg-amber-400/10 text-amber-100",
    badge: "border-amber-300/25 bg-amber-400/10 text-amber-100",
  },
  cyan: {
    card: "border-cyan-300/20 bg-gradient-to-br from-cyan-500/18 via-[#0f172a] to-[#0b1220] hover:border-cyan-300/40 hover:bg-cyan-500/10",
    icon: "border-cyan-300/25 bg-cyan-400/10 text-cyan-100",
    badge: "border-cyan-300/25 bg-cyan-400/10 text-cyan-100",
  },
  violet: {
    card: "border-violet-300/20 bg-gradient-to-br from-violet-500/18 via-[#0f172a] to-[#0b1220] hover:border-violet-300/40 hover:bg-violet-500/10",
    icon: "border-violet-300/25 bg-violet-400/10 text-violet-100",
    badge: "border-violet-300/25 bg-violet-400/10 text-violet-100",
  },
  rose: {
    card: "border-rose-300/20 bg-gradient-to-br from-rose-500/18 via-[#0f172a] to-[#0b1220] hover:border-rose-300/40 hover:bg-rose-500/10",
    icon: "border-rose-300/25 bg-rose-400/10 text-rose-100",
    badge: "border-rose-300/25 bg-rose-400/10 text-rose-100",
  },
};

function SelectionOptionCard({
  tone,
  icon,
  title,
  description,
  badge,
  onClick,
  className = "",
}: {
  tone: SelectionOptionTone;
  icon: ReactNode;
  title: string;
  description: string;
  badge?: string;
  onClick: () => void;
  className?: string;
}) {
  const toneClass = SELECTION_OPTION_TONE_CLASSES[tone];

  return (
    <button
      type="button"
      onClick={onClick}
      className={`group min-h-[150px] rounded-2xl border p-5 text-left transition ${toneClass.card} ${className}`}
    >
      <div className="mb-4 flex items-center justify-between gap-3">
        <div
          className={`master-data-selection-option-icon inline-flex h-12 w-12 shrink-0 items-center justify-center rounded-2xl border text-lg font-bold ${toneClass.icon}`}
        >
          {icon}
        </div>

        {badge && (
          <span
            className={`master-data-selection-option-badge shrink-0 rounded-full border px-3.5 py-1.5 text-xs font-bold tracking-wide ${toneClass.badge}`}
          >
            {badge}
          </span>
        )}
      </div>

      <div className="master-data-selection-option-title break-words text-base font-bold text-slate-100">
        {title}
      </div>
      <div className="master-data-selection-option-description mt-2 break-words text-sm leading-6 text-slate-400">
        {description}
      </div>
    </button>
  );
}

function LoadingSpinner({ className = "h-10 w-10" }: { className?: string }) {
  const spinnerSize = className.includes("h-4")
    ? "1rem"
    : className.includes("h-5")
    ? "1.25rem"
    : className.includes("h-6")
    ? "1.5rem"
    : className.includes("h-8")
    ? "2rem"
    : "2.5rem";

  return (
    <svg
      aria-hidden="true"
      viewBox="0 0 24 24"
      className={`block shrink-0 animate-spin ${className}`}
      style={{
        width: spinnerSize,
        height: spinnerSize,
        minWidth: spinnerSize,
        minHeight: spinnerSize,
        maxWidth: spinnerSize,
        maxHeight: spinnerSize,
      }}
    >
      <circle
        cx="12"
        cy="12"
        r="9"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        className="text-white/15"
      />
      <path
        d="M12 3a9 9 0 0 1 9 9"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        className="text-[#c59b5a]"
      />
    </svg>
  );
}

function LoadingText({ label }: { label: string }) {
  return (
    <span className="inline-flex items-center justify-center gap-2">
      <span>{label}</span>
      <LoadingSpinner className="h-4 w-4" />
    </span>
  );
}

function MasterDataTopLeftNotices({
  items,
}: {
  items: MasterDataNoticeItem[];
}) {
  const [hiddenNoticeKeys, setHiddenNoticeKeys] = useState<
    Record<string, boolean>
  >({});

  const [exitingNoticeKeys, setExitingNoticeKeys] = useState<
    Record<string, boolean>
  >({});

  const hideTimersRef = useRef<Record<string, number>>({});
  const removeTimersRef = useRef<Record<string, number>>({});

  const getNoticeKey = (item: MasterDataNoticeItem) =>
    [
      item.key,
      item.tone,
      item.title,
      item.loading ? "loading" : "static",
      typeof item.message === "string" ? item.message : "",
    ].join("::");

  const noticeSignature = items.map(getNoticeKey).join("||");

  const clearNoticeTimers = (key: string) => {
    const hideTimer = hideTimersRef.current[key];
    const removeTimer = removeTimersRef.current[key];

    if (hideTimer) {
      window.clearTimeout(hideTimer);
    }

    if (removeTimer) {
      window.clearTimeout(removeTimer);
    }

    delete hideTimersRef.current[key];
    delete removeTimersRef.current[key];
  };

  useEffect(() => {
    const currentNoticeKeySet = new Set(items.map(getNoticeKey));

    Object.keys(hideTimersRef.current).forEach((key) => {
      if (!currentNoticeKeySet.has(key)) {
        clearNoticeTimers(key);
      }
    });

    Object.keys(removeTimersRef.current).forEach((key) => {
      if (!currentNoticeKeySet.has(key)) {
        clearNoticeTimers(key);
      }
    });

    setHiddenNoticeKeys((prev) => {
      let changed = false;
      const next = { ...prev };

      Object.keys(next).forEach((key) => {
        if (!currentNoticeKeySet.has(key)) {
          delete next[key];
          changed = true;
        }
      });

      return changed ? next : prev;
    });

    setExitingNoticeKeys((prev) => {
      let changed = false;
      const next = { ...prev };

      Object.keys(next).forEach((key) => {
        if (!currentNoticeKeySet.has(key)) {
          delete next[key];
          changed = true;
        }
      });

      return changed ? next : prev;
    });

    items.forEach((item) => {
      const noticeKey = getNoticeKey(item);

      if (
        hiddenNoticeKeys[noticeKey] ||
        exitingNoticeKeys[noticeKey] ||
        hideTimersRef.current[noticeKey] ||
        removeTimersRef.current[noticeKey]
      ) {
        return;
      }

      if (item.loading) {
        return;
      }

      hideTimersRef.current[noticeKey] = window.setTimeout(() => {
        setExitingNoticeKeys((prev) => ({
          ...prev,
          [noticeKey]: true,
        }));

        removeTimersRef.current[noticeKey] = window.setTimeout(() => {
          setHiddenNoticeKeys((prev) => ({
            ...prev,
            [noticeKey]: true,
          }));

          setExitingNoticeKeys((prev) => {
            const next = { ...prev };
            delete next[noticeKey];
            return next;
          });

          clearNoticeTimers(noticeKey);
        }, 460);
      }, 5000);
    });
  }, [noticeSignature, hiddenNoticeKeys, exitingNoticeKeys]);

  useEffect(() => {
    return () => {
      Object.keys(hideTimersRef.current).forEach(clearNoticeTimers);
      Object.keys(removeTimersRef.current).forEach(clearNoticeTimers);
    };
  }, []);

  const visibleItems = items.filter(
    (item) => !hiddenNoticeKeys[getNoticeKey(item)]
  );

  if (visibleItems.length === 0) {
    return null;
  }

  const getNoticeIcon = (item: MasterDataNoticeItem) => {
    if (item.loading) {
      return <span className="master-data-notice-spinner" aria-hidden="true" />;
    }

    if (item.tone === "success") {
      return "✓";
    }

    if (item.tone === "error") {
      return "!";
    }

    return "●";
  };

  return (
    <div className="master-data-top-left-notices">
      {visibleItems.map((item) => {
        const noticeKey = getNoticeKey(item);

        return (
          <div
            key={noticeKey}
            className={`master-data-notice master-data-notice--${item.tone} ${
              exitingNoticeKeys[noticeKey] ? "master-data-notice--exiting" : ""
            }`}
          >
            <div className="master-data-notice-icon">
              {getNoticeIcon(item)}
            </div>

            <div className="min-w-0 flex-1">
              <div className="master-data-notice-title">
                {item.title}
              </div>

              {item.message && (
                <div className="master-data-notice-message">
                  {item.message}
                </div>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function MasterDataLoadingPanel({
  title = "読み込み中",
  message = "画面を準備しています",
}: {
  title?: string;
  message?: string;
}) {
  return (
    <div className="master-data-loading-panel flex h-full min-h-0 items-center justify-center px-6">
      <div className="master-data-loading-card flex min-h-[310px] w-full max-w-[460px] flex-col items-center justify-center rounded-[28px] border border-white/10 bg-white/[0.04] px-8 py-10 text-center shadow-[0_24px_80px_rgba(0,0,0,0.45)] backdrop-blur-xl">
        <MasterDataBrandLogo className="h-auto w-[220px] shrink-0" />

        <LoadingSpinner />

        <div className="mt-5 text-sm font-semibold tracking-[0.22em] text-[#d9b56f]">
          {title}
        </div>

        <div className="mt-2 text-xs text-slate-400">
          {message}
        </div>
      </div>
    </div>
  );
}

function BlockingLoadingOverlay({
  title,
  message,
}: {
  title: string;
  message: string;
}) {
  if (typeof document === "undefined") return null;

  return createPortal(
    <div className="app-modal-root master-data-loading-overlay fixed inset-0 z-[10050] bg-slate-950/55 backdrop-blur-md">
      <MasterDataLoadingPanel title={title} message={message} />
    </div>,
    document.body
  );
}

function PageSelectDropdown({
  page,
  totalPages,
  disabled,
  open,
  onToggle,
  onClose,
  onSelect,
  className = "h-9 min-w-[100px] text-xs",
}: {
  page: number;
  totalPages: number;
  disabled: boolean;
  open: boolean;
  onToggle: () => void;
  onClose: () => void;
  onSelect: (pageNumber: number) => void;
  className?: string;
}) {
  const pageOptions = useMemo(
    () =>
      Array.from(
        { length: Math.max(totalPages, 1) },
        (_, index) => index + 1
      ),
    [totalPages]
  );

  return (
    <div
      className="relative mt-1"
      onBlur={(e) => {
        if (!e.currentTarget.contains(e.relatedTarget as Node | null)) {
          onClose();
        }
      }}
    >
      <button
        type="button"
        onClick={onToggle}
        disabled={disabled}
        className={`group/page relative flex items-center justify-between gap-2 rounded-xl border border-indigo-300/30 bg-gradient-to-br from-indigo-400/15 via-[#0f172a] to-[#07111f] px-3 font-black text-slate-100 shadow-inner outline-none transition hover:border-indigo-300/50 hover:bg-indigo-500/15 focus:border-indigo-300/60 focus:ring-2 focus:ring-indigo-300/20 disabled:opacity-40 ${className}`}
      >
        <span className="font-black" style={{ fontWeight: 900 }}>
          {page}
        </span>
        <span className="text-[10px] font-black text-indigo-100 transition group-hover/page:translate-y-0.5">
          ▾
        </span>
      </button>

      {open && !disabled && (
        <div className="app-scrollbar absolute left-1/2 top-[calc(100%+8px)] z-[120] max-h-[280px] w-[118px] -translate-x-1/2 overflow-y-auto rounded-2xl border border-indigo-300/25 bg-[#07111f]/98 p-1.5 shadow-[0_18px_44px_rgba(0,0,0,0.45)] backdrop-blur-xl">
          {pageOptions.map((pageNumber) => {
            const active = page === pageNumber;

            return (
              <button
                key={pageNumber}
                type="button"
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => onSelect(pageNumber)}
                className={`flex h-9 w-full items-center justify-center rounded-xl text-sm font-black transition ${
                  active
                    ? "bg-gradient-to-r from-indigo-400 to-cyan-400 text-[#03131f] shadow-[0_0_18px_rgba(45,212,191,0.24)]"
                    : "text-slate-100 hover:bg-indigo-400/12 hover:text-indigo-100"
                }`}
                style={{ fontWeight: 900 }}
              >
                {pageNumber}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

function HeaderCell({
  label,
  filterKey,
  filterState,
  isOpen,
  canShowFilterButton,
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
  canShowFilterButton: boolean;
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
  const active = canShowFilterButton && hasActiveFilter(filterState);

  const visibleValues = useMemo(() => {
    const searchText = filterState.valueSearch.trim().toLowerCase();

    if (!searchText) {
      return filterState.availableValues;
    }

    return filterState.availableValues.filter((value) => {
      const labelValue = value === "" ? "(空白)" : value;
      return labelValue.toLowerCase().includes(searchText);
    });
  }, [filterState.availableValues, filterState.valueSearch]);

const selectedValueSet = useMemo(
  () => new Set(filterState.selectedValues),
  [filterState.selectedValues]
);

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

  const filterPanelBodyRef = useRef<HTMLDivElement | null>(null);
  const filterValueListBoxRef = useRef<HTMLDivElement | null>(null);
  const filterValueScrollTargetRef = useRef<HTMLElement | null>(null);

  const scrollFilterValuesTop = () => {
    filterPanelBodyRef.current?.scrollTo({ top: 0 });

    const listBox = filterValueListBoxRef.current;
    if (!listBox) return;

    listBox.scrollTo({ top: 0 });

    const cachedTarget = filterValueScrollTargetRef.current;

    if (cachedTarget && listBox.contains(cachedTarget)) {
      cachedTarget.scrollTo({ top: 0 });
      return;
    }

    const listScrollTarget = listBox.querySelector<HTMLElement>("div");

    if (listScrollTarget) {
      filterValueScrollTargetRef.current = listScrollTarget;
      listScrollTarget.scrollTo({ top: 0 });
    }
};

  useEffect(() => {
    if (!isOpen) return;

    window.requestAnimationFrame(() => {
      scrollFilterValuesTop();
    });
  }, [isOpen, filterState.valueOffset, filterState.valueSearch]);

  return (
    <div className="relative border-r border-white/5 last:border-r-0">
      <div
        className={`grid items-center gap-2 px-4 py-3 text-sm font-semibold ${
          active ? "bg-sky-500/10 text-sky-100" : "text-slate-100"
        }`}
        style={{
          gridTemplateColumns: canShowFilterButton
            ? "minmax(0, 1fr) 28px"
            : "minmax(0, 1fr)",
        }}
      >
        <span className="block w-full whitespace-nowrap text-center">
          {label}
        </span>

        {canShowFilterButton && (
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
        )}
      </div>

      {canShowFilterButton &&
        isOpen &&
        typeof document !== "undefined" &&
        createPortal(
          <div
            className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-2"
            onClick={() => onToggleOpen(filterKey)}
          >
            {filterState.valueLoading && (
              <BlockingLoadingOverlay
                title="フィルタ候補読み込み中"
                message="フィルタ候補を読み込んでいます"
              />
            )}
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

                <div ref={filterPanelBodyRef} className="flex-1 overflow-y-auto px-4 py-4">
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
                              onClick={() => {
                                scrollFilterValuesTop();
                                onLoadPreviousValues(filterKey);
                              }}
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
                              onClick={() => {
                                scrollFilterValuesTop();
                                onLoadMoreValues(filterKey);
                              }}
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

                    <div ref={filterValueListBoxRef} className="rounded-xl border border-white/10 bg-[#0f172a] p-2">
                      {visibleValues.length === 0 ? (
                        <div className="px-2 py-6 text-center text-xs text-slate-500">
                          {filterState.valueLoading ? null : "候補がありません"}
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
                            selectedValueSet,
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
  selectedValueSet,
  onToggleValue,
}: RowComponentProps<{
  visibleValues: string[];
  filterKey: FilterKey;
  filterState: ColumnFilterState;
  selectedValueSet: Set<string>;
  onToggleValue: (key: FilterKey, value: string) => void;
}>) {
  const value = visibleValues[index];
  const checked =
    !filterState.valueFilterEnabled || selectedValueSet.has(value);

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
        <Cell title={row.permit_number || ""}><EmptyValue value={row.permit_number} /></Cell>
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

  const [openPageSizeDropdown, setOpenPageSizeDropdown] = useState<
    "main" | "permission" | null
  >(null);

  const [openPageDropdown, setOpenPageDropdown] = useState<
    "mainHeader" | "permissionHeader" | null
  >(null);

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

  const [advancedFilterReturnTarget, setAdvancedFilterReturnTarget] = useState<
    "sidebarSearch" | "permissionSearch" | null
  >(null);

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

  const [localCrawlWorker, setLocalCrawlWorker] =
    useState<LocalCrawlWorkerStatus | null>(null);
  const [localCrawlWorkerError, setLocalCrawlWorkerError] = useState("");

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
  const crawlStatusRequestIdRef = useRef(0);

  const crawlRecoveringRef = useRef(false);
  const crawlStatusErrorCountRef = useRef(0);
  const crawlRestoreTimerRef = useRef<number | null>(null);

  const clearCrawlRestoreTimer = () => {
    if (crawlRestoreTimerRef.current !== null) {
      window.clearTimeout(crawlRestoreTimerRef.current);
      crawlRestoreTimerRef.current = null;
    }
  };

  const [crawlElapsedMs, setCrawlElapsedMs] = useState(0);
  const crawlStartedAtRef = useRef<number | null>(null);
  const crawlElapsedBaseMsRef = useRef(0);
  const crawlElapsedTimerRef = useRef<number | null>(null);

  const [crawlPreviewOpen, setCrawlPreviewOpen] = useState(false);
  const [crawlPreviewRows, setCrawlPreviewRows] = useState<CrawlPreviewRow[]>([]);

  const [crawlPreviewTab, setCrawlPreviewTab] = useState<
    "candidate" | "multiple" | "excluded"
  >("candidate");

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

  const [listAddSourceOpen, setListAddSourceOpen] = useState(false);
  const [mynaviYearOpen, setMynaviYearOpen] = useState(false);
  const [mynaviPageCountOpen, setMynaviPageCountOpen] = useState(false);
  const [mynaviResultOpen, setMynaviResultOpen] = useState(false);
  const [mynaviProgressOpen, setMynaviProgressOpen] = useState(false);

  const [mynaviLoading, setMynaviLoading] = useState(false);
  const [mynaviMessage, setMynaviMessage] = useState("");
  const [mynaviError, setMynaviError] = useState("");

  const [selectedMynaviGradYear, setSelectedMynaviGradYear] = useState("27");
  const [mynaviTotalPages, setMynaviTotalPages] = useState(0);
  const [selectedMynaviPageCount, setSelectedMynaviPageCount] = useState("all");

  const [mynaviJobId, setMynaviJobId] = useState<string | null>(null);
  const [mynaviJobStatus, setMynaviJobStatus] =
    useState<MynaviJobStatus>("idle");

  const [mynaviPhase, setMynaviPhase] = useState<
    "idle" | "count_pages" | "collect_urls" | "scrape_details" | "completed" | "error"
  >("idle");

  const [mynaviTotalUrls, setMynaviTotalUrls] = useState(0);
  const [mynaviProcessedCount, setMynaviProcessedCount] = useState(0);
  const [mynaviSuccessCount, setMynaviSuccessCount] = useState(0);
  const [mynaviFailedCount, setMynaviFailedCount] = useState(0);
  const [mynaviCurrentPageNumber, setMynaviCurrentPageNumber] = useState(0);
  const [mynaviCsvFileName, setMynaviCsvFileName] = useState("");

  const [mynaviJobMode, setMynaviJobMode] =
    useState<MynaviJobMode>("count_pages");

  const [mynaviCurrentField, setMynaviCurrentField] = useState<string | null>(null);
  const [mynaviCurrentCompany, setMynaviCurrentCompany] = useState<string | null>(null);
  const [mynaviCurrentCompanyIndex, setMynaviCurrentCompanyIndex] = useState(0);
  const [mynaviDetectedTotalPages, setMynaviDetectedTotalPages] = useState(0);

  const mynaviPageCountOptions = useMemo(() => {
    const max = Math.min(mynaviTotalPages, 999);

    if (max <= 0) {
      return [];
    }

    return Array.from({ length: max }, (_, index) => String(index + 1));
  }, [mynaviTotalPages]);

  const mynaviStatusTimerRef = useRef<number | null>(null);
  const mynaviUserCanceledRef = useRef(false);

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

  const [itemInspectionExcludedPreviewRows, setItemInspectionExcludedPreviewRows] =
    useState<ItemInspectionPreviewChange[]>([]);

  const [itemInspectionExcludedTotalCount, setItemInspectionExcludedTotalCount] =
    useState(0);

  const [itemInspectionCheckedPreviewRowIds, setItemInspectionCheckedPreviewRowIds] =
    useState<Record<string, boolean>>({});

  const [itemInspectionPreviewPage, setItemInspectionPreviewPage] = useState(1);

  const [itemInspectionPreviewTab, setItemInspectionPreviewTab] =
    useState<"candidate" | "excluded">("candidate");

  const pagedCandidateItemInspectionPreviewChanges = useMemo(() => {
    const start =
      (itemInspectionPreviewPage - 1) * ITEM_INSPECTION_PREVIEW_PAGE_SIZE;

    return itemInspectionPreviewChanges.slice(
      start,
      start + ITEM_INSPECTION_PREVIEW_PAGE_SIZE
    );
  }, [itemInspectionPreviewChanges, itemInspectionPreviewPage]);

  const visibleItemInspectionPreviewChanges =
    itemInspectionPreviewTab === "candidate"
      ? pagedCandidateItemInspectionPreviewChanges
      : itemInspectionExcludedPreviewRows;

  const visibleItemInspectionPreviewTotalCount =
    itemInspectionPreviewTab === "candidate"
      ? itemInspectionPreviewChanges.length
      : itemInspectionExcludedTotalCount;

  const itemInspectionPreviewTotalPages = useMemo(
    () =>
      Math.max(
        1,
        Math.ceil(
          visibleItemInspectionPreviewTotalCount /
            ITEM_INSPECTION_PREVIEW_PAGE_SIZE
        )
      ),
    [visibleItemInspectionPreviewTotalCount]
  );

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
  const [dedupeFieldOpen, setDedupeFieldOpen] = useState(false);
  const [dedupeMethodOpen, setDedupeMethodOpen] = useState(false);

  const [dedupeScopeOpen, setDedupeScopeOpen] = useState(false);

  const [dedupeTargetScope, setDedupeTargetScope] = useState<
    "filtered" | "all" | null
  >(null);

  const [dedupeSelectedField, setDedupeSelectedField] =
    useState<FilterKey | null>(null);

  const [dedupeMatchMethod, setDedupeMatchMethod] =
    useState<DedupeMatchMethod | null>(null);

  const [importConfirmOpen, setImportConfirmOpen] = useState(false);
  const [importDuplicateConfirmOpen, setImportDuplicateConfirmOpen] = useState(false);
  const [exportScopeOpen, setExportScopeOpen] = useState(false);
  const [exportConfirmOpen, setExportConfirmOpen] = useState(false);
  const [exportDuplicateConfirmOpen, setExportDuplicateConfirmOpen] = useState(false);
  const [exportMode, setExportMode] = useState<"all" | "filtered">("filtered");
  const [exporting, setExporting] = useState(false);
  const [csvTemplateSaving, setCsvTemplateSaving] = useState(false);
  const [previewCsvExporting, setPreviewCsvExporting] = useState(false);

  const [previewCsvScopeOpen, setPreviewCsvScopeOpen] = useState(false);
  const [previewCsvConfirmOpen, setPreviewCsvConfirmOpen] = useState(false);
  const [previewCsvSource, setPreviewCsvSource] = useState<
    "crawl" | "item_inspection" | null
  >(null);
  const [previewCsvMode, setPreviewCsvMode] = useState<"all" | "candidate">(
    "all"
  );

  const [singleFilterClearConfirm, setSingleFilterClearConfirm] =
    useState<FilterClearConfirmTarget | null>(null);

  const [allFiltersClearConfirmOpen, setAllFiltersClearConfirmOpen] = useState(false);

  const [allFiltersClearConfirmTarget, setAllFiltersClearConfirmTarget] =
    useState<"main" | "permission" | null>(null);

  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [openSidebarPanel, setOpenSidebarPanel] =
    useState<SidebarPanelKey | null>(null);

  const [themeMode, setThemeMode] = useState<"system" | "dark" | "light">("system");

  const [systemThemeMode, setSystemThemeMode] = useState<"dark" | "light">(() => {
    if (typeof window === "undefined") {
      return "dark";
    }

    return window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  });

  const effectiveThemeMode = themeMode === "system" ? systemThemeMode : themeMode;

  const [settingsTab, setSettingsTab] = useState<
    "profile" | "loginHistory" | "permissionManagement"
  >("profile");
  const [loginHistory, setLoginHistory] = useState<
    MasterDataLoginHistoryItem[]
  >([]);

  const [permissionEmployees, setPermissionEmployees] = useState<
    MasterDataPermissionEmployee[]
  >([]);
  const [selectedPermissionEmployeeId, setSelectedPermissionEmployeeId] =
    useState("");
  const [permissionLoading, setPermissionLoading] = useState(false);
  const [permissionSaving, setPermissionSaving] = useState(false);
  const [permissionError, setPermissionError] = useState("");
  const [permissionMessage, setPermissionMessage] = useState("");
  const [permissionListScopeOpen, setPermissionListScopeOpen] = useState(false);

  const [permissionListScopeSearchOpen, setPermissionListScopeSearchOpen] =
    useState(false);

  const permissionListScopeBaseColumnStatesRef =
    useRef<Record<FilterKey, ColumnFilterState> | null>(null);

  const permissionListScopeBaseAdvancedFiltersRef =
    useRef<AdvancedFiltersState | null>(null);

  const permissionListScopeBasePageRef = useRef(1);
  const permissionListScopeBaseLimitRef = useRef("200");

  const [currentUserPermissions, setCurrentUserPermissions] =
    useState<MasterDataPermissions>({});

  const [currentUserPermissionLoaded, setCurrentUserPermissionLoaded] =
    useState(false);

  const selectedPermissionEmployee = useMemo(
    () =>
      permissionEmployees.find(
        (employee) => employee.id === selectedPermissionEmployeeId
      ) ?? null,
    [permissionEmployees, selectedPermissionEmployeeId]
  );

  const canUsePermission = (permissionKey: MasterDataPermissionKey) => {
    if (!loginUser) {
      return true;
    }

    if (loginUser.role === "スーパー管理者") {
      return true;
    }

    if (!currentUserPermissionLoaded) {
      return true;
    }

    return currentUserPermissions[permissionKey] !== false;
  };

  const canUseAnyPermission = (permissionKeys: MasterDataPermissionKey[]) => {
    return permissionKeys.some((permissionKey) =>
      canUsePermission(permissionKey)
    );
  };

  const getAdvancedFilterPermissionKey = (
    key: AdvancedFilterModalKey
  ): MasterDataPermissionKey => {
    return `search.${key}` as MasterDataPermissionKey;
  };

  const [settingsPasswordVisible, setSettingsPasswordVisible] = useState(false);

  const [loginStatus, setLoginStatus] =
    useState<MasterDataLoginStatus>("checking");

  const [loginId, setLoginId] = useState("");
  const [loginPassword, setLoginPassword] = useState("");
  const [loginPasswordVisible, setLoginPasswordVisible] = useState(false);
  const [loginLoading, setLoginLoading] = useState(false);
  const [loginError, setLoginError] = useState("");
  const [loginUser, setLoginUser] = useState<MasterDataLoginUser | null>(null);
  const [screenReady, setScreenReady] = useState(false);

  const [inspectionCancelConfirmTarget, setInspectionCancelConfirmTarget] =
    useState<InspectionCancelConfirmTarget | null>(null);

  const isMasterDataLoginKeepAliveModalOpen =
    crawlProgressOpen ||
    itemInspectionProgressOpen ||
    itemInspectionPreviewConfirmOpen ||
    crawlPreviewOpen;

  const visibleCrawlConfirmFieldOptions = CRAWL_CONFIRM_FIELD_OPTIONS.filter(
    (field) =>
      canUsePermission(
        `inspection.crawlField.${field.key}` as MasterDataPermissionKey
      )
  );

  const visibleItemInspectionColumnDefs = COLUMN_DEFS.filter((column) =>
    canUsePermission(
      `inspection.itemInspectionField.${column.key}` as MasterDataPermissionKey
    )
  );

  const canUseCrawlPanel =
    canUsePermission("inspection.crawl") &&
    visibleCrawlConfirmFieldOptions.length > 0;

  const canUseItemInspectionPanel =
    canUsePermission("inspection.itemInspection") &&
    visibleItemInspectionColumnDefs.length > 0;

  const canShowListColumnFilters = canUsePermission("search.columnFilters");

  const createVisibleCrawlFieldSelections = (
    checked: boolean
  ): Record<CrawlFieldKey, boolean> => {
    const visibleKeySet = new Set(
      visibleCrawlConfirmFieldOptions.map((field) => field.key)
    );

    return CRAWL_CONFIRM_FIELD_OPTIONS.reduce((acc, field) => {
      acc[field.key] = checked && visibleKeySet.has(field.key);
      return acc;
    }, {} as Record<CrawlFieldKey, boolean>);
  };

  const createVisibleItemInspectionSelections = (
    checked: boolean
  ): Record<FilterKey, boolean> => {
    const visibleKeySet = new Set(
      visibleItemInspectionColumnDefs.map((column) => column.key)
    );

    return COLUMN_DEFS.reduce((acc, column) => {
      acc[column.key] = checked && visibleKeySet.has(column.key);
      return acc;
    }, {} as Record<FilterKey, boolean>);
  };

  const fetchDataRequestIdRef = useRef(0);
  const filterValueRequestIdRef = useRef<Partial<Record<FilterKey, number>>>({});
  const crawlPreviewRequestIdRef = useRef(0);
  const masterListScrollRef = useRef<HTMLDivElement | null>(null);
  const permissionListScopeScrollRef = useRef<HTMLDivElement | null>(null);
  const crawlPreviewScrollRef = useRef<HTMLDivElement | null>(null);
  const itemInspectionPreviewScrollRef = useRef<HTMLDivElement | null>(null);

  const fetchCurrentUserPermissions = async (targetUser = loginUser) => {
    setCurrentUserPermissionLoaded(false);

    if (!targetUser) {
      setCurrentUserPermissions({});
      setCurrentUserPermissionLoaded(true);
      return;
    }

    if (targetUser.role === "スーパー管理者") {
      setCurrentUserPermissions({});
      setCurrentUserPermissionLoaded(true);
      return;
    }

    try {
      const res = await fetch("/api/master_data/permissions/me", {
        method: "GET",
        cache: "no-store",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "ログイン中ユーザーの権限取得に失敗しました");
      }

      setCurrentUserPermissions(data.permissions || {});
      setCurrentUserPermissionLoaded(true);
    } catch {
      setCurrentUserPermissions({});
      setCurrentUserPermissionLoaded(true);
    }
  };

  const fetchPermissionEmployees = async () => {
    if (!isMasterDataManagerRole(loginUser?.role)) {
      setPermissionEmployees([]);
      setSelectedPermissionEmployeeId("");
      return;
    }

    setPermissionLoading(true);
    setPermissionError("");
    setPermissionMessage("");

    try {
      const res = await fetch("/api/master_data/permissions", {
        method: "GET",
        cache: "no-store",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "権限情報の取得に失敗しました");
      }

      const employees = data.employees || [];

      setPermissionEmployees(employees);
      setSelectedPermissionEmployeeId((currentId) => {
        if (currentId && employees.some((employee) => employee.id === currentId)) {
          return currentId;
        }

        return employees[0]?.id ?? "";
      });
    } catch (e) {
      setPermissionEmployees([]);
      setSelectedPermissionEmployeeId("");
      setPermissionError(
        e instanceof Error ? e.message : "権限情報の取得に失敗しました"
      );
    } finally {
      setPermissionLoading(false);
    }
  };

  const handleToggleEmployeePermission = (
    employeeId: string,
    permissionKey: MasterDataPermissionKey
  ) => {
    setPermissionEmployees((prev) =>
      prev.map((employee) => {
        if (employee.id !== employeeId) {
          return employee;
        }

        const currentChecked = employee.permissions[permissionKey] !== false;
        const nextChecked = !currentChecked;
        const nextPermissions = { ...employee.permissions };

        nextPermissions[permissionKey] = nextChecked;

        if (permissionKey === "inspection.crawl") {
          CRAWL_FIELD_PERMISSION_KEYS.forEach((fieldPermissionKey) => {
            nextPermissions[fieldPermissionKey] = nextChecked;
          });
        }

        if (permissionKey === "inspection.itemInspection") {
          ITEM_INSPECTION_FIELD_PERMISSION_KEYS.forEach((fieldPermissionKey) => {
            nextPermissions[fieldPermissionKey] = nextChecked;
          });
        }

        return {
          ...employee,
          permissions: syncInspectionParentPermissions(nextPermissions),
        };
      })
    );
  };

  const handleSetEmployeePermissionGroup = (
    employeeId: string,
    permissionKeys: MasterDataPermissionKey[],
    checked: boolean
  ) => {
    setPermissionEmployees((prev) =>
      prev.map((employee) => {
        if (employee.id !== employeeId) {
          return employee;
        }

        const nextPermissions = { ...employee.permissions };

        permissionKeys.forEach((permissionKey) => {
          nextPermissions[permissionKey] = checked;
        });

        if (permissionKeys.includes("inspection.crawl")) {
          CRAWL_FIELD_PERMISSION_KEYS.forEach((fieldPermissionKey) => {
            nextPermissions[fieldPermissionKey] = checked;
          });
        }

        if (permissionKeys.includes("inspection.itemInspection")) {
          ITEM_INSPECTION_FIELD_PERMISSION_KEYS.forEach((fieldPermissionKey) => {
            nextPermissions[fieldPermissionKey] = checked;
          });
        }

        return {
          ...employee,
          permissions: syncInspectionParentPermissions(nextPermissions),
        };
      })
    );
  };

    const buildCurrentPermissionListScope = (): PermissionListScopeFilters => {
      const filterModels = buildRequestFilterModels(appliedColumnStates);
      const advancedFilters = buildRequestAdvancedFilters(
        appliedAdvancedFilters,
        advancedValueOptions
      );

      const sortColumn = COLUMN_DEFS.find(
        (column) => appliedColumnStates[column.key].sortDirection !== ""
      );

      const scope: PermissionListScopeFilters = {
        filterModels,
        advancedFilters,
      };

      if (sortColumn) {
        scope.sortKey = sortColumn.key;
        scope.sortDirection = appliedColumnStates[sortColumn.key].sortDirection;
      }

      return scope;
    };

    const handleApplyPermissionListScope = () => {
      if (!selectedPermissionEmployee) {
        return;
      }

      const listScopeFilters = buildCurrentPermissionListScope();

      const nextAllowedFilters = {
        ...(selectedPermissionEmployee.allowedFilters || {}),
        listScopeFilters,
      };

      setPermissionEmployees((prev) =>
        prev.map((employee) => {
          if (employee.id !== selectedPermissionEmployee.id) {
            return employee;
          }

          return {
            ...employee,
            allowedFilters: nextAllowedFilters,
          };
        })
      );

      closePermissionListScopeModal();
      setPermissionMessage(
        "リストの絞り込み範囲を反映しました。保存ボタンを押すと確定します"
      );
    };

  const handleClearPermissionListScope = () => {
    if (!selectedPermissionEmployee) {
      return;
    }

    setPermissionEmployees((prev) =>
      prev.map((employee) => {
        if (employee.id !== selectedPermissionEmployee.id) {
          return employee;
        }

        const nextAllowedFilters = { ...(employee.allowedFilters || {}) };
        delete nextAllowedFilters.listScopeFilters;

        return {
          ...employee,
          allowedFilters: nextAllowedFilters,
        };
      })
    );

    setPermissionMessage(
      "リストの絞り込み範囲を解除しました。保存ボタンを押すと確定します"
    );
  };

  const isPermissionScopeRecord = (
    value: unknown
  ): value is Record<string, unknown> => {
    return value !== null && typeof value === "object" && !Array.isArray(value);
  };

  const toPermissionScopeStringArray = (value: unknown) => {
    if (!Array.isArray(value)) {
      return [];
    }

    return value
      .map((item) => String(item ?? "").trim())
      .filter((item) => item !== "");
  };

  const toPermissionScopeString = (value: unknown) => {
    return String(value ?? "").trim();
  };

  const createColumnStatesFromPermissionListScope = (
    listScopeFilters: unknown
  ): Record<FilterKey, ColumnFilterState> => {
    const next = createInitialColumnStates();

    if (!isPermissionScopeRecord(listScopeFilters)) {
      return next;
    }

    const filterModels = listScopeFilters.filterModels;

    if (!isPermissionScopeRecord(filterModels)) {
      return next;
    }

    COLUMN_DEFS.forEach((column) => {
      const model = filterModels[column.key];

      if (!isPermissionScopeRecord(model)) {
        return;
      }

      const conditionType = toPermissionScopeString(model.conditionType);
      const sortDirection = toPermissionScopeString(model.sortDirection);

      if (
        sortDirection === "" ||
        sortDirection === "asc" ||
        sortDirection === "desc"
      ) {
        next[column.key].sortDirection = sortDirection;
      }

      if (
        conditionType === "" ||
        conditionType === "contains" ||
        conditionType === "not_contains" ||
        conditionType === "equals" ||
        conditionType === "not_equals" ||
        conditionType === "starts_with" ||
        conditionType === "ends_with" ||
        conditionType === "is_empty" ||
        conditionType === "is_not_empty"
      ) {
        next[column.key].conditionType = conditionType;
      }

      next[column.key].conditionValue = toPermissionScopeString(
        model.conditionValue
      );

      next[column.key].valueFilterEnabled =
        model.valueFilterEnabled === true;

      next[column.key].selectedValues = toPermissionScopeStringArray(
        model.selectedValues
      );
    });

    const sortKey = toPermissionScopeString(listScopeFilters.sortKey);
    const sortDirection = toPermissionScopeString(
      listScopeFilters.sortDirection
    );

    if (
      COLUMN_DEFS.some((column) => column.key === sortKey) &&
      (sortDirection === "asc" || sortDirection === "desc")
    ) {
      COLUMN_DEFS.forEach((column) => {
        next[column.key].sortDirection =
          column.key === sortKey ? sortDirection : "";
      });
    }

    return next;
  };

  const createAdvancedFiltersFromPermissionListScope = (
    listScopeFilters: unknown
  ): AdvancedFiltersState => {
    const next = createInitialAdvancedFiltersState();

    if (!isPermissionScopeRecord(listScopeFilters)) {
      return next;
    }

    const advancedFilters = listScopeFilters.advancedFilters;

    if (!isPermissionScopeRecord(advancedFilters)) {
      return next;
    }

    if (isPermissionScopeRecord(advancedFilters.companyName)) {
      next.companyName.keyword = toPermissionScopeString(
        advancedFilters.companyName.keyword
      );
    }

    if (isPermissionScopeRecord(advancedFilters.prefectures)) {
      next.prefectures.regions = toPermissionScopeStringArray(
        advancedFilters.prefectures.regions
      );
      next.prefectures.prefectures = toPermissionScopeStringArray(
        advancedFilters.prefectures.prefectures
      );
      next.prefectures.cities = toPermissionScopeStringArray(
        advancedFilters.prefectures.cities
      );
    }

    if (isPermissionScopeRecord(advancedFilters.industries)) {
      next.industries.bigIndustries = toPermissionScopeStringArray(
        advancedFilters.industries.bigIndustries
      );
      next.industries.smallIndustries = toPermissionScopeStringArray(
        advancedFilters.industries.smallIndustries
      );
    }

    if (isPermissionScopeRecord(advancedFilters.established)) {
      next.established.years = toPermissionScopeStringArray(
        advancedFilters.established.years
      );
      next.established.yearMonths = toPermissionScopeStringArray(
        advancedFilters.established.yearMonths
      );
      next.established.from = toPermissionScopeString(
        advancedFilters.established.from
      );
      next.established.to = toPermissionScopeString(
        advancedFilters.established.to
      );
    }

    if (isPermissionScopeRecord(advancedFilters.capital)) {
      next.capital.min = toPermissionScopeString(advancedFilters.capital.min);
      next.capital.max = toPermissionScopeString(advancedFilters.capital.max);
    }

    if (isPermissionScopeRecord(advancedFilters.employeeCount)) {
      next.employeeCount.min = toPermissionScopeString(
        advancedFilters.employeeCount.min
      );
      next.employeeCount.max = toPermissionScopeString(
        advancedFilters.employeeCount.max
      );
    }

    if (isPermissionScopeRecord(advancedFilters.tags)) {
      next.tags.parents = toPermissionScopeStringArray(
        advancedFilters.tags.parents
      );
      next.tags.tags = toPermissionScopeStringArray(
        advancedFilters.tags.tags
      );
    }

    return next;
  };

  const openPermissionListScopeModal = () => {
    permissionListScopeBaseColumnStatesRef.current =
      cloneColumnStates(appliedColumnStates);

    permissionListScopeBaseAdvancedFiltersRef.current =
      cloneAdvancedFiltersState(appliedAdvancedFilters);

    permissionListScopeBasePageRef.current = page;
    permissionListScopeBaseLimitRef.current = limit;

    const savedListScopeFilters =
      selectedPermissionEmployee?.allowedFilters?.listScopeFilters;

    const scopeColumnStates =
      createColumnStatesFromPermissionListScope(savedListScopeFilters);

    const scopeAdvancedFilters =
      createAdvancedFiltersFromPermissionListScope(savedListScopeFilters);

    setDraftColumnStates(scopeColumnStates);
    setAppliedColumnStates(scopeColumnStates);
    setDraftAdvancedFilters(scopeAdvancedFilters);
    setAppliedAdvancedFilters(scopeAdvancedFilters);
    setPage(1);

    setOpenAdvancedFilterKey(null);
    setOpenFilterKey(null);
    setPermissionListScopeSearchOpen(false);
    setPermissionListScopeOpen(true);
  };

  const closePermissionListScopeModal = () => {
    setPermissionListScopeOpen(false);
    setPermissionListScopeSearchOpen(false);
    setOpenFilterKey(null);
    setOpenAdvancedFilterKey(null);

    if (permissionListScopeBaseColumnStatesRef.current) {
      const baseColumnStates = cloneColumnStates(
        permissionListScopeBaseColumnStatesRef.current
      );

      setDraftColumnStates(baseColumnStates);
      setAppliedColumnStates(baseColumnStates);
    }

    if (permissionListScopeBaseAdvancedFiltersRef.current) {
      const baseAdvancedFilters = cloneAdvancedFiltersState(
        permissionListScopeBaseAdvancedFiltersRef.current
      );

      setDraftAdvancedFilters(baseAdvancedFilters);
      setAppliedAdvancedFilters(baseAdvancedFilters);
    }

    setPage(permissionListScopeBasePageRef.current);
    setLimit(permissionListScopeBaseLimitRef.current);

    permissionListScopeBaseColumnStatesRef.current = null;
    permissionListScopeBaseAdvancedFiltersRef.current = null;
  };

  const handleSetAllEmployeePermissions = (
    employeeId: string,
    checked: boolean
  ) => {
    const allPermissionKeys = [
      ...LIST_DATA_PERMISSION_ITEMS.map((item) => item.key),
      ...MASTER_DATA_PERMISSION_GROUPS.flatMap((group) =>
        group.items.map((item) => item.key)
      ),
    ];

    handleSetEmployeePermissionGroup(employeeId, allPermissionKeys, checked);
  };

  const handleSaveEmployeePermissions = async () => {
    if (!selectedPermissionEmployee) {
      return;
    }

    setPermissionSaving(true);
    setPermissionError("");
    setPermissionMessage("");

    try {
      const res = await fetch("/api/master_data/permissions", {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          userId: selectedPermissionEmployee.id,
          permissions: selectedPermissionEmployee.permissions,
          allowedFilters: selectedPermissionEmployee.allowedFilters || {},
        }),
        cache: "no-store",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok || !data.employee) {
        throw new Error(data.error || "権限情報の保存に失敗しました");
      }

      setPermissionEmployees((prev) =>
        prev.map((employee) =>
          employee.id === data.employee?.id ? data.employee : employee
        )
      );
      
      await fetchPermissionEmployees();

      setPermissionMessage("権限を保存しました");
    } catch (e) {
      setPermissionError(
        e instanceof Error ? e.message : "権限情報の保存に失敗しました"
      );
    } finally {
      setPermissionSaving(false);
    }
  };

  const handleLoginSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    const normalizedLoginId = loginId.trim();

    if (normalizedLoginId === "" || loginPassword.trim() === "") {
      setLoginError("IDとパスワードを入力してください");
      return;
    }

    setLoginLoading(true);
    setLoginError("");

    try {
      const res = await fetch("/api/master_data/login", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          id: normalizedLoginId,
          password: loginPassword,
        }),
        cache: "no-store",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "IDまたはパスワードが違います");
      }

      const nextLoginUser = data.loginUser ?? null;

      const nextLoginHistory =
        data.loginHistory ||
        (data.loginHistoryEvent ? [data.loginHistoryEvent] : []);

            if (typeof window !== "undefined") {
              window.sessionStorage.setItem(MASTER_DATA_LOGIN_SESSION_KEY, "1");
              refreshMasterDataLoginExpiresAt();

              if (nextLoginUser) {
                window.sessionStorage.setItem(
                  MASTER_DATA_LOGIN_USER_KEY,
                  JSON.stringify(nextLoginUser)
                );
              } else {
                window.sessionStorage.removeItem(MASTER_DATA_LOGIN_USER_KEY);
              }
            }

            setLoginUser(nextLoginUser);
            setLoginHistory(nextLoginHistory);
            setLoginStatus("logged_in");
            setLoginPassword("");
            setLoginError("");
    } catch (e) {
      setLoginStatus("logged_out");
      setLoginUser(null);
      setLoginError(
        e instanceof Error ? e.message : "ログインに失敗しました"
      );
    } finally {
      setLoginLoading(false);
    }
  };

  const handleLogout = async () => {
    try {
      await fetch("/api/master_data/login", {
        method: "DELETE",
        cache: "no-store",
      });
    } catch {
      // ログアウト時は画面側の状態を戻すことを優先します
    }

    clearMasterDataLoginSessionStorage();

    setLoginUser(null);
    setLoginHistory([]);
    setLoginStatus("logged_out");
    setLoginId("");
    setLoginPassword("");
    setSettingsPasswordVisible(false);
    setLoginPasswordVisible(false);
    setLoginError("");
    setPermissionEmployees([]);
    setSelectedPermissionEmployeeId("");
    setPermissionError("");
    setPermissionMessage("");
    setCurrentUserPermissions({});
    setCurrentUserPermissionLoaded(false);
  };

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

      const requestId = (filterValueRequestIdRef.current[key] ?? 0) + 1;
      filterValueRequestIdRef.current[key] = requestId;

      setDraftColumnStates((prev) => {
        const current = prev[key];

        return {
          ...prev,
          [key]: {
            ...current,
            valueLoading: true,
            ...(reset
              ? {
                  availableValues: [],
                  valueCounts: {},
                  availableValueTotal: 0,
                  availableValueMatchedCount: 0,
                  hasMoreValues: false,
                  valueOffset: 0,
                }
              : {}),
          },
        };
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
          if (filterValueRequestIdRef.current[key] !== requestId) {
            return prev;
          }

          const current = prev[key];
          const incomingValues = data.values || [];
          const incomingAllValues =
            reset && Array.isArray(data.allValues)
              ? data.allValues
              : current.allValues;

          return {
            ...prev,
            [key]: {
              ...current,
              availableValues: incomingValues,
              allValues: incomingAllValues,
              valueCounts: data.valueCounts || {},
              availableValueTotal: data.valueTotal || 0,
              availableValueMatchedCount: data.valueMatchedCount || 0,
              totalItemCount: data.totalItemCount || 0,
              checkedItemCount: data.checkedItemCount || 0,
              valueOffset: data.valueOffset ?? currentOffset,
              valueLimit: data.valueLimit || currentLimit,
              hasMoreValues: data.hasMoreValues ?? false,
              valueLoading: false,
              selectedValues: currentState.valueFilterEnabled
                ? current.selectedValues
                : [],
            },
          };
        });
      } catch (e) {
        if (filterValueRequestIdRef.current[key] !== requestId) {
          return;
        }

        setDraftColumnStates((prev) => ({
          ...prev,
          [key]: {
            ...prev[key],
            valueLoading: false,
          },
        }));
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
    if (typeof window === "undefined") return;

    let canceled = false;

    const prepareInitialScreen = async () => {
      const savedLoginSession = window.sessionStorage.getItem(
        MASTER_DATA_LOGIN_SESSION_KEY
      );

      const savedLoginExpiresAt = Number(
        window.sessionStorage.getItem(MASTER_DATA_LOGIN_EXPIRES_AT_KEY)
      );

      const isValidLoginSession =
        savedLoginSession === "1" &&
        Number.isFinite(savedLoginExpiresAt) &&
        savedLoginExpiresAt > Date.now();

      let savedLoginUser: MasterDataLoginUser | null = null;

      if (!isValidLoginSession) {
        window.sessionStorage.removeItem(MASTER_DATA_LOGIN_SESSION_KEY);
        window.sessionStorage.removeItem(MASTER_DATA_LOGIN_EXPIRES_AT_KEY);
        window.sessionStorage.removeItem(MASTER_DATA_LOGIN_USER_KEY);
      } else {
        refreshMasterDataLoginExpiresAt();

        try {
          const savedLoginUserText = window.sessionStorage.getItem(
            MASTER_DATA_LOGIN_USER_KEY
          );

          savedLoginUser = savedLoginUserText
            ? (JSON.parse(savedLoginUserText) as MasterDataLoginUser)
            : null;
        } catch {
          window.sessionStorage.removeItem(MASTER_DATA_LOGIN_USER_KEY);
          savedLoginUser = null;
        }
      }

      try {
        await document.fonts.ready;
      } catch {
        // フォント読み込みに失敗しても画面は表示します
      }

      if (canceled) return;

      const savedLoginHistory =
        isValidLoginSession && savedLoginUser
          ? await fetchMasterDataLoginHistory(savedLoginUser.id)
          : [];

      setLoginUser(isValidLoginSession ? savedLoginUser : null);
      setLoginHistory(savedLoginHistory);
      setLoginStatus(isValidLoginSession ? "logged_in" : "logged_out");
      setScreenReady(true);
    };

    void prepareInitialScreen();

    return () => {
      canceled = true;
    };
  }, []);

  useEffect(() => {
    if (loginStatus !== "logged_in") return;

    fetchData();
  }, [page, limit, appliedColumnStates, appliedAdvancedFilters, loginStatus]);

  useEffect(() => {
    masterListScrollRef.current?.scrollTo({ top: 0 });
    permissionListScopeScrollRef.current?.scrollTo({ top: 0 });
  }, [page, limit]);

  useEffect(() => {
    if (!canShowListColumnFilters) {
      setOpenFilterKey(null);
    }
  }, [canShowListColumnFilters]);

  useEffect(() => {
    if (loginStatus !== "logged_in" || !loginUser) {
      setCurrentUserPermissions({});
      setCurrentUserPermissionLoaded(false);
      return;
    }

    void fetchCurrentUserPermissions(loginUser);
  }, [loginStatus, loginUser?.id, loginUser?.role, loginUser?.organization]);

  useEffect(() => {
    if (loginStatus !== "logged_in") return;

    const timer = window.setInterval(() => {
      const savedLoginSession = window.sessionStorage.getItem(
        MASTER_DATA_LOGIN_SESSION_KEY
      );

      const savedLoginExpiresAt = getMasterDataLoginExpiresAt();

      if (
        isMasterDataLoginKeepAliveModalOpen &&
        savedLoginSession === "1"
      ) {
        refreshMasterDataLoginExpiresAt();
        return;
      }

      if (
        savedLoginSession !== "1" ||
        !Number.isFinite(savedLoginExpiresAt) ||
        savedLoginExpiresAt <= Date.now()
      ) {
        void handleLogout();
      }
    }, 30 * 1000);

    return () => {
      window.clearInterval(timer);
    };
  }, [loginStatus, isMasterDataLoginKeepAliveModalOpen]);

  useEffect(() => {
    if (loginStatus !== "logged_in") return;

    const refreshLoginSessionByActivity = () => {
      const savedLoginSession = window.sessionStorage.getItem(
        MASTER_DATA_LOGIN_SESSION_KEY
      );

      const savedLoginExpiresAt = getMasterDataLoginExpiresAt();

      if (savedLoginSession !== "1") {
        return;
      }

      if (isMasterDataLoginKeepAliveModalOpen) {
        refreshMasterDataLoginExpiresAt();
        return;
      }

      if (!Number.isFinite(savedLoginExpiresAt)) {
        return;
      }

      if (savedLoginExpiresAt <= Date.now()) {
        void handleLogout();
        return;
      }

      refreshMasterDataLoginExpiresAt();
    };

    const activityEvents: Array<keyof WindowEventMap> = [
      "mousemove",
      "mousedown",
      "keydown",
      "scroll",
      "wheel",
      "touchstart",
    ];

    activityEvents.forEach((eventName) => {
      window.addEventListener(eventName, refreshLoginSessionByActivity);
    });

    return () => {
      activityEvents.forEach((eventName) => {
        window.removeEventListener(eventName, refreshLoginSessionByActivity);
      });
    };
  }, [loginStatus, isMasterDataLoginKeepAliveModalOpen]);

  const checkLocalCrawlWorker = async () => {
    if (isLocalAppRuntime()) {
      setLocalCrawlWorker(null);
      setLocalCrawlWorkerError("");
      return null;
    }

    try {
      const res = await fetch(LOCAL_CRAWL_WORKER_STATUS_URL, {
        method: "GET",
        cache: "no-store",
      });

      const data = (await res.json()) as LocalCrawlWorkerStatus;

      if (!res.ok || !data.ok || !data.workerId) {
        throw new Error("workerが起動していません");
      }

      setLocalCrawlWorker(data);
      setLocalCrawlWorkerError("");
      return data;
    } catch {
      setLocalCrawlWorker(null);
      setLocalCrawlWorkerError(
        "このPCのworkerが起動していません。master-crawl-worker.exe を起動してください。"
      );
      return null;
    }
  };

  useEffect(() => {
    if (loginStatus !== "logged_in") return;

    void checkLocalCrawlWorker();

    const timer = window.setInterval(() => {
      void checkLocalCrawlWorker();
    }, 5000);

    return () => {
      window.clearInterval(timer);
    };
  }, [loginStatus]);

  useEffect(() => {
    return () => {
      clearMynaviStatusPolling();
    };
  }, []);

  useEffect(() => {
    setHeaderStickyTop(0);
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;

    const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");

    const updateSystemThemeMode = () => {
      setSystemThemeMode(mediaQuery.matches ? "dark" : "light");
    };

    updateSystemThemeMode();
    mediaQuery.addEventListener("change", updateSystemThemeMode);

    return () => {
      mediaQuery.removeEventListener("change", updateSystemThemeMode);
    };
  }, []);

  useLayoutEffect(() => {
    if (typeof document === "undefined") return;

    document.body.setAttribute("data-app-theme", effectiveThemeMode);

    return () => {
      document.body.removeAttribute("data-app-theme");
    };
  }, [effectiveThemeMode]);

  const handleOpenFilter = async (key: FilterKey) => {
    if (openFilterKey === key) {
      setOpenFilterKey(null);
      return;
    }

    if (!canShowListColumnFilters) {
      setOpenFilterKey(null);
      return;
    }

    setOpenAdvancedFilterKey(null);

    const nextOpenState: ColumnFilterState = {
      ...appliedColumnStates[key],
      selectedValues: appliedColumnStates[key].valueFilterEnabled
        ? [...appliedColumnStates[key].selectedValues]
        : [],

      allValues: appliedColumnStates[key].valueFilterEnabled
        ? [...appliedColumnStates[key].allValues]
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

      next[key].valueFilterEnabled = false;
      next[key].selectedValues = [];
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

    const closeAdvancedFilterModal = (returnToSearch = true) => {
      setOpenAdvancedFilterKey(null);

      if (!returnToSearch) {
        setAdvancedFilterReturnTarget(null);
        return;
      }

      if (advancedFilterReturnTarget === "permissionSearch") {
        setPermissionListScopeSearchOpen(true);
      }

      if (advancedFilterReturnTarget === "sidebarSearch") {
        setOpenSidebarPanel("search");
      }

      setAdvancedFilterReturnTarget(null);
    };

    const handleOpenAdvancedFilter = async (key: AdvancedFilterModalKey) => {
      if (openAdvancedFilterKey === key) {
        closeAdvancedFilterModal();
        return;
      }

      setAdvancedFilterReturnTarget(
        permissionListScopeOpen ? "permissionSearch" : "sidebarSearch"
      );

      if (!permissionListScopeOpen) {
        setOpenSidebarPanel(null);
      }

      setPermissionListScopeSearchOpen(false);
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

      if (nextPanel === "settings") {
        setSettingsTab("profile");
        setSettingsPasswordVisible(false);

        void fetchMasterDataLoginHistory(loginUser?.id).then((history) => {
          setLoginHistory(history);
        });

        if (!isMasterDataManagerRole(loginUser?.role)) {
          setPermissionEmployees([]);
          setSelectedPermissionEmployeeId("");
          setPermissionError("");
          setPermissionMessage("");
        }
      }

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
      closeAdvancedFilterModal();
    };

    const applyAdvancedFilter = () => {
      setAppliedAdvancedFilters(cloneAdvancedFiltersState(draftAdvancedFilters));
      setPage(1);
      closeAdvancedFilterModal();
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
      if (allFiltersClearConfirmTarget === "permission") {
        setDraftColumnStates((prev) => createClearedColumnStates(prev));
        setAppliedColumnStates((prev) => createClearedColumnStates(prev));
        setDraftAdvancedFilters(createInitialAdvancedFiltersState());
        setAppliedAdvancedFilters(createInitialAdvancedFiltersState());
        setPage(1);
        setOpenFilterKey(null);
        setOpenAdvancedFilterKey(null);
        setPermissionListScopeSearchOpen(false);
        setAdvancedFilterReturnTarget(null);
        setAllFiltersClearConfirmOpen(false);
        setAllFiltersClearConfirmTarget(null);
        return;
      }

      clearAllFilters();
      setAllFiltersClearConfirmOpen(false);
      setAllFiltersClearConfirmTarget(null);
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
                      <span
                        className="app-fit-one-line flex-1"
                        style={{ fontSize: getSingleLineFitFontSize(region) }}
                      >
                        {isRegionOpen ? "▼" : "▶"} {region}
                      </span>
                      <span className="shrink-0 text-sm font-bold text-slate-200">
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
                                <span
                                  className="app-fit-one-line flex-1"
                                  style={{ fontSize: getSingleLineFitFontSize(item.prefecture) }}
                                >
                                  {isPrefectureOpen ? "▼" : "▶"} {item.prefecture}
                                </span>
                                <span className="shrink-0 text-sm font-bold text-slate-200">
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
                                  <span
                                    className="app-fit-one-line min-w-0 flex-1"
                                    style={{ fontSize: getSingleLineFitFontSize(city) }}
                                  >
                                    {city}
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
                      <span
                        className="app-fit-one-line flex-1"
                        style={{ fontSize: getSingleLineFitFontSize(industryParent) }}
                      >
                        {isParentOpen ? "▼" : "▶"} {industryParent}
                      </span>
                      <span className="shrink-0 text-sm font-bold text-slate-200">
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
                                <span
                                  className="app-fit-one-line flex-1"
                                  style={{ fontSize: getSingleLineFitFontSize(item.bigIndustry) }}
                                >
                                  {isOpen ? "▼" : "▶"} {item.bigIndustry}
                                </span>
                                <span className="shrink-0 text-sm font-bold text-slate-200">
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
                                  <span
                                    className="app-fit-one-line min-w-0 flex-1"
                                    style={{ fontSize: getSingleLineFitFontSize(smallIndustry) }}
                                  >
                                    {smallIndustry}
                                  </span>
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
                      <span
                        className="app-fit-one-line flex-1"
                        style={{ fontSize: getSingleLineFitFontSize(item.parent) }}
                      >
                        {isOpen ? "▼" : "▶"} {item.parent}
                      </span>
                      <span className="shrink-0 text-sm font-bold text-slate-200">
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
                        <span
                          className="app-fit-one-line min-w-0 flex-1"
                          style={{ fontSize: getSingleLineFitFontSize(tag) }}
                        >
                          {tag}
                        </span>
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
      const visibleAdvancedFilterButtons = ADVANCED_FILTER_BUTTONS.filter((button) =>
        canUsePermission(getAdvancedFilterPermissionKey(button.key))
      );

      const searchPanelMeta: Record<
        AdvancedFilterModalKey,
        { icon: string; description: string }
      > = {
        companyName: {
          icon: "Aa",
          description: "企業名に含まれる文字で絞り込みます",
        },
        prefecture: {
          icon: "📍",
          description: "都道府県や市区町村から探します",
        },
        industry: {
          icon: "🏭",
          description: "大業種・小業種で分類します",
        },
        established: {
          icon: "📅",
          description: "設立年月の範囲で絞り込みます",
        },
        capital: {
          icon: "¥",
          description: "資本金の金額範囲で絞り込みます",
        },
        employeeCount: {
          icon: "👥",
          description: "従業員数の範囲で絞り込みます",
        },
        tag: {
          icon: "＃",
          description: "登録済みタグでリストを探します",
        },
      };

      return (
        <div className="space-y-4">
          {visibleAdvancedFilterButtons.length === 0 && (
            <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-8 text-center text-sm text-slate-400">
              使用できる検索項目がありません
            </div>
          )}

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            {visibleAdvancedFilterButtons.map((button) => {
              const active = hasActiveAdvancedFilter(
                button.key,
                appliedAdvancedFilters
              );
              const meta = searchPanelMeta[button.key];

              return (
                <button
                  key={button.key}
                  type="button"
                  onClick={() => {
                    setOpenSidebarPanel(null);
                    handleOpenAdvancedFilter(button.key);
                  }}
                  className={`group min-h-[118px] rounded-2xl border p-4 text-left transition ${
                    active
                      ? "border-sky-400/40 bg-gradient-to-br from-sky-500/20 via-[#0f172a] to-indigo-500/10 text-sky-100 shadow-[0_0_28px_rgba(56,189,248,0.16)] hover:bg-sky-500/25"
                      : "border-white/10 bg-gradient-to-br from-white/10 via-[#0f172a] to-[#0b1220] text-slate-200 hover:border-sky-300/30 hover:bg-white/10"
                  }`}
                >
                  <div className="mb-3 flex items-center justify-between gap-3">
                    <div
                      className={`inline-flex h-11 w-11 items-center justify-center rounded-2xl border text-sm font-bold ${
                        active
                          ? "border-sky-300/30 bg-sky-400/20 text-sky-100"
                          : "border-white/10 bg-white/5 text-slate-100 group-hover:border-sky-300/30 group-hover:bg-sky-400/10"
                      }`}
                    >
                      {meta.icon}
                    </div>

                    {active && (
                      <span className="rounded-full border border-sky-300/30 bg-sky-400/10 px-2 py-1 text-[10px] font-semibold text-sky-100">
                        適用中
                      </span>
                    )}
                  </div>

                  <div className="text-sm font-semibold text-slate-100">
                    {button.label}
                  </div>
                  <div className="mt-2 text-xs leading-5 text-slate-400">
                    {meta.description}
                  </div>
                </button>
              );
            })}
          </div>
        </div>
      );
    }

    if (openSidebarPanel === "list") {
      return (
        <div className="space-y-4">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            {!canUseAnyPermission([
              "list.delete",
              "list.add",
              "list.itemDelete",
              "list.dedupe",
            ]) && (
              <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-8 text-center text-sm text-slate-400 sm:col-span-2">
                使用できるリスト操作がありません
              </div>
            )}

            {canUsePermission("list.add") && (
              <button
                type="button"
                onClick={handleOpenListAdd}
                disabled={mynaviLoading}
                className="group min-h-[132px] rounded-2xl border border-emerald-300/20 bg-gradient-to-br from-emerald-500/18 via-[#0f172a] to-[#0b1220] p-4 text-left transition hover:border-emerald-300/40 hover:bg-emerald-500/10 disabled:opacity-50"
              >
                <div className="mb-3 inline-flex h-11 w-11 items-center justify-center rounded-2xl border border-emerald-300/25 bg-emerald-400/10 text-lg text-emerald-100">
                  ＋
                </div>
                <div className="text-sm font-semibold text-slate-100">
                  リスト追加
                </div>
                <div className="mt-2 text-xs leading-5 text-slate-400">
                  新しい企業リストを取り込み、管理対象に追加します
                </div>
              </button>
            )}

            {canUsePermission("list.delete") && (
              <button
                type="button"
                onClick={() => setListDeleteScopeOpen(true)}
                disabled={listDeleting}
                className="group min-h-[132px] rounded-2xl border border-rose-300/20 bg-gradient-to-br from-rose-500/18 via-[#0f172a] to-[#0b1220] p-4 text-left transition hover:border-rose-300/40 hover:bg-rose-500/10 disabled:opacity-50"
              >
                <div className="mb-3 inline-flex h-11 w-11 items-center justify-center rounded-2xl border border-rose-300/25 bg-rose-400/10 text-lg text-rose-100">
                  🗑
                </div>
                <div className="text-sm font-semibold text-slate-100">
                  リスト削除
                </div>
                <div className="mt-2 text-xs leading-5 text-slate-400">
                  条件に合うリスト、または全リストを削除します
                </div>
              </button>
            )}

            {canUsePermission("list.itemDelete") && (
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
                className="group min-h-[132px] rounded-2xl border border-cyan-300/20 bg-gradient-to-br from-cyan-500/18 via-[#0f172a] to-[#0b1220] p-4 text-left transition hover:border-cyan-300/40 hover:bg-cyan-500/10 disabled:opacity-50"
              >
                <div className="mb-3 inline-flex h-11 w-11 items-center justify-center rounded-2xl border border-cyan-300/25 bg-cyan-400/10 text-lg text-cyan-100">
                  ✂
                </div>
                <div className="text-sm font-semibold text-slate-100">
                  項目削除
                </div>
                <div className="mt-2 text-xs leading-5 text-slate-400">
                  選択した項目の中身だけをまとめて削除します
                </div>
              </button>
            )}

            {canUsePermission("list.dedupe") && (
              <button
                type="button"
                onClick={() => {
                  setDedupeTargetScope(null);
                  setDedupeSelectedField(null);
                  setDedupeMatchMethod(null);
                  setDedupeFieldOpen(false);
                  setDedupeMethodOpen(false);
                  setDedupeScopeOpen(true);
                }}
                disabled={deduplicating}
                className="master-data-dedupe-menu-button group min-h-[132px] rounded-2xl border border-violet-300/20 bg-gradient-to-br from-violet-500/18 via-[#0f172a] to-[#0b1220] p-4 text-left transition hover:border-violet-300/40 hover:bg-violet-500/10 disabled:opacity-50"
              >
                <div className="mb-3 inline-flex h-11 w-11 items-center justify-center rounded-2xl border border-violet-300/25 bg-violet-400/10 text-lg text-violet-100">
                  ⧉
                </div>
                <div className="text-sm font-semibold text-slate-100">
                  重複削除
                </div>
                <div className="mt-2 text-xs leading-5 text-slate-400">
                  重複している企業データを整理します
                </div>
              </button>
            )}
          </div>
        </div>
      );
    }

    if (openSidebarPanel === "csv") {
      return (
        <div className="space-y-4">
          {canUsePermission("csv.import") && (
            <label className="group flex min-h-[118px] cursor-pointer items-center gap-4 rounded-2xl border border-emerald-300/20 bg-gradient-to-br from-emerald-500/18 via-[#0f172a] to-[#0b1220] p-4 transition hover:border-emerald-300/40 hover:bg-emerald-500/10">
              <span className="inline-flex h-12 w-12 shrink-0 items-center justify-center rounded-2xl border border-emerald-300/25 bg-emerald-400/10 text-lg text-emerald-100">
                📁
              </span>

              <span className="min-w-0 flex-1">
                <span className="block text-sm font-semibold text-slate-100">
                  ファイル選択
                </span>
                <span className="mt-1 block text-xs leading-5 text-slate-400">
                  CSVファイルを選択して、投入候補として追加します
                </span>
              </span>

              <span className="shrink-0 rounded-full border border-emerald-300/25 bg-emerald-400/10 px-3 py-1 text-xs font-semibold text-emerald-100">
                CSV
              </span>

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
                className="hidden"
              />
            </label>
          )}

          {canUsePermission("csv.import") && selectedFiles.length > 0 && (
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

          <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
            {!canUseAnyPermission(["csv.import", "csv.export", "csv.template"]) && (
              <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-8 text-center text-sm text-slate-400 sm:col-span-3">
                使用できるCSV操作がありません
              </div>
            )}

            {canUsePermission("csv.import") && (
              <button
                onClick={handleImportClick}
                disabled={importing}
                className="group min-h-[126px] rounded-2xl border border-emerald-300/20 bg-gradient-to-br from-emerald-500/18 via-[#0f172a] to-[#0b1220] p-4 text-left transition hover:border-emerald-300/40 hover:bg-emerald-500/10 disabled:opacity-50"
              >
                <div className="mb-3 inline-flex h-11 w-11 items-center justify-center rounded-2xl border border-emerald-300/25 bg-emerald-400/10 text-emerald-100">
                  <svg
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    className="h-5 w-5"
                  >
                    <path d="M12 19V5" />
                    <path d="M6 11l6-6 6 6" />
                    <path d="M5 19h14" />
                  </svg>
                </div>
                <div className="text-sm font-semibold text-slate-100">
                  CSV投入
                </div>
                <div className="mt-2 text-xs leading-5 text-slate-400">
                  選択したCSVをデータベースに取り込みます
                </div>
              </button>
            )}

            {canUsePermission("csv.export") && (
              <button
                onClick={handleExportClick}
                className="group min-h-[126px] rounded-2xl border border-teal-300/20 bg-gradient-to-br from-teal-500/18 via-[#0f172a] to-[#0b1220] p-4 text-left transition hover:border-teal-300/40 hover:bg-teal-500/10"
              >
                <div className="mb-3 inline-flex h-11 w-11 items-center justify-center rounded-2xl border border-teal-300/25 bg-teal-400/10 text-teal-100">
                  <svg
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    className="h-5 w-5"
                  >
                    <path d="M12 5v14" />
                    <path d="M18 13l-6 6-6-6" />
                    <path d="M5 5h14" />
                  </svg>
                </div>
                <div className="text-sm font-semibold text-slate-100">
                  CSV抽出
                </div>
                <div className="mt-2 text-xs leading-5 text-slate-400">
                  現在のリストをCSVとして出力します
                </div>
              </button>
            )}

            {canUsePermission("csv.template") && (
              <button
                onClick={handleDownloadTemplate}
                                className="master-data-csv-template-button group min-h-[126px] rounded-2xl border border-indigo-300/20 bg-gradient-to-br from-indigo-500/18 via-[#0f172a] to-[#0b1220] p-4 text-left transition hover:border-indigo-300/40 hover:bg-indigo-500/10"
              >
                <div className="mb-3 inline-flex h-11 w-11 items-center justify-center rounded-2xl border border-indigo-300/25 bg-indigo-400/10 text-lg text-indigo-100">
                  ⬚
                </div>
                <div className="text-sm font-semibold text-slate-100">
                  CSVテンプレート
                </div>
                <div className="mt-2 text-xs leading-5 text-slate-400">
                  投入用CSVのひな形をダウンロードします
                </div>
              </button>
            )}
          </div>
        </div>
      );
    }

    if (openSidebarPanel === "inspection") {
      return (
        <div className="space-y-4">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            {!canUseCrawlPanel && !canUseItemInspectionPanel && (
              <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-8 text-center text-sm text-slate-400 sm:col-span-2">
                使用できる精査操作がありません
              </div>
            )}

            {canUseCrawlPanel && (
              <button
                type="button"
                onClick={() => {
                  setCrawlFieldSelections(createVisibleCrawlFieldSelections(false));
                  setCrawlTargetScope(null);
                  setCrawlScopeOpen(true);
                }}
                disabled={crawling}
                className="master-data-crawl-menu-button group min-h-[138px] rounded-2xl border border-amber-300/20 bg-gradient-to-br from-amber-500/18 via-[#0f172a] to-[#0b1220] p-4 text-left transition hover:border-amber-300/40 hover:bg-amber-500/10 disabled:opacity-50"
              >
                <div className="mb-3 inline-flex h-11 w-11 items-center justify-center rounded-2xl border border-amber-300/25 bg-amber-400/10 text-lg text-amber-100">
                  🤖
                </div>
                <div className="text-sm font-semibold text-slate-100">
                  クローリング
                </div>
                <div className="mt-2 text-xs leading-5 text-slate-400">
                  企業サイトを確認し、選択した項目の候補を取得します
                </div>
              </button>
            )}

            {canUseItemInspectionPanel && (
              <button
                type="button"
                onClick={() => {
                  setItemInspectionSelections(createVisibleItemInspectionSelections(false));
                  setItemInspectionMethodSelections(
                    createEmptyItemInspectionMethodSelections()
                  );
                  setItemInspectionPreviewChanges([]);
                  setItemInspectionExcludedPreviewRows([]);
                  setItemInspectionExcludedTotalCount(0);
                  setItemInspectionPreviewTab("candidate");
                  setItemInspectionPreviewPage(1);
                  setItemInspectionCheckedPreviewRowIds({});
                  setItemInspectionMessage("");
                  setItemInspectionError("");
                  setItemInspectionMethodOpen(false);
                  setItemInspectionPreviewConfirmOpen(false);
                  setItemInspectionTargetScope(null);
                  setItemInspectionScopeOpen(true);
                }}
                disabled={itemInspecting}
                className="group min-h-[138px] rounded-2xl border border-cyan-300/20 bg-gradient-to-br from-cyan-500/18 via-[#0f172a] to-[#0b1220] p-4 text-left transition hover:border-cyan-300/40 hover:bg-cyan-500/10 disabled:opacity-50"
              >
                <div className="mb-3 inline-flex h-11 w-11 items-center justify-center rounded-2xl border border-cyan-300/25 bg-cyan-400/10 text-lg text-cyan-100">
                  ✓
                </div>
                <div className="text-sm font-semibold text-slate-100">
                  項目精査
                </div>
                <div className="mt-2 text-xs leading-5 text-slate-400">
                  登録済みデータを確認し、不要な値や修正候補を精査します
                </div>
              </button>
            )}
          </div>
        </div>
      );
    }

    if (openSidebarPanel === "settings") {
      const nameParts = splitMasterDataLoginName(loginUser?.name);

      return (
        <div className="space-y-5">
          <div
            className={`sticky top-0 z-30 grid gap-2 rounded-2xl border border-sky-300/10 bg-gradient-to-br from-white/8 via-[#0f172a]/95 to-[#0b1220]/95 p-2 shadow-[0_12px_30px_rgba(0,0,0,0.25)] backdrop-blur-xl ${
              isMasterDataManagerRole(loginUser?.role) ? "grid-cols-3" : "grid-cols-2"
            }`}
          >
            <button
              type="button"
              onClick={() => setSettingsTab("profile")}
              className={`group flex h-12 items-center justify-center gap-2 rounded-xl border px-3 text-sm font-bold transition ${
                settingsTab === "profile"
                  ? "border-sky-400/40 bg-gradient-to-br from-sky-500/24 via-white/8 to-indigo-500/10 text-sky-100 shadow-[0_0_24px_rgba(56,189,248,0.14)]"
                  : "border-white/10 bg-white/5 text-slate-300 hover:border-sky-300/30 hover:bg-white/10"
              }`}
            >
              <span className="inline-flex h-7 w-7 items-center justify-center rounded-lg border border-violet-300/20 bg-violet-400/10">
                👤
              </span>
              <span className="font-black" style={{ fontWeight: 900 }}>
                プロフィール
              </span>
            </button>

            {isMasterDataManagerRole(loginUser?.role) && (
              <button
                type="button"
                onClick={() => {
                  setSettingsTab("permissionManagement");
                  void fetchPermissionEmployees();
                }}
                className={`group flex h-12 items-center justify-center gap-2 rounded-xl border px-3 text-sm font-bold transition ${
                  settingsTab === "permissionManagement"
                    ? "border-sky-400/40 bg-gradient-to-br from-sky-500/24 via-white/8 to-indigo-500/10 text-sky-100 shadow-[0_0_24px_rgba(56,189,248,0.14)]"
                    : "border-white/10 bg-white/5 text-slate-300 hover:border-sky-300/30 hover:bg-white/10"
                }`}
              >
                <span className="inline-flex h-7 w-7 items-center justify-center rounded-lg border border-violet-300/20 bg-violet-400/10">
                  🔐
                </span>
                <span className="font-black" style={{ fontWeight: 900 }}>
                  権限管理
                </span>
              </button>
            )}

            <button
              type="button"
              onClick={() => setSettingsTab("loginHistory")}
              className={`group flex h-12 items-center justify-center gap-2 rounded-xl border px-3 text-sm font-bold transition ${
                settingsTab === "loginHistory"
                  ? "border-sky-400/40 bg-gradient-to-br from-sky-500/24 via-white/8 to-indigo-500/10 text-sky-100 shadow-[0_0_24px_rgba(56,189,248,0.14)]"
                  : "border-white/10 bg-white/5 text-slate-300 hover:border-sky-300/30 hover:bg-white/10"
              }`}
            >
              <span className="inline-flex h-7 w-7 items-center justify-center rounded-lg border border-violet-300/20 bg-violet-400/10">
                🕘
              </span>
              <span className="font-black" style={{ fontWeight: 900 }}>
                ログイン履歴
              </span>
            </button>
          </div>

          {settingsTab === "profile" && (
            <div className="space-y-4">
              <div className="rounded-2xl border border-white/10 bg-gradient-to-br from-sky-500/10 via-[#0f172a] to-indigo-500/10 p-5">
                <div className="mb-4 flex items-center gap-3">
                  <div className="inline-flex h-12 w-12 items-center justify-center rounded-2xl border border-sky-300/20 bg-sky-400/10 text-sky-100">
                    <svg
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="1.8"
                      className="h-6 w-6"
                    >
                      <path d="M20 21a8 8 0 0 0-16 0" />
                      <circle cx="12" cy="8" r="4" />
                    </svg>
                  </div>

                  <div className="min-w-0">
                    <div className="text-sm font-semibold text-slate-100">
                      プロフィール
                    </div>
                    <div className="mt-1 text-xs text-slate-400">
                      ログイン中のアカウント情報を表示しています
                    </div>
                  </div>
                </div>

                <div className="space-y-3">
                  <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                    <div className="rounded-xl border border-white/10 bg-[#0b1220]/80 p-4">
                      <div className="mb-2 text-xs font-semibold text-slate-400">
                        姓
                      </div>
                      <div className="truncate text-sm font-semibold text-slate-100">
                        {nameParts.lastName}
                      </div>
                    </div>

                    <div className="rounded-xl border border-white/10 bg-[#0b1220]/80 p-4">
                      <div className="mb-2 text-xs font-semibold text-slate-400">
                        名
                      </div>
                      <div className="truncate text-sm font-semibold text-slate-100">
                        {nameParts.firstName}
                      </div>
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-[#0b1220]/80 p-4">
                    <div className="mb-2 text-xs font-semibold text-slate-400">
                      所属
                    </div>
                    <div
                      className="truncate text-sm font-semibold text-slate-100"
                      title={loginUser?.organization ?? ""}
                    >
                      {loginUser?.organization ?? "-"}
                    </div>
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-white/10 bg-[#0f172a] p-5">
                <div className="mb-4 text-sm font-semibold text-slate-100">
                  ログイン情報
                </div>

                <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
                  <div className="rounded-xl border border-white/10 bg-[#0b1220]/80 p-4">
                    <div className="mb-2 text-xs font-semibold text-slate-400">
                      ID
                    </div>
                    <div
                      className="truncate text-sm font-semibold text-slate-100"
                      title={loginUser?.id ?? ""}
                    >
                      {loginUser?.id ?? "-"}
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-[#0b1220]/80 p-4">
                    <div className="mb-2 text-xs font-semibold text-slate-400">
                      パスワード
                    </div>

                    <div className="relative">
                      <div
                        className="truncate pr-10 text-sm font-semibold text-slate-100"
                        title={settingsPasswordVisible ? loginUser?.password ?? "" : ""}
                      >
                        {loginUser?.password
                          ? settingsPasswordVisible
                            ? loginUser.password
                            : "•".repeat(Math.max(loginUser.password.length, 8))
                          : "-"}
                      </div>

                      {loginUser?.password && (
                        <button
                          type="button"
                          onClick={() =>
                            setSettingsPasswordVisible((prev) => !prev)
                          }
                          className="absolute right-0 top-1/2 inline-flex h-8 w-8 -translate-y-1/2 items-center justify-center rounded-lg text-slate-400 transition hover:bg-white/10 hover:text-slate-100"
                          aria-label={
                            settingsPasswordVisible
                              ? "パスワードを隠す"
                              : "パスワードを表示する"
                          }
                        >
                          {settingsPasswordVisible ? (
                            <svg
                              viewBox="0 0 24 24"
                              fill="none"
                              stroke="currentColor"
                              strokeWidth="1.8"
                              className="h-5 w-5"
                            >
                              <path d="M3 3l18 18" />
                              <path d="M10.7 10.7a2 2 0 0 0 2.6 2.6" />
                              <path d="M9.5 5.3A9.5 9.5 0 0 1 12 5c5 0 8.5 4.5 9.5 7-0.4 1-1.3 2.4-2.7 3.7" />
                              <path d="M6.2 6.8C4.4 8.1 3.2 10.1 2.5 12c1 2.5 4.5 7 9.5 7 1.5 0 2.8-.4 4-1" />
                            </svg>
                          ) : (
                            <svg
                              viewBox="0 0 24 24"
                              fill="none"
                              stroke="currentColor"
                              strokeWidth="1.8"
                              className="h-5 w-5"
                            >
                              <path d="M2.5 12S6 5 12 5s9.5 7 9.5 7-3.5 7-9.5 7-9.5-7-9.5-7Z" />
                              <circle cx="12" cy="12" r="3" />
                            </svg>
                          )}
                        </button>
                      )}
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-[#0b1220]/80 p-4">
                    <div className="mb-2 text-xs font-semibold text-slate-400">
                      ロール
                    </div>
                    <div
                      className={`inline-flex rounded-full border px-3 py-1 text-xs font-semibold ${getMasterDataRoleBadgeClass(
                        loginUser?.role
                      )}`}
                    >
                      {loginUser?.role ?? "-"}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {permissionListScopeOpen &&
            selectedPermissionEmployee &&
            typeof document !== "undefined" &&
            createPortal(
              <div
                className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
                onClick={closePermissionListScopeModal}
              >
                <div className="flex min-h-full items-center justify-center">
                  <div
                    className="flex h-[calc(100dvh-16px)] w-[calc(100vw-16px)] max-w-none flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                      <div>
                        <div className="text-sm font-semibold text-slate-100">
                          リストの絞り込み範囲
                        </div>
                        <div className="mt-1 text-xs text-slate-400">
                          ここで表示されているリスト範囲を、このアカウントに適用します
                        </div>
                      </div>

                      <button
                        type="button"
                        onClick={closePermissionListScopeModal}
                        className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                      >
                        ×
                      </button>
                    </div>

                    <div className="flex flex-1 flex-col overflow-hidden px-4 py-3">
                      <div className="mb-3 flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-sky-300/20 bg-gradient-to-br from-sky-500/14 via-[#0f172a]/90 to-[#0b1220]/95 px-4 py-3 shadow-[0_16px_40px_rgba(0,0,0,0.22)]">
                        <div className="min-w-[360px] flex-1 text-sm font-medium leading-6 text-sky-100">
                          <span className="block">
                            上部のリスト内フィルタや検索条件で絞り込んだ状態を確認してください。
                          </span>
                          <span className="mt-1 block">
                            右下の「適用」を押すと、このアカウントはその絞り込み範囲のリストだけを操作対象にできます。
                          </span>
                        </div>

                        <div className="flex flex-wrap items-center justify-end gap-2">
                          <button
                            type="button"
                            onClick={() => setPermissionListScopeSearchOpen(true)}
                            className="group inline-flex min-h-[72px] min-w-[170px] flex-col items-center justify-center gap-1 rounded-2xl border border-sky-300/30 bg-gradient-to-br from-sky-500/24 via-white/8 to-indigo-500/10 px-4 py-2 text-sm font-black text-sky-100 shadow-[0_0_24px_rgba(56,189,248,0.14)] transition hover:border-sky-300/50 hover:bg-sky-500/20"
                          >
                            <span className="inline-flex h-7 w-7 items-center justify-center rounded-xl border border-sky-300/25 bg-sky-400/10">
                              🔎
                            </span>
                            <span className="font-black" style={{ fontWeight: 900 }}>
                              検索
                            </span>
                          </button>

                          <button
                            type="button"
                            onClick={() => {
                              setPermissionListScopeSearchOpen(false);
                              setOpenFilterKey(null);
                              setOpenAdvancedFilterKey(null);
                              setAdvancedFilterReturnTarget(null);
                              setAllFiltersClearConfirmTarget("permission");
                              setAllFiltersClearConfirmOpen(true);
                            }}
                            className="group inline-flex min-h-[72px] min-w-[170px] flex-col items-center justify-center gap-1 rounded-2xl border border-sky-300/30 bg-gradient-to-br from-sky-500/24 via-white/8 to-indigo-500/10 px-4 py-2 text-sm font-black text-sky-100 shadow-[0_0_24px_rgba(56,189,248,0.14)] transition hover:border-sky-300/50 hover:bg-sky-500/20"
                          >
                            <span className="inline-flex h-7 w-7 items-center justify-center rounded-xl border border-sky-300/25 bg-sky-400/10">
                              ↺
                            </span>
                            <span className="font-black" style={{ fontWeight: 900 }}>
                              フィルタ解除
                            </span>
                          </button>

                          <div className="flex min-h-[72px] min-w-[170px] flex-col items-center justify-center rounded-2xl border border-sky-300/15 bg-gradient-to-br from-sky-500/12 via-white/5 to-[#0b1220] px-3 py-2 text-center">
                            <span className="text-[11px] font-semibold text-slate-400">総件数</span>
                            <span className="mt-0.5 text-sm font-bold text-white">
                              {total.toLocaleString()}件
                            </span>
                          </div>

                          <div className="flex min-h-[72px] min-w-[170px] flex-col items-center justify-center overflow-visible rounded-2xl border border-indigo-300/15 bg-gradient-to-br from-indigo-500/12 via-white/5 to-[#0b1220] px-3 py-2 text-center">
                            <span className="text-[11px] font-semibold text-slate-400">
                              現在ページ
                            </span>

                            <PageSelectDropdown
                              page={page}
                              totalPages={totalPages}
                              disabled={limit === "all"}
                              open={openPageDropdown === "permissionHeader"}
                              onToggle={() =>
                                setOpenPageDropdown((current) =>
                                  current === "permissionHeader" ? null : "permissionHeader"
                                )
                              }
                              onClose={() => setOpenPageDropdown(null)}
                              onSelect={(pageNumber) => {
                                setPage(pageNumber);
                                setOpenPageDropdown(null);
                              }}
                            />
                          </div>

                          <div className="flex min-h-[72px] min-w-[170px] flex-col items-center justify-center rounded-2xl border border-cyan-300/15 bg-gradient-to-br from-cyan-500/12 via-white/5 to-[#0b1220] px-3 py-2 text-center">
                            <span className="text-[11px] font-semibold text-slate-400">
                              総ページ数
                            </span>
                            <span className="mt-0.5 text-sm font-bold text-white">
                              {totalPages.toLocaleString()}
                            </span>
                          </div>

                          <div className="flex min-h-[72px] min-w-[170px] flex-col items-center justify-center rounded-2xl border border-emerald-300/15 bg-gradient-to-br from-emerald-500/12 via-white/5 to-[#0b1220] px-3 py-2 text-center">
                            <span className="text-[11px] font-semibold text-slate-400">
                              表示件数
                            </span>

                            <div
                              className="relative mt-1"
                              onBlur={(e) => {
                                if (!e.currentTarget.contains(e.relatedTarget as Node | null)) {
                                  setOpenPageSizeDropdown(null);
                                }
                              }}
                            >
                              <button
                                type="button"
                                onClick={() =>
                                  setOpenPageSizeDropdown((current) =>
                                    current === "permission" ? null : "permission"
                                  )
                                }
                                className="group/size relative flex h-9 min-w-[100px] items-center justify-between gap-2 rounded-xl border border-emerald-300/30 bg-gradient-to-br from-emerald-400/15 via-[#0f172a] to-[#07111f] px-3 text-xs font-black text-slate-100 shadow-inner outline-none transition hover:border-emerald-300/50 hover:bg-emerald-500/15 focus:border-emerald-300/60 focus:ring-2 focus:ring-emerald-300/20"
                              >
                                <span className="font-black" style={{ fontWeight: 900 }}>
                                  {pageSizeOptions.find((opt) => opt.value === limit)?.label ?? `${limit}件`}
                                </span>
                                <span className="text-[10px] font-black text-emerald-100 transition group-hover/size:translate-y-0.5">
                                  ▾
                                </span>
                              </button>

                              {openPageSizeDropdown === "permission" && (
                                <div className="absolute left-1/2 top-[calc(100%+8px)] z-[120] w-[118px] -translate-x-1/2 overflow-hidden rounded-2xl border border-emerald-300/25 bg-[#07111f]/98 p-1.5 shadow-[0_18px_44px_rgba(0,0,0,0.45)] backdrop-blur-xl">
                                  {pageSizeOptions.map((opt) => {
                                    const active = limit === opt.value;

                                    return (
                                      <button
                                        key={opt.value}
                                        type="button"
                                        onMouseDown={(e) => e.preventDefault()}
                                        onClick={() => {
                                          setLimit(opt.value);
                                          setPage(1);
                                          setOpenPageSizeDropdown(null);
                                        }}
                                        className={`flex h-9 w-full items-center justify-center rounded-xl text-sm font-black transition ${
                                          active
                                            ? "bg-gradient-to-r from-emerald-400 to-cyan-400 text-[#03131f] shadow-[0_0_18px_rgba(45,212,191,0.24)]"
                                            : "text-slate-100 hover:bg-emerald-400/12 hover:text-emerald-100"
                                        }`}
                                        style={{ fontWeight: 900 }}
                                      >
                                        {opt.label}
                                      </button>
                                    );
                                  })}
                                </div>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>

                      <div className="mb-2 text-xs text-slate-400">
                        対象アカウント：{selectedPermissionEmployee.name}
                      </div>

                      <div
                        ref={permissionListScopeScrollRef}
                        className="app-scrollbar min-h-0 flex-1 overflow-auto rounded-xl border border-white/10 bg-[#0b1326]/90"
                      >
                        <div className="min-w-[5230px]">
                          <div
                            className="sticky top-0 z-20 grid border-b border-white/10 bg-[#162033]/95 backdrop-blur-xl"
                            style={{
                              gridTemplateColumns: GRID_TEMPLATE,
                            }}
                          >
                            {COLUMN_DEFS.map((column) => (
                              <HeaderCell
                                key={`permission-scope-${column.key}`}
                                label={column.label}
                                filterKey={column.key}
                                filterState={draftColumnStates[column.key]}
                                isOpen={openFilterKey === column.key}
                                canShowFilterButton={true}
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
                                  setSingleFilterClearConfirm({
                                    type: "column",
                                    key,
                                  })
                                }
                                onApply={applyColumnFilter}
                                onClear={clearColumnFilter}
                              />
                            ))}
                          </div>

                          {rows.length === 0 ? (
                            <div className="px-4 py-10 text-center text-sm text-slate-500">
                              表示できるリストがありません
                            </div>
                          ) : (
                            rows.map((row, rowIndex) => (
                              <div
                                key={`permission-scope-row-${rowIndex}`}
                                className="grid border-b border-white/5 bg-[#0f172a]/60 transition hover:bg-[#162033]"
                                style={{ gridTemplateColumns: GRID_TEMPLATE }}
                              >
                                {COLUMN_DEFS.map((column) => {
                                  const value = row[column.key];

                                  return (
                                    <div
                                      key={`permission-scope-cell-${rowIndex}-${column.key}`}
                                      title={value || ""}
                                      className="app-table-cell px-4 py-3 text-sm text-slate-100"
                                    >
                                      {value && value.trim() !== "" ? (
                                        value
                                      ) : (
                                        <span className="text-slate-500">-</span>
                                      )}
                                    </div>
                                  );
                                })}
                              </div>
                            ))
                          )}
                        </div>
                      </div>
                    </div>

                      <div className="mx-4 mt-2 mb-3 flex flex-wrap items-center justify-center gap-2 rounded-2xl border border-white/10 bg-gradient-to-br from-white/8 via-[#0b1326]/85 to-[#08101d]/90 p-2 shadow-[0_18px_44px_rgba(0,0,0,0.22)]">
                        <button
                          type="button"
                          onClick={() => setPage(1)}
                          disabled={page <= 1 || limit === "all"}
                          className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-sky-300/30 hover:bg-sky-500/10 hover:text-sky-100 disabled:opacity-40"
                        >
                          最初
                        </button>

                        <button
                          type="button"
                          onClick={() => setPage((p) => Math.max(1, p - 1))}
                          disabled={page <= 1 || limit === "all"}
                          className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-sky-300/30 hover:bg-sky-500/10 hover:text-sky-100 disabled:opacity-40"
                        >
                          前へ
                        </button>

                        {pageNumbers[0] > 1 && (
                          <span className="px-1 text-sm text-slate-500">...</span>
                        )}

                        {pageNumbers.map((n) => (
                          <button
                            key={`permission-scope-page-${n}`}
                            type="button"
                            onClick={() => setPage(n)}
                            disabled={limit === "all"}
                            className={`rounded-xl border px-4 py-2 text-sm font-semibold transition disabled:opacity-40 ${
                              page === n
                                ? "border-sky-400/40 bg-sky-500/20 text-sky-100"
                                : "border-white/10 bg-white/5 text-slate-200 hover:border-sky-300/30 hover:bg-sky-500/10 hover:text-sky-100"
                            }`}
                          >
                            {n}
                          </button>
                        ))}

                        {pageNumbers[pageNumbers.length - 1] < totalPages && (
                          <span className="px-1 text-sm text-slate-500">...</span>
                        )}

                        <button
                          type="button"
                          onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                          disabled={page >= totalPages || limit === "all"}
                          className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-sky-300/30 hover:bg-sky-500/10 hover:text-sky-100 disabled:opacity-40"
                        >
                          次へ
                        </button>

                        <button
                          type="button"
                          onClick={() => setPage(totalPages)}
                          disabled={page >= totalPages || limit === "all"}
                          className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-sky-300/30 hover:bg-sky-500/10 hover:text-sky-100 disabled:opacity-40"
                        >
                          最後
                        </button>
                      </div>

                    <div className="flex justify-end gap-2 border-t border-white/10 px-4 py-4">
                      <button
                        type="button"
                        onClick={closePermissionListScopeModal}
                        className="h-10 rounded-xl border border-white/10 bg-white/5 px-4 text-sm font-medium text-slate-200 transition hover:bg-white/10"
                      >
                        キャンセル
                      </button>

                      <button
                        type="button"
                        onClick={handleApplyPermissionListScope}
                        className="h-10 rounded-xl bg-sky-500 px-5 text-sm font-medium text-white transition hover:bg-sky-400"
                      >
                        適用
                      </button>
                    </div>
                  </div>
                </div>
              </div>,
              document.body
            )}

            {permissionListScopeSearchOpen &&
              selectedPermissionEmployee &&
              typeof document !== "undefined" &&
              createPortal(
                <div
                  className="app-modal-root fixed inset-0 z-[10000] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
                  onClick={() => setPermissionListScopeSearchOpen(false)}
                >
                  <div className="flex min-h-full items-center justify-center">
                    <div
                      className="flex w-full max-w-[760px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                        <div>
                          <div className="text-sm font-semibold text-slate-100">
                            検索
                          </div>
                          <div className="mt-1 text-xs text-slate-400">
                            絞り込みに使う項目を選択してください
                          </div>
                        </div>

                        <button
                          type="button"
                          onClick={() => setPermissionListScopeSearchOpen(false)}
                          className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                        >
                          ×
                        </button>
                      </div>

                      <div className="p-4">
                        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                          {ADVANCED_FILTER_BUTTONS.filter((button) =>
                            canUsePermission(getAdvancedFilterPermissionKey(button.key))
                          ).map((button) => {
                            const active = hasActiveAdvancedFilter(
                              button.key,
                              appliedAdvancedFilters
                            );
                            const meta = ADVANCED_FILTER_PANEL_META[button.key];

                            return (
                              <button
                                key={`permission-scope-search-popup-${button.key}`}
                                type="button"
                                onClick={() => {
                                  setPermissionListScopeSearchOpen(false);
                                  void handleOpenAdvancedFilter(button.key);
                                }}
                                className={`group min-h-[118px] rounded-2xl border p-4 text-left transition ${
                                  active
                                    ? "border-sky-400/40 bg-gradient-to-br from-sky-500/20 via-[#0f172a] to-indigo-500/10 text-sky-100 shadow-[0_0_28px_rgba(56,189,248,0.16)] hover:bg-sky-500/25"
                                    : "border-white/10 bg-gradient-to-br from-white/10 via-[#0f172a] to-[#0b1220] text-slate-200 hover:border-sky-300/30 hover:bg-white/10"
                                }`}
                              >
                                <div className="mb-3 flex items-center justify-between gap-3">
                                  <div
                                    className={`inline-flex h-11 w-11 items-center justify-center rounded-2xl border text-sm font-bold ${
                                      active
                                        ? "border-sky-300/30 bg-sky-400/20 text-sky-100"
                                        : "border-white/10 bg-white/5 text-slate-100 group-hover:border-sky-300/30 group-hover:bg-sky-400/10"
                                    }`}
                                  >
                                    {meta.icon}
                                  </div>

                                  {active && (
                                    <span className="rounded-full border border-sky-300/30 bg-sky-400/10 px-2 py-1 text-[10px] font-semibold text-sky-100">
                                      適用中
                                    </span>
                                  )}
                                </div>

                                <div className="text-sm font-semibold text-slate-100">
                                  {button.label}
                                </div>
                                <div className="mt-2 text-xs leading-5 text-slate-400">
                                  {meta.description}
                                </div>
                              </button>
                            );
                          })}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>,
                document.body
              )}

          {settingsTab === "loginHistory" && (
            <div className="rounded-2xl border border-white/10 bg-[#0f172a] p-5">
              <div className="mb-4 flex items-center justify-between gap-3">
                <div>
                  <div className="text-sm font-semibold text-slate-100">
                    ログイン履歴
                  </div>
                  <div className="mt-1 text-xs text-slate-400">
                    最近のログインイベントを最大50件表示します
                  </div>
                </div>

                <div className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs font-semibold text-slate-300">
                  {loginHistory.length.toLocaleString()}件
                </div>
              </div>

              <div className="overflow-hidden rounded-xl border border-white/10">
                <div className="grid grid-cols-[1.1fr_0.9fr_0.8fr] border-b border-white/10 bg-white/5 px-4 py-3 text-xs font-semibold text-slate-300">
                  <div>ログイン日時</div>
                  <div>IPアドレス</div>
                  <div>ブラウザ</div>
                </div>

                {loginHistory.length === 0 ? (
                  <div className="px-4 py-8 text-center text-sm text-slate-500">
                    ログイン履歴はまだありません
                  </div>
                ) : (
                  <div className="max-h-[420px] overflow-y-auto">
                    {loginHistory.map((item, index) => (
                      <div
                        key={`${item.loggedAt}-${index}`}
                        className="grid grid-cols-[1.1fr_0.9fr_0.8fr] border-b border-white/5 px-4 py-3 text-sm text-slate-200 last:border-b-0 hover:bg-white/5"
                      >
                        <div className="truncate" title={item.loggedAt}>
                          {formatMasterDataLoginDate(item.loggedAt)}
                        </div>

                        <div className="truncate" title={item.ipAddress}>
                          {item.ipAddress}
                        </div>

                        <div className="truncate" title={item.browser}>
                          {item.browser}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}

          {isMasterDataManagerRole(loginUser?.role) &&
            settingsTab === "permissionManagement" && (
              <div className="grid grid-cols-1 gap-4 xl:grid-cols-[320px_minmax(0,1fr)]">
                <div className="rounded-2xl border border-white/10 bg-[#0f172a] p-5">
                  <div className="mb-4 flex items-center justify-between gap-3">
                    <div>
                      <div className="text-sm font-semibold text-slate-100">
                        {loginUser?.role === "スーパー管理者"
                          ? "管理者・従業員アカウント"
                          : "従業員アカウント"}
                      </div>
                      <div className="mt-1 text-xs text-slate-400">
                        {loginUser?.role === "スーパー管理者"
                          ? "全ての所属の管理者・従業員を表示します"
                          : "同じ所属の従業員のみ表示します"}
                      </div>
                    </div>

                    <button
                      type="button"
                      onClick={() => void fetchPermissionEmployees()}
                      disabled={permissionLoading}
                      className="h-8 rounded-lg border border-white/10 bg-white/5 px-3 text-xs font-semibold text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                    >
                      更新
                    </button>
                  </div>

                  {permissionLoading ? null : permissionEmployees.length === 0 ? (
                    <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-8 text-center text-sm text-slate-400">
                      表示できるアカウントがありません
                    </div>
                  ) : (
                    <div className="space-y-2">
                      {permissionEmployees.map((employee) => {
                        const active = employee.id === selectedPermissionEmployeeId;

                        return (
                          <button
                            key={employee.id}
                            type="button"
                            onClick={() => {
                              setSelectedPermissionEmployeeId(employee.id);
                              setPermissionError("");
                              setPermissionMessage("");
                              setPermissionListScopeOpen(false);
                              setPermissionListScopeSearchOpen(false);
                              setOpenFilterKey(null);
                              setOpenAdvancedFilterKey(null);
                            }}
                            className={`w-full rounded-xl border px-4 py-3 text-left transition ${
                              active
                                ? "border-sky-400/40 bg-sky-500/20 text-sky-100"
                                : "border-white/10 bg-white/5 text-slate-200 hover:bg-white/10"
                            }`}
                          >
                            <div className="truncate text-sm font-semibold">
                              {employee.name}
                            </div>
                            <div className="mt-1 truncate text-xs text-slate-400">
                              ID：{employee.id}
                            </div>
                            <div className="mt-1 truncate text-xs text-slate-500">
                              所属：{employee.organization || "-"}
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  )}
                </div>

                <div className="rounded-2xl border border-white/10 bg-[#0f172a] p-5">
                  {!selectedPermissionEmployee ? (
                    <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-10 text-center text-sm text-slate-400">
                      左側からアカウントを選択してください
                    </div>
                  ) : (
                    <div className="space-y-5">
                      <div className="flex flex-col gap-3 border-b border-white/10 pb-4 md:flex-row md:items-start md:justify-between">
                        <div>
                          <div className="text-sm font-semibold text-slate-100">
                            {selectedPermissionEmployee.name} の権限
                          </div>
                          <div className="mt-1 text-xs text-slate-400">
                            チェックを外した項目は、このアカウントでは使えない設定にします
                          </div>
                        </div>

                        <div className="flex min-w-[190px] shrink-0 flex-col items-end gap-2">
                          <button
                            type="button"
                            onClick={handleSaveEmployeePermissions}
                            disabled={permissionSaving}
                            className="inline-flex h-10 w-fit items-center justify-center rounded-xl bg-sky-500 px-4 text-sm font-semibold text-white transition hover:bg-sky-400 disabled:opacity-50"
                          >
                            保存
                          </button>

                          <div className="flex min-w-[180px] shrink-0 items-center justify-end gap-2 self-end">
                            <button
                              type="button"
                              onClick={() =>
                                handleSetAllEmployeePermissions(
                                  selectedPermissionEmployee.id,
                                  true
                                )
                              }
                              className="inline-flex h-8 min-w-[76px] items-center justify-center whitespace-nowrap rounded-lg border border-white/10 bg-white/5 px-3 text-center text-xs font-semibold text-slate-200 transition hover:bg-white/10"
                            >
                              全選択
                            </button>

                            <button
                              type="button"
                              onClick={() =>
                                handleSetAllEmployeePermissions(
                                  selectedPermissionEmployee.id,
                                  false
                                )
                              }
                              className="inline-flex h-8 min-w-[92px] items-center justify-center whitespace-nowrap rounded-lg border border-white/10 bg-white/5 px-3 text-center text-xs font-semibold text-slate-200 transition hover:bg-white/10"
                            >
                              選択解除
                            </button>
                          </div>
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-[#0b1220]/80 p-4">
                        <div className="mb-3 flex items-center justify-between gap-3">
                          <div>
                            <div className="text-sm font-semibold text-slate-100">
                              リストデータ
                            </div>
                            <div className="mt-1 text-xs text-slate-400">
                              このアカウントが操作できるリスト範囲を設定します
                            </div>
                          </div>

                          <button
                            type="button"
                            onClick={openPermissionListScopeModal}
                            className="inline-flex h-8 items-center justify-center whitespace-nowrap rounded-lg border border-white/10 bg-white/5 px-3 text-xs font-semibold text-slate-200 transition hover:bg-white/10"
                          >
                            リストの絞り込み範囲
                          </button>
                        </div>

                        <div className="space-y-2">
                          {LIST_DATA_PERMISSION_ITEMS.map((permission) => (
                            <label
                              key={permission.key}
                              className="flex cursor-pointer items-center gap-3 rounded-lg px-2 py-2 text-sm text-slate-200 transition hover:bg-white/5"
                            >
                              <input
                                type="checkbox"
                                checked={
                                  selectedPermissionEmployee.permissions[
                                    permission.key
                                  ] !== false
                                }
                                onChange={() =>
                                  handleToggleEmployeePermission(
                                    selectedPermissionEmployee.id,
                                    permission.key
                                  )
                                }
                                className="h-4 w-4 accent-sky-500"
                              />
                              <span className="min-w-0 flex-1">
                                {permission.label}
                              </span>
                            </label>
                          ))}
                        </div>

                        <div className="mt-3 flex flex-wrap items-center justify-between gap-2 rounded-lg border border-white/10 bg-white/5 px-3 py-2">
                          <div className="text-xs text-slate-400">
                            設定状況：
                            {selectedPermissionEmployee.allowedFilters?.listScopeFilters
                              ? "絞り込み範囲あり"
                              : "全リストを表示"}
                          </div>

                          {Boolean(selectedPermissionEmployee.allowedFilters?.listScopeFilters) && (
                            <button
                              type="button"
                              onClick={handleClearPermissionListScope}
                              className="inline-flex h-7 items-center justify-center rounded-lg border border-white/10 bg-white/5 px-3 text-xs font-semibold text-slate-200 transition hover:bg-white/10"
                            >
                              範囲解除
                            </button>
                          )}
                        </div>
                      </div>

                      <div className="grid grid-cols-1 gap-4 2xl:grid-cols-2">
                        {MASTER_DATA_PERMISSION_GROUPS.map((group) => (
                          <div
                            key={group.title}
                            className="rounded-xl border border-white/10 bg-[#0b1220]/80 p-4"
                          >
                            <div className="mb-3 flex items-center justify-between gap-3">
                              <div className="text-sm font-semibold text-slate-100">
                                {group.title}
                              </div>

                              <div className="flex shrink-0 items-center gap-2">
                                <button
                                  type="button"
                                  onClick={() =>
                                    handleSetEmployeePermissionGroup(
                                      selectedPermissionEmployee.id,
                                      group.items.map((item) => item.key),
                                      true
                                    )
                                  }
                                  className="inline-flex h-7 min-w-[68px] items-center justify-center whitespace-nowrap rounded-lg border border-white/10 bg-white/5 px-2 text-center text-[10px] font-semibold text-slate-200 transition hover:bg-white/10"
                                >
                                  全選択
                                </button>

                                <button
                                  type="button"
                                  onClick={() =>
                                    handleSetEmployeePermissionGroup(
                                      selectedPermissionEmployee.id,
                                      group.items.map((item) => item.key),
                                      false
                                    )
                                  }
                                  className="inline-flex h-7 min-w-[68px] items-center justify-center whitespace-nowrap rounded-lg border border-white/10 bg-white/5 px-2 text-center text-[10px] font-semibold text-slate-200 transition hover:bg-white/10"
                                >
                                  選択解除
                                </button>
                              </div>
                            </div>

                            <div className="space-y-2">
                              {group.items.map((permission) => (
                                <label
                                  key={permission.key}
                                  className="flex cursor-pointer items-center gap-3 rounded-lg px-2 py-2 text-sm text-slate-200 transition hover:bg-white/5"
                                >
                                  <input
                                    type="checkbox"
                                    checked={
                                      selectedPermissionEmployee.permissions[
                                        permission.key
                                      ] !== false
                                    }
                                    onChange={() =>
                                      handleToggleEmployeePermission(
                                        selectedPermissionEmployee.id,
                                        permission.key
                                      )
                                    }
                                    className="h-4 w-4 accent-sky-500"
                                  />
                                  <span className="min-w-0 flex-1">
                                    {permission.label}
                                  </span>
                                </label>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            )}
        </div>
      );
    }

    if (openSidebarPanel === "theme") {
      const themeOptions: {
        key: "system" | "light" | "dark";
        label: string;
        description: string;
        icon: string;
      }[] = [
        {
          key: "system",
          label: "システム",
          description: "PCやブラウザの設定に合わせて自動で切り替えます",
          icon: "🖥",
        },
        {
          key: "light",
          label: "ライト",
          description: "明るい画面で表示します",
          icon: "☀",
        },
        {
          key: "dark",
          label: "ダーク",
          description: "暗い画面で表示します",
          icon: "☾",
        },
      ];

      return (
        <div className="grid grid-cols-1 gap-3">
          {themeOptions.map((option) => {
            const active = themeMode === option.key;

            const themeCardClass =
              option.key === "light"
                ? active
                  ? "border-amber-400/70 bg-gradient-to-br from-amber-200 via-yellow-100 to-orange-200 text-[#78350f] shadow-[0_0_28px_rgba(245,158,11,0.22)] hover:border-amber-400/80 hover:from-amber-100 hover:to-orange-100"
                  : "border-amber-300/40 bg-gradient-to-br from-amber-400/22 via-yellow-300/14 to-orange-300/12 text-[#92400e] hover:border-amber-300/70 hover:from-amber-400/30 hover:to-orange-300/20"
                : option.key === "dark"
                ? active
                  ? "border-slate-400/60 bg-gradient-to-br from-slate-950 via-slate-900 to-indigo-950 text-[#f8fafc] shadow-[0_0_28px_rgba(15,23,42,0.32)] hover:border-slate-300/70 hover:from-slate-900 hover:to-indigo-900"
                  : "border-slate-500/40 bg-gradient-to-br from-slate-950/90 via-slate-900/85 to-indigo-950/70 text-[#f8fafc] hover:border-slate-300/60 hover:from-slate-900 hover:to-indigo-900"
                : active
                ? "border-sky-400/40 bg-gradient-to-br from-sky-500/20 via-[#0f172a] to-indigo-500/10 text-sky-100 shadow-[0_0_28px_rgba(56,189,248,0.16)] hover:bg-sky-500/25"
                : "border-white/10 bg-gradient-to-br from-white/10 via-[#0f172a] to-[#0b1220] text-slate-200 hover:border-sky-300/30 hover:bg-white/10";

            const themeIconClass =
              option.key === "light"
                ? active
                  ? "border-amber-500/40 bg-amber-100/90 text-amber-700"
                  : "border-amber-300/35 bg-amber-400/15 text-amber-100 group-hover:border-amber-300/55 group-hover:bg-amber-400/22"
                : option.key === "dark"
                ? active
                  ? "border-slate-300/40 bg-slate-800 text-slate-100"
                  : "border-slate-400/30 bg-slate-800/60 text-slate-100 group-hover:border-slate-300/50 group-hover:bg-slate-700/80"
                : active
                ? "border-sky-300/30 bg-sky-400/20 text-sky-100"
                : "border-white/10 bg-white/5 text-slate-100 group-hover:border-sky-300/30 group-hover:bg-sky-400/10";

            const themeLabelClass =
              option.key === "light" || option.key === "dark"
                ? "text-slate-100"
                : "text-slate-100";

            const themeDescriptionClass =
              option.key === "light" || option.key === "dark"
                ? "text-slate-100"
                : "text-slate-400";

            const themeBadgeClass =
              option.key === "light"
                ? "border-amber-600/30 bg-amber-100/80 text-amber-800"
                : option.key === "dark"
                ? "border-slate-300/35 bg-slate-700/80 text-slate-100"
                : "border-sky-300/40 bg-sky-400/15 text-sky-100";

            return (
              <button
                key={option.key}
                type="button"
                onClick={() => {
                  setThemeMode(option.key);
                }}
                className={`group rounded-2xl border p-4 text-left transition ${themeCardClass} ${
                  option.key === "light"
                    ? "theme-light-mode-button"
                    : option.key === "dark"
                    ? "theme-dark-mode-button"
                    : ""
                }`}
              >
                <div className="flex items-center gap-3">
                  <div
                    className={`inline-flex h-12 w-12 shrink-0 items-center justify-center rounded-2xl border text-xl ${themeIconClass} ${
                      option.key === "light"
                        ? "theme-light-mode-icon"
                        : option.key === "dark"
                        ? "theme-dark-mode-icon"
                        : ""
                    }`}
                  >
                    {option.icon}
                  </div>

                  <div className="min-w-0 flex-1">
                    <div className="flex items-center justify-between gap-3">
                      <div className={`text-sm font-semibold ${themeLabelClass}`}>
                        {option.label}
                      </div>

                      {active && (
                        <span className={`shrink-0 rounded-full border px-4 py-1.5 text-xs font-bold ${themeBadgeClass}`}>
                          選択中
                        </span>
                      )}
                    </div>

                    <div className={`mt-1 text-xs leading-5 ${themeDescriptionClass}`}>
                      {option.description}
                    </div>
                  </div>
                </div>
              </button>
            );
          })}
        </div>
      );
    }

    return null;
  };

  const clearMynaviStatusPolling = () => {
    if (mynaviStatusTimerRef.current !== null) {
      window.clearInterval(mynaviStatusTimerRef.current);
      mynaviStatusTimerRef.current = null;
    }
  };

  const applyMynaviStatus = (data: ApiResponse) => {
    setMynaviJobId(data.jobId ?? null);
    setMynaviJobStatus((data.jobStatus as MynaviJobStatus) ?? "idle");
    setMynaviJobMode((data.mynaviJobMode as MynaviJobMode) ?? "count_pages");
    setMynaviPhase(
      (data.mynaviPhase as
        | "idle"
        | "count_pages"
        | "collect_urls"
        | "scrape_details"
        | "completed"
        | "error") ?? "idle"
    );

    setMynaviTotalPages(
      data.mynaviTotalPages ?? data.mynaviDetectedTotalPages ?? 0
    );
    setMynaviDetectedTotalPages(data.mynaviDetectedTotalPages ?? 0);
    setMynaviTotalUrls(data.mynaviTotalUrls ?? 0);
    setMynaviProcessedCount(data.mynaviProcessedCount ?? 0);
    setMynaviSuccessCount(data.mynaviSuccessCount ?? 0);
    setMynaviFailedCount(data.mynaviFailedCount ?? 0);
    setMynaviCurrentPageNumber(data.mynaviCurrentPageNumber ?? 0);
    setMynaviCsvFileName(data.mynaviCsvFileName ?? "");
    setMynaviCurrentField(data.mynaviCurrentField ?? null);
    setMynaviCurrentCompany(data.mynaviCurrentCompany ?? null);
    setMynaviCurrentCompanyIndex(data.mynaviCurrentCompanyIndex ?? 0);
  };

  const startMynaviStatusPolling = (jobId: string) => {
    clearMynaviStatusPolling();
    mynaviUserCanceledRef.current = false;

    mynaviStatusTimerRef.current = window.setInterval(async () => {
      try {
        const res = await fetch("/api/master_data/mynavi", {
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
          throw new Error(data.error || "マイナビ新卒の進捗取得に失敗しました");
        }

        if (mynaviUserCanceledRef.current) {
          clearMynaviStatusPolling();
          return;
        }

        applyMynaviStatus(data);

        if (mynaviUserCanceledRef.current) {
          clearMynaviStatusPolling();
          return;
        }

        if (data.jobStatus === "paused") {
          clearMynaviStatusPolling();
          setMynaviLoading(false);
          setMynaviProgressOpen(false);
          setMynaviResultOpen(true);
          setMynaviMessage("マイナビ新卒を中断しました");
          setMynaviError("");
          return;
        }

        if (data.jobStatus === "completed") {
          clearMynaviStatusPolling();
          setMynaviLoading(false);

          if (data.mynaviJobMode === "count_pages") {
            setMynaviProgressOpen(false);
            setMynaviTotalPages(
              data.mynaviTotalPages ?? data.mynaviDetectedTotalPages ?? 0
            );
            setSelectedMynaviPageCount("all");
            setMynaviPageCountOpen(true);
            return;
          }

          setMynaviProgressOpen(false);
          setMynaviResultOpen(true);
          setMynaviMessage("マイナビ新卒の取得が完了しました");
          setMynaviError("");
          return;
        }

        if (data.jobStatus === "error") {
          clearMynaviStatusPolling();
          setMynaviLoading(false);
          setMynaviProgressOpen(false);
          setMynaviError(data.error || "マイナビ新卒の取得でエラーが発生しました");
        }
      } catch (e) {
        clearMynaviStatusPolling();
        setMynaviLoading(false);
        setMynaviProgressOpen(false);
        setMynaviError(
          e instanceof Error
            ? e.message
            : "マイナビ新卒の進捗取得でエラーが発生しました"
        );
      }
    }, 1200);
  };

  const handleOpenListAdd = () => {
    setMynaviMessage("");
    setMynaviError("");
    setListAddSourceOpen(true);
  };

  const handleOpenMynaviYear = () => {
    setListAddSourceOpen(false);
    setMynaviYearOpen(true);
  };

  const handleConfirmMynaviGradYear = async () => {
    mynaviUserCanceledRef.current = false;

    setMynaviLoading(true);
    setMynaviError("");
    setMynaviMessage("");
    setMynaviPageCountOpen(false);
    setMynaviResultOpen(false);
    setMynaviProgressOpen(true);
    setMynaviJobId(null);
    setMynaviJobStatus("running");
    setMynaviJobMode("count_pages");
    setMynaviPhase("count_pages");
    setMynaviDetectedTotalPages(0);
    setMynaviCurrentField("ページ数を確認しています");
    setMynaviCurrentCompany(null);
    setMynaviCurrentCompanyIndex(0);
    setMynaviTotalUrls(0);
    setMynaviProcessedCount(0);
    setMynaviSuccessCount(0);
    setMynaviFailedCount(0);
    setMynaviCurrentPageNumber(1);

    try {
      const res = await fetch("/api/master_data/mynavi", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "start_count_job",
          gradYear: selectedMynaviGradYear,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "ページ数の取得開始に失敗しました");
      }

      if (mynaviUserCanceledRef.current) {
        if (data.jobId) {
          void fetch("/api/master_data/mynavi", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              action: "cancel_job",
              jobId: data.jobId,
            }),
          });
        }

        return;
      }

      applyMynaviStatus(data);
      setMynaviYearOpen(false);

      if (data.jobId) {
        startMynaviStatusPolling(data.jobId);
      }
    } catch (e) {
      setMynaviLoading(false);
      setMynaviProgressOpen(false);
      setMynaviJobStatus("error");
      setMynaviError(
        e instanceof Error ? e.message : "ページ数取得でエラーが発生しました"
      );
    }
  };

  const handleStartMynaviJob = async () => {
    mynaviUserCanceledRef.current = false;

    setMynaviLoading(true);
    setMynaviError("");
    setMynaviMessage("");
    setMynaviResultOpen(false);
    setMynaviProgressOpen(true);
    setMynaviJobId(null);
    setMynaviJobStatus("running");
    setMynaviJobMode("scrape");
    setMynaviPhase("idle");
    setMynaviCurrentField("取得開始準備中");
    setMynaviCurrentCompany(null);
    setMynaviCurrentCompanyIndex(0);
    setMynaviTotalUrls(0);
    setMynaviProcessedCount(0);
    setMynaviSuccessCount(0);
    setMynaviFailedCount(0);
    setMynaviCurrentPageNumber(0);
    setMynaviCsvFileName("");

    try {
      const res = await fetch("/api/master_data/mynavi", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "start_job",
          gradYear: selectedMynaviGradYear,
          pageCount:
            selectedMynaviPageCount === "all"
              ? "all"
              : Number(selectedMynaviPageCount),
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "マイナビ新卒の取得開始に失敗しました");
      }

      if (mynaviUserCanceledRef.current) {
        if (data.jobId) {
          void fetch("/api/master_data/mynavi", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              action: "cancel_job",
              jobId: data.jobId,
            }),
          });
        }

        return;
      }

      applyMynaviStatus(data);
      setMynaviPageCountOpen(false);

      if (data.jobId) {
        startMynaviStatusPolling(data.jobId);
      }
    } catch (e) {
      setMynaviLoading(false);
      setMynaviProgressOpen(false);
      setMynaviJobStatus("error");
      setMynaviError(
        e instanceof Error ? e.message : "マイナビ新卒の取得開始でエラーが発生しました"
      );
    }
  };

  const handleDownloadMynaviCsv = async () => {
    if (!mynaviJobId) return;

    try {
      const res = await fetch(
        `/api/master_data/mynavi?action=download_csv&jobId=${encodeURIComponent(
          mynaviJobId
        )}`,
        {
          method: "GET",
          cache: "no-store",
        }
      );

      if (!res.ok) {
        const data = await readApiResponse(res);
        throw new Error(data.error || "CSVダウンロードに失敗しました");
      }

      const blob = await res.blob();
      const downloadUrl = window.URL.createObjectURL(blob);
      const a = document.createElement("a");

      a.href = downloadUrl;
      a.download =
        mynaviCsvFileName ||
        `mynavi_${selectedMynaviGradYear}_result.csv`;

      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(downloadUrl);
    } catch (e) {
      setMynaviError(
        e instanceof Error ? e.message : "CSVダウンロードでエラーが発生しました"
      );
    }
  };

  const handlePauseMynaviJob = async () => {
    if (!mynaviJobId) return;

    try {
      const res = await fetch("/api/master_data/mynavi", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "pause_job",
          jobId: mynaviJobId,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "中断に失敗しました");
      }

      setMynaviMessage("中断指示を受け付けました");
      setMynaviError("");
    } catch (e) {
      setMynaviError(
        e instanceof Error ? e.message : "中断処理でエラーが発生しました"
      );
    }
  };

  const handleCancelMynaviJob = async () => {
    mynaviUserCanceledRef.current = true;

    const targetJobId = mynaviJobId;

    clearMynaviStatusPolling();
    setMynaviLoading(false);
    setMynaviProgressOpen(false);
    setMynaviPageCountOpen(false);
    setMynaviResultOpen(false);
    setMynaviJobId(null);
    setMynaviJobStatus("idle");
    setMynaviPhase("idle");
    setMynaviCurrentField(null);
    setMynaviCurrentCompany(null);
    setMynaviCurrentCompanyIndex(0);

    if (!targetJobId) {
      setMynaviMessage("マイナビ新卒を中止しました");
      setMynaviError("");
      return;
    }

    try {
      const res = await fetch("/api/master_data/mynavi", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "cancel_job",
          jobId: targetJobId,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "中止に失敗しました");
      }

      setMynaviMessage("マイナビ新卒を中止しました");
      setMynaviError("");
    } catch (e) {
      setMynaviError(
        e instanceof Error ? e.message : "中止処理でエラーが発生しました"
      );
    }
  };

  const handleResumeMynaviJob = async () => {
    if (!mynaviJobId) return;

    setMynaviLoading(true);
    setMynaviError("");
    setMynaviMessage("");
    setMynaviResultOpen(false);
    setMynaviProgressOpen(true);

    try {
      const res = await fetch("/api/master_data/mynavi", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "resume_job",
          jobId: mynaviJobId,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "再開に失敗しました");
      }

      applyMynaviStatus(data);
      startMynaviStatusPolling(mynaviJobId);
    } catch (e) {
      setMynaviLoading(false);
      setMynaviProgressOpen(false);
      setMynaviError(
        e instanceof Error ? e.message : "再開処理でエラーが発生しました"
      );
    }
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

const getCsvFileNameFromContentDisposition = (contentDisposition: string | null) => {
  if (!contentDisposition) {
    return null;
  }

  const utf8Match = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i);

  if (utf8Match?.[1]) {
    try {
      return decodeURIComponent(utf8Match[1].replace(/^"|"$/g, ""));
    } catch {
      return utf8Match[1].replace(/^"|"$/g, "");
    }
  }

  const filenameMatch =
    contentDisposition.match(/filename="([^"]+)"/i) ??
    contentDisposition.match(/filename=([^;]+)/i);

  return filenameMatch?.[1]?.trim().replace(/^"|"$/g, "") ?? null;
};

const downloadBlobByAnchor = (blob: Blob, fileName: string) => {
  const downloadUrl = window.URL.createObjectURL(blob);
  const a = document.createElement("a");

  a.href = downloadUrl;
  a.download = fileName;
  a.style.display = "none";

  document.body.appendChild(a);
  a.click();
  a.remove();

  window.setTimeout(() => {
    window.URL.revokeObjectURL(downloadUrl);
  }, 1000);
};

const writeResponseBodyToFile = async (
  res: Response,
  fileHandle: {
    createWritable: () => Promise<{
      write: (data: Blob | Uint8Array | string) => Promise<void>;
      close: () => Promise<void>;
    }>;
  }
) => {
  const writable = await fileHandle.createWritable();

  try {
    if (!res.body) {
      await writable.write(await res.blob());
      return;
    }

    const reader = res.body.getReader();

    try {
      while (true) {
        const { done, value } = await reader.read();

        if (done) {
          break;
        }

        if (value) {
          await writable.write(value);
        }
      }
    } finally {
      reader.releaseLock();
    }
  } finally {
    await writable.close();
  }
};

const handleExport = async (shouldDeduplicate: boolean) => {
  setImportError("");
  setImportMessage("");
  setExportConfirmOpen(false);
  setExportDuplicateConfirmOpen(false);
  setExporting(true);

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
            write: (data: Blob | Uint8Array | string) => Promise<void>;
            close: () => Promise<void>;
          }>;
        }>;
      }
    ).showSaveFilePicker;

    let fileHandle: {
      createWritable: () => Promise<{
        write: (data: Blob | Uint8Array | string) => Promise<void>;
        close: () => Promise<void>;
      }>;
    } | null = null;

    if (showSaveFilePicker) {
      fileHandle = await showSaveFilePicker({
        suggestedName: "master_data.csv",
        types: [
          {
            description: "CSVファイル",
            accept: {
              "text/csv": [".csv"],
            },
          },
        ],
      });
    }

    const body: Record<string, unknown> = {
      exportScope: exportMode,
      dedupeByCompany: shouldDeduplicate ? "1" : "0",
    };

    if (exportMode === "filtered") {
      body.filterModels = buildRequestFilterModels(appliedColumnStates);
      body.advancedFilters = buildRequestAdvancedFilters(
        appliedAdvancedFilters,
        advancedValueOptions
      );

      const sortColumn = COLUMN_DEFS.find(
        (column) => appliedColumnStates[column.key].sortDirection !== ""
      );

      if (sortColumn) {
        body.sortKey = sortColumn.key;
        body.sortDirection =
          appliedColumnStates[sortColumn.key].sortDirection;
      }
    }

    const res = await fetch("/api/master_data/export", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
      cache: "no-store",
    });

    if (!res.ok) {
      const data = await readApiResponse(res);
      throw new Error(data.error || "CSV抽出に失敗しました");
    }

    const fileName =
      getCsvFileNameFromContentDisposition(
        res.headers.get("Content-Disposition")
      ) ?? "master_data.csv";

    if (fileHandle) {
      await writeResponseBodyToFile(res, fileHandle);
    } else {
      const blob = await res.blob();
      downloadBlobByAnchor(blob, fileName);
    }

    setImportMessage("CSVを保存しました");
  } catch (e) {
    if (e instanceof DOMException && e.name === "AbortError") {
      return;
    }

    setImportError(
      e instanceof Error ? e.message : "CSV抽出に失敗しました"
    );
  } finally {
    setExporting(false);
  }
};

const handleDownloadTemplate = async () => {
  setImportError("");
  setImportMessage("");
  setCsvTemplateSaving(true);

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
      throw new Error("この環境では保存場所の選択に対応していません");
    }

    const fileHandle = await showSaveFilePicker({
      suggestedName: "master_data_template.csv",
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
      e instanceof Error ? e.message : "CSVテンプレート保存に失敗しました"
    );
  } finally {
    setCsvTemplateSaving(false);
  }
};

const openPreviewCsvScope = (source: "crawl" | "item_inspection") => {
  setPreviewCsvSource(source);
  setPreviewCsvMode("all");
  setPreviewCsvConfirmOpen(false);
  setPreviewCsvScopeOpen(true);
};

const handlePreviewCsvExport = async () => {
  if (!previewCsvSource) {
    return;
  }

  setPreviewCsvConfirmOpen(false);
  setPreviewCsvExporting(true);

  try {
    if (previewCsvSource === "crawl") {
      if (!crawlJobId) {
        throw new Error("クローリングジョブが見つかりません");
      }

      const crawlCsvFileName = createPreviewCsvFileName(
        "crawl",
        previewCsvMode
      );

      const crawlCsvFileHandle = await openPreviewCsvSaveFile(
        crawlCsvFileName
      );

      const candidateRows = await fetchAllCrawlPreviewRowsForCsv(
        crawlJobId,
        "candidate"
      );

      const candidateKeys = collectCrawlPreviewCandidateKeys(candidateRows);

      const exportRows =
        previewCsvMode === "candidate"
          ? buildCrawlPreviewCsvRows(candidateRows, "candidate", candidateKeys)
          : [
              ...buildCrawlPreviewCsvRows(
                candidateRows,
                "candidate",
                candidateKeys
              ),
              ...buildCrawlPreviewCsvRows(
                await fetchAllCrawlPreviewRowsForCsv(crawlJobId, "excluded"),
                "excluded",
                candidateKeys
              ),
            ];

      if (exportRows.length === 0) {
        throw new Error("CSV抽出対象がありません");
      }

      const csvText = buildPreviewCsvText(
        buildPreviewCsvHeaders(candidateKeys),
        exportRows
      );

      await savePreviewCsvText(
        csvText,
        crawlCsvFileName,
        crawlCsvFileHandle
      );

      setPreviewCsvSource(null);
      setCrawlMessage(
        previewCsvMode === "all"
          ? "全てのリストをCSV保存しました"
          : "候補のみリストをCSV保存しました"
      );
      return;
    }

    if (!itemInspectionJobId) {
      throw new Error("項目精査ジョブが見つかりません");
    }

    const itemInspectionCsvFileName = createPreviewCsvFileName(
      "item_inspection",
      previewCsvMode
    );

    const itemInspectionCsvFileHandle = await openPreviewCsvSaveFile(
      itemInspectionCsvFileName
    );

    const candidateRows = await fetchAllItemInspectionPreviewRowsForCsv(
      itemInspectionJobId,
      "candidate"
    );

    const candidateKeys =
      collectItemInspectionPreviewCandidateKeys(candidateRows);

    const exportRows =
      previewCsvMode === "candidate"
        ? buildItemInspectionPreviewCsvRows(
            candidateRows,
            "candidate",
            candidateKeys
          )
        : [
            ...buildItemInspectionPreviewCsvRows(
              candidateRows,
              "candidate",
              candidateKeys
            ),
            ...buildItemInspectionPreviewCsvRows(
              await fetchAllItemInspectionPreviewRowsForCsv(
                itemInspectionJobId,
                "excluded"
              ),
              "excluded",
              candidateKeys
            ),
          ];

    if (exportRows.length === 0) {
      throw new Error("CSV抽出対象がありません");
    }

    const csvText = buildPreviewCsvText(
      buildPreviewCsvHeaders(candidateKeys),
      exportRows
    );

    await savePreviewCsvText(
      csvText,
      itemInspectionCsvFileName,
      itemInspectionCsvFileHandle
    );

    setPreviewCsvSource(null);
    setItemInspectionMessage(
      previewCsvMode === "all"
        ? "全てのリストをCSV保存しました"
        : "候補のみリストをCSV保存しました"
    );
  } catch (e) {
    if (e instanceof DOMException && e.name === "AbortError") {
      return;
    }

    const message = e instanceof Error ? e.message : "CSV抽出に失敗しました";

    if (previewCsvSource === "crawl") {
      setCrawlError(message);
    } else {
      setItemInspectionError(message);
    }
  } finally {
    setPreviewCsvExporting(false);
  }
};

  const PREVIEW_CSV_BASE_FIELDS: Array<{
    key: keyof Row;
    label: string;
  }> = [
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
    { key: "permit_number", label: "許可番号" },
    { key: "memo", label: "メモ" },
  ];

  const ITEM_INSPECTION_FIELD_LABEL_TO_KEY: Partial<
    Record<string, keyof Row>
  > = {
    代表者名: "representative_name",
  };

  const escapePreviewCsvValue = (value: unknown) => {
    const text = value == null ? "" : String(value);
    return `"${text.replace(/"/g, `""`)}"`;
  };

  const buildPreviewCsvText = (
    headers: string[],
    rows: Array<Record<string, unknown>>
  ) => {
    const lines = [
      headers.map((header) => escapePreviewCsvValue(header)).join(","),
      ...rows.map((row) =>
        headers.map((header) => escapePreviewCsvValue(row[header] ?? "")).join(",")
      ),
    ];

    return "\uFEFF" + lines.join("\r\n");
  };

  const createPreviewCsvFileName = (
    source: "crawl" | "item_inspection",
    mode: "all" | "candidate"
  ) => {
    const now = new Date();
    const yyyy = now.getFullYear();
    const mm = String(now.getMonth() + 1).padStart(2, "0");
    const dd = String(now.getDate()).padStart(2, "0");
    const hh = String(now.getHours()).padStart(2, "0");
    const mi = String(now.getMinutes()).padStart(2, "0");
    const ss = String(now.getSeconds()).padStart(2, "0");

    return `master_data_${source}_${mode}_${yyyy}${mm}${dd}_${hh}${mi}${ss}.csv`;
  };

  const openPreviewCsvSaveFile = async (fileName: string) => {
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
            write: (data: Blob | Uint8Array | string) => Promise<void>;
            close: () => Promise<void>;
          }>;
        }>;
      }
    ).showSaveFilePicker;

    if (!showSaveFilePicker) {
      throw new Error("この環境では保存場所の選択に対応していません");
    }

    return await showSaveFilePicker({
      suggestedName: fileName,
      types: [
        {
          description: "CSVファイル",
          accept: {
            "text/csv": [".csv"],
          },
        },
      ],
    });
  };

  const savePreviewCsvText = async (
    csvText: string,
    fileName: string,
    fileHandle?: Awaited<ReturnType<typeof openPreviewCsvSaveFile>> | null
  ) => {
    const csvBlob = new Blob([csvText], {
      type: "text/csv;charset=utf-8;",
    });

    if (!fileHandle) {
      downloadBlobByAnchor(csvBlob, fileName);
      return;
    }

    const writable = await fileHandle.createWritable();
    await writable.write(csvBlob);
    await writable.close();
  };

  const buildPreviewCsvHeaders = (candidateKeys: Array<keyof Row>) => {
    const candidateKeySet = new Set<keyof Row>(candidateKeys);
    const headers = ["区分"];

    PREVIEW_CSV_BASE_FIELDS.forEach((field) => {
      if (candidateKeySet.has(field.key)) {
        headers.push(`${field.label}（変更前）`);
        headers.push(`${field.label}（候補一覧）`);
      } else {
        headers.push(field.label);
      }
    });

    return headers;
  };

  const createBasePreviewCsvRow = (
    sourceRow: Row | null,
    sectionLabel: string
  ) => {
    const csvRow: Record<string, unknown> = {
      区分: sectionLabel,
    };

    PREVIEW_CSV_BASE_FIELDS.forEach((field) => {
      csvRow[field.label] = sourceRow?.[field.key] ?? "";
    });

    return csvRow;
  };

  const fetchAllCrawlPreviewRowsForCsv = async (
    jobId: string,
    previewTab: "candidate" | "excluded"
  ) => {
    const allRows: CrawlPreviewRow[] = [];
    let page = 1;
    let totalPages = 1;

    while (page <= totalPages) {
      const res = await fetch("/api/master_data/crawl", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "get_job_status",
          jobId,
          previewTab,
          previewPage: page,
          previewPageSize: 100,
        }),
        cache: "no-store",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "クローリング結果確認の取得に失敗しました");
      }

      allRows.push(...(data.previewRows || []));

      const pageSize = Math.max(Number(data.previewPageSize ?? 100), 1);
      totalPages = Math.max(
        1,
        Math.ceil((data.previewTotal ?? 0) / pageSize)
      );

      page += 1;
    }

    return allRows;
  };

  const fetchAllItemInspectionPreviewRowsForCsv = async (
    jobId: string,
    previewTab: "candidate" | "excluded"
  ) => {
    if (previewTab === "candidate") {
      const res = await fetch("/api/master_data/item_inspection", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "get_job_status",
          jobId,
          previewTab: "candidate",
          previewPage: 1,
          previewPageSize: 100,
        }),
        cache: "no-store",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "項目精査結果確認の取得に失敗しました");
      }

      return data.inspectionPreviewChanges || [];
    }

    const allRows: ItemInspectionPreviewChange[] = [];
    let page = 1;
    let totalPages = 1;

    while (page <= totalPages) {
      const res = await fetch("/api/master_data/item_inspection", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "get_job_status",
          jobId,
          previewTab: "excluded",
          previewPage: page,
          previewPageSize: 100,
        }),
        cache: "no-store",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "項目精査結果確認の取得に失敗しました");
      }

      allRows.push(...(data.inspectionPreviewChanges || []));

      const pageSize = Math.max(Number(data.previewPageSize ?? 100), 1);
      totalPages = Math.max(
        1,
        Math.ceil((data.previewTotal ?? 0) / pageSize)
      );

      page += 1;
    }

    return allRows;
  };

  const collectCrawlPreviewCandidateKeys = (rows: CrawlPreviewRow[]) => {
    return Array.from(
      new Set(
        rows.flatMap((row) =>
          row.changes.map((change) => change.key as keyof Row)
        )
      )
    );
  };

  const collectItemInspectionPreviewCandidateKeys = (
    rows: ItemInspectionPreviewChange[]
  ) => {
    return Array.from(
      new Set(
        rows
          .map((row) => ITEM_INSPECTION_FIELD_LABEL_TO_KEY[row.fieldLabel])
          .filter((value): value is keyof Row => Boolean(value))
      )
    );
  };

  const buildCrawlPreviewCsvRows = (
    rows: CrawlPreviewRow[],
    previewTab: "candidate" | "excluded",
    candidateKeys: Array<keyof Row>
  ) => {
    const candidateKeySet = new Set<keyof Row>(candidateKeys);

    return rows.map((row) => {
      const csvRow = createBasePreviewCsvRow(
        row.source_row,
        previewTab === "candidate" ? "候補" : "候補外"
      );

      const changeMap = new Map<keyof Row, CrawlPreviewChange>(
        row.changes.map((change) => [change.key as keyof Row, change])
      );

      PREVIEW_CSV_BASE_FIELDS.forEach((field) => {
        if (!candidateKeySet.has(field.key)) {
          return;
        }

        const change = changeMap.get(field.key);
        delete csvRow[field.label];
        csvRow[`${field.label}（変更前）`] = change?.before ?? "";
        csvRow[`${field.label}（候補一覧）`] = change
          ? change.candidates.length > 0
            ? change.candidates.join(" / ")
            : change.after ?? ""
          : "";
      });

      return csvRow;
    });
  };

  const getItemInspectionCandidateText = (row: ItemInspectionPreviewChange) => {
    if (row.action === "delete") {
      return "(削除)";
    }

    if (row.action === "review") {
      return "(要確認)";
    }

    if (row.action === "none") {
      return "";
    }

    return row.afterValue ?? "";
  };

  const buildItemInspectionPreviewCsvRows = (
    rows: ItemInspectionPreviewChange[],
    previewTab: "candidate" | "excluded",
    candidateKeys: Array<keyof Row>
  ) => {
    const candidateKeySet = new Set<keyof Row>(candidateKeys);

    return rows.map((row) => {
      const csvRow = createBasePreviewCsvRow(
        row.source_row,
        previewTab === "candidate" ? "候補" : "候補外"
      );

      const fieldKey = ITEM_INSPECTION_FIELD_LABEL_TO_KEY[row.fieldLabel];

      PREVIEW_CSV_BASE_FIELDS.forEach((field) => {
        if (!candidateKeySet.has(field.key)) {
          return;
        }

        delete csvRow[field.label];

        if (fieldKey === field.key) {
          csvRow[`${field.label}（変更前）`] = row.beforeValue ?? "";
          csvRow[`${field.label}（候補一覧）`] =
            previewTab === "candidate"
              ? getItemInspectionCandidateText(row)
              : "";
        } else {
          csvRow[`${field.label}（変更前）`] = "";
          csvRow[`${field.label}（候補一覧）`] = "";
        }
      });

      return csvRow;
    });
  };

  const getDefaultCrawlPreviewValue = (change: CrawlPreviewChange) => {
    return change.after ?? change.candidates[0] ?? null;
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

  const fetchCrawlPreviewPage = async (
    jobId: string,
    nextPage: number,
    previewTab: "candidate" | "multiple" | "excluded" = crawlPreviewTab
  ) => {
    const requestId = ++crawlPreviewRequestIdRef.current;

    setCrawlPreviewLoading(true);
    setCrawlPreviewRows([]);
    setCrawlPreviewPage(nextPage);
    crawlPreviewScrollRef.current?.scrollTo({ top: 0 });

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
          previewTab,
        }),
        cache: "no-store",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "クローリング結果確認の取得に失敗しました");
      }

      if (crawlPreviewRequestIdRef.current !== requestId) {
        return;
      }

      applyCrawlStatus(data);
      setCrawlPreviewRows(data.previewRows || []);
      setCrawlPreviewTotalCount(data.previewTotal ?? 0);
      setCrawlPreviewPage(data.previewPage ?? nextPage);
    } catch (e) {
      if (crawlPreviewRequestIdRef.current !== requestId) {
        return;
      }

      setCrawlError(
        e instanceof Error
          ? e.message
          : "クローリング結果確認の取得でエラーが発生しました"
      );
    } finally {
      if (crawlPreviewRequestIdRef.current === requestId) {
        setCrawlPreviewLoading(false);
      }
    }
  };

  const handleCrawlPreviewPageChange = async (nextPage: number) => {
    if (!crawlJobId) return;
    if (nextPage < 1 || nextPage > crawlPreviewTotalPages) return;

    await fetchCrawlPreviewPage(crawlJobId, nextPage);
  };

  const handleChangeCrawlPreviewTab = async (
    nextTab: "candidate" | "multiple" | "excluded"
  ) => {
    if (!crawlJobId) return;
    setCrawlPreviewTab(nextTab);
    await fetchCrawlPreviewPage(crawlJobId, 1, nextTab);
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

    const nextElapsedMs = Math.max(
      Math.floor(crawlElapsedBaseMsRef.current + runningMs),
      0
    );

    const activeJobId = crawlJobId ?? loadActiveCrawlJobId();
    const isRunning =
      crawlJobStatus === "running" ||
      crawling ||
      crawlStartedAtRef.current !== null;

    setCrawlElapsedMs(nextElapsedMs);
    saveCrawlElapsedSnapshot(activeJobId, nextElapsedMs, isRunning);
  };

  const startCrawlElapsedTracking = (reset = false) => {
    const activeJobId = crawlJobId ?? loadActiveCrawlJobId();

    if (reset) {
      crawlElapsedBaseMsRef.current = 0;
      setCrawlElapsedMs(0);
      saveCrawlElapsedSnapshot(activeJobId, 0, true);
    } else {
      const savedElapsedMs = loadCrawlElapsedSnapshotMs(activeJobId);

      crawlElapsedBaseMsRef.current = Math.max(
        crawlElapsedBaseMsRef.current,
        savedElapsedMs,
        crawlElapsedMs,
        0
      );
    }

    crawlStartedAtRef.current = Date.now();

    const nextElapsedMs = Math.max(Math.floor(crawlElapsedBaseMsRef.current), 0);
    setCrawlElapsedMs(nextElapsedMs);
    saveCrawlElapsedSnapshot(activeJobId, nextElapsedMs, true);

    clearCrawlElapsedTimer();
    crawlElapsedTimerRef.current = window.setInterval(updateCrawlElapsedMs, 1000);
  };

  const stopCrawlElapsedTracking = () => {
    if (crawlStartedAtRef.current !== null) {
      crawlElapsedBaseMsRef.current += Date.now() - crawlStartedAtRef.current;
      crawlStartedAtRef.current = null;
    }

    const activeJobId = crawlJobId ?? loadActiveCrawlJobId();
    const savedElapsedMs = loadCrawlElapsedSnapshotMs(activeJobId);

    const nextElapsedMs = Math.max(
      Math.floor(crawlElapsedBaseMsRef.current),
      crawlElapsedMs,
      savedElapsedMs,
      0
    );

    crawlElapsedBaseMsRef.current = nextElapsedMs;
    setCrawlElapsedMs(nextElapsedMs);
    saveCrawlElapsedSnapshot(activeJobId, nextElapsedMs, false);
    clearCrawlElapsedTimer();
  };

  const getCrawlElapsedMsFromApi = (data: ApiResponse) => {
    const elapsedMs = Number(data.elapsedMs);

    if (Number.isFinite(elapsedMs) && elapsedMs >= 0) {
      return Math.floor(elapsedMs);
    }

    const elapsedSeconds = Number(data.elapsedSeconds);

    if (Number.isFinite(elapsedSeconds) && elapsedSeconds >= 0) {
      return Math.floor(elapsedSeconds * 1000);
    }

    return null;
  };

  const syncCrawlElapsedFromApi = (data: ApiResponse) => {
    const activeJobId = data.jobId ?? crawlJobId ?? loadActiveCrawlJobId();
    const serverElapsedMs = getCrawlElapsedMsFromApi(data);
    const savedElapsedMs = loadCrawlElapsedSnapshotMs(activeJobId);

    const currentRunningMs =
      crawlStartedAtRef.current !== null
        ? Date.now() - crawlStartedAtRef.current
        : 0;

    const currentClientElapsedMs = Math.max(
      Math.floor(crawlElapsedBaseMsRef.current + currentRunningMs),
      crawlElapsedMs,
      0
    );

    const nextElapsedMs = Math.max(
      serverElapsedMs ?? 0,
      savedElapsedMs,
      currentClientElapsedMs,
      0
    );

    const isRunning = data.jobStatus === "running";

    crawlElapsedBaseMsRef.current = nextElapsedMs;
    crawlStartedAtRef.current = isRunning ? Date.now() : null;

    setCrawlElapsedMs(nextElapsedMs);
    saveCrawlElapsedSnapshot(activeJobId, nextElapsedMs, isRunning);

    if (isRunning) {
      if (crawlElapsedTimerRef.current === null) {
        crawlElapsedTimerRef.current = window.setInterval(
          updateCrawlElapsedMs,
          1000
        );
      }

      return;
    }

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

  const crawlCandidatePercent =
    crawlProcessedCount === 0
      ? 0
      : Math.round((crawlUpdatedCount / crawlProcessedCount) * 100);

  const clearCrawlStatusPolling = () => {
    crawlStatusRequestIdRef.current += 1;

    if (crawlStatusTimerRef.current !== null) {
      window.clearInterval(crawlStatusTimerRef.current);
      crawlStatusTimerRef.current = null;
    }
  };

  const tryRestoreCrawlJob = async (targetJobId?: string | null) => {
  const restoreJobId = targetJobId ?? crawlJobId ?? loadActiveCrawlJobId();
  if (!restoreJobId || crawlRecoveringRef.current) return false;

  const savedElapsedMs = loadCrawlElapsedSnapshotMs(restoreJobId);
  if (savedElapsedMs > 0) {
    crawlElapsedBaseMsRef.current = Math.max(
      crawlElapsedBaseMsRef.current,
      savedElapsedMs,
      crawlElapsedMs,
      0
    );
    setCrawlElapsedMs(crawlElapsedBaseMsRef.current);
  }

  crawlRecoveringRef.current = true;

  try {
    const res = await fetch("/api/master_data/crawl", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        action: "get_job_status",
        jobId: restoreJobId,
        previewPage: 1,
        previewPageSize: CRAWL_PREVIEW_PAGE_SIZE,
      }),
      cache: "no-store",
    });

    const data = await readApiResponse(res);

    if (!res.ok || !data.ok) {
      throw new Error(data.error || "保存済みクローリング状態の復元に失敗しました");
    }

    setCrawlJobId(restoreJobId);
    saveActiveCrawlJobId(restoreJobId);
    applyCrawlStatus(data);
    setCrawlSelectedChanges({});
    setCrawlMessage("");
    setCrawlError("");

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
      startCrawlElapsedTracking(false);
      startCrawlStatusPolling(restoreJobId);
      crawlStatusErrorCountRef.current = 0;
      return true;
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

    if (data.jobStatus === "error") {
      setCrawlError(data.error || "クローリング中にエラーが発生しました");
    }

    if (data.jobStatus === "completed" && previewTotal === 0 && remainingCount === 0) {
      setCrawlJobId(null);
      saveActiveCrawlJobId(null);
    }

    crawlStatusErrorCountRef.current = 0;
    return true;
  } catch (e) {
    const message =
      e instanceof Error ? e.message : "保存済みクローリング状態の復元に失敗しました";

    if (message.includes("クローリングジョブが見つかりません")) {
      setCrawlJobId(null);
      saveActiveCrawlJobId(null);
    }

    return false;
  } finally {
    crawlRecoveringRef.current = false;
  }
};

const scheduleCrawlRecovery = (targetJobId?: string | null) => {
  const restoreJobId = targetJobId ?? crawlJobId ?? loadActiveCrawlJobId();
  if (!restoreJobId) return;

  clearCrawlStatusPolling();
  clearCrawlRestoreTimer();

  setCrawling(true);
  setCrawlProgressOpen(true);
  setCrawlPreviewOpen(false);
  setCrawlResumeConfirmOpen(false);
  setCrawlMessage("接続が切れました。復旧を確認しています");
  setCrawlError("");

  const retryRestore = async () => {
    const restored = await tryRestoreCrawlJob(restoreJobId);

    if (restored) {
      crawlStatusErrorCountRef.current = 0;
      return;
    }

    crawlStatusErrorCountRef.current += 1;

    setCrawling(true);
    setCrawlProgressOpen(true);
    setCrawlMessage("接続が切れました。復旧を確認しています");
    setCrawlError("");

    const nextDelayMs = Math.min(
      1500 + crawlStatusErrorCountRef.current * 500,
      10000
    );

    crawlRestoreTimerRef.current = window.setTimeout(() => {
      void retryRestore();
    }, nextDelayMs);
  };

  crawlRestoreTimerRef.current = window.setTimeout(() => {
    void retryRestore();
  }, 1500);
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

    syncCrawlElapsedFromApi(data);
  };

  const startCrawlStatusPolling = (jobId: string) => {
    clearCrawlStatusPolling();

    const pollingRequestId = crawlStatusRequestIdRef.current + 1;
    crawlStatusRequestIdRef.current = pollingRequestId;

    const isActivePollingRequest = () =>
      crawlStatusRequestIdRef.current === pollingRequestId;

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
          cache: "no-store",
        });

        const data = await readApiResponse(res);

        if (!isActivePollingRequest()) {
          return;
        }

        if (!res.ok || !data.ok) {
          throw new Error(data.error || "クローリング進捗の取得に失敗しました");
        }

        clearCrawlRestoreTimer();
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
          setCrawlMessage("クローリングを中断しました");
          saveActiveCrawlJobId(jobId);
          return;
        }

        if (data.jobStatus === "completed") {
          const previewRows = data.previewRows || [];
          const previewTotal = data.previewTotal ?? 0;

          stopCrawlElapsedTracking();
          clearCrawlStatusPolling();
          clearCrawlRestoreTimer();
          setCrawling(false);
          setCrawlProgressOpen(false);
          setCrawlJobStatus("completed");

          if (previewTotal === 0) {
            setCrawlPreviewRows([]);
            setCrawlPreviewTotalCount(0);
            setCrawlPreviewPage(1);
            setCrawlPreviewOpen(false);
            setCrawlMessage("保存候補はありませんでした");
            setCrawlSelectedChanges({});
            setCrawlJobId(null);
            saveActiveCrawlJobId(null);
          } else {
            setCrawlPreviewRows(previewRows);
            setCrawlPreviewTotalCount(previewTotal);
            setCrawlPreviewPage(data.previewPage ?? 1);
            setCrawlSelectedChanges({});
            setCrawlPreviewOpen(true);
            saveActiveCrawlJobId(jobId);
          }

          return;
        }

        if (data.jobStatus === "error") {
          const previewRows = data.previewRows || [];
          const previewTotal = data.previewTotal ?? 0;
          const remainingCount = data.remainingCount ?? 0;

          stopCrawlElapsedTracking();
          clearCrawlStatusPolling();
          clearCrawlRestoreTimer();
          setCrawling(false);
          setCrawlProgressOpen(false);
          setCrawlPreviewRows(previewRows);
          setCrawlPreviewTotalCount(previewTotal);
          setCrawlPreviewPage(data.previewPage ?? 1);
          setCrawlPreviewOpen(previewTotal > 0);
          setCrawlResumeConfirmOpen(previewTotal === 0 && remainingCount > 0);
          setCrawlError(data.error || "クローリング中にエラーが発生しました");
          saveActiveCrawlJobId(jobId);
          return;
        }
      } catch (e) {
        if (!isActivePollingRequest()) {
          return;
        }

        const message =
          e instanceof Error ? e.message : "クローリング進捗取得でエラーが発生しました";

        if (message.includes("クローリングジョブが見つかりません")) {
          clearCrawlStatusPolling();
          clearCrawlRestoreTimer();
          setCrawlJobId(null);
          saveActiveCrawlJobId(null);
          setCrawlProgressOpen(false);
          setCrawlError(message);
          return;
        }

        scheduleCrawlRecovery(jobId);
      }
    };

    void poll();

    const CRAWL_STATUS_POLL_INTERVAL_MS = isLocalAppRuntime() ? 1500 : 5000;

    crawlStatusTimerRef.current = window.setInterval(() => {
      void poll();
    }, CRAWL_STATUS_POLL_INTERVAL_MS);
  };

  const restoreSavedCrawlJob = async () => {
    const storedJobId = loadActiveCrawlJobId();
    if (!storedJobId) return;

    await tryRestoreCrawlJob(storedJobId);
  };

  useEffect(() => {
    if (loginStatus !== "logged_in") return;

    void restoreSavedCrawlJob();
  }, [loginStatus]);

  useEffect(() => {
    if (loginStatus !== "logged_in") return;

    const handleRecover = () => {
      const storedJobId = loadActiveCrawlJobId();
      if (!storedJobId) return;

      if (document.visibilityState && document.visibilityState !== "visible") {
        return;
      }

      void tryRestoreCrawlJob(storedJobId);
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        handleRecover();
      }
    };

    window.addEventListener("focus", handleRecover);
    window.addEventListener("online", handleRecover);
    document.addEventListener("visibilitychange", handleVisibilityChange);

    return () => {
      window.removeEventListener("focus", handleRecover);
      window.removeEventListener("online", handleRecover);
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      clearCrawlRestoreTimer();
    };
  }, [crawlJobId, loginStatus]);

  useEffect(() => {
    const saveBeforeClose = () => {
      updateCrawlElapsedMs();
    };

    window.addEventListener("pagehide", saveBeforeClose);
    window.addEventListener("beforeunload", saveBeforeClose);

    return () => {
      saveBeforeClose();
      window.removeEventListener("pagehide", saveBeforeClose);
      window.removeEventListener("beforeunload", saveBeforeClose);
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
      setCrawlMessage("クローリングを中断しました");
      setCrawlError("");
    } catch (e) {
      setCrawlError(
        e instanceof Error ? e.message : "中止処理でエラーが発生しました"
      );
    }
  };

  const handleCancelCrawlPreview = () => {
    const dismissedJobId = crawlJobId ?? loadActiveCrawlJobId();

    setCrawlPreviewOpen(false);
    setCrawlResumeConfirmOpen(false);
    setCrawlPreviewRows([]);
    setCrawlPreviewTotalCount(0);
    setCrawlPreviewPage(1);
    setCrawlSelectedChanges({});
    setCrawlJobId(null);
    saveActiveCrawlJobId(null);
    deleteCrawlElapsedMs(dismissedJobId);
    setCrawlJobStatus("idle");
  };

  const handlePauseCrawl = async () => {
    if (!crawlJobId) return;

    clearCrawlStatusPolling();
    setCrawlMessage("クローリングを中断しています。保存候補を読み込みます");
    setCrawlError("");

    try {
      const targetJobId = crawlJobId;

      const res = await fetch("/api/master_data/crawl", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "pause_job",
          jobId: targetJobId,
        }),
        cache: "no-store",
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "中断に失敗しました");
      }

      applyCrawlStatus(data);
      await tryRestoreCrawlJob(targetJobId);

      setCrawlMessage("クローリングを中断しました。保存できます。");
    } catch (e) {
      startCrawlStatusPolling(crawlJobId);
      setCrawlError(
        e instanceof Error ? e.message : "中断処理でエラーが発生しました"
      );
    }
  };

  const handleResumeCrawl = async () => {
    if (!crawlJobId) return;

    stopCrawlElapsedTracking();
    clearCrawlStatusPolling();
    setCrawlResumeConfirmOpen(false);
    setCrawlPreviewOpen(false);
    setCrawlPreviewRows([]);
    setCrawlPreviewTotalCount(0);
    setCrawlMessage("");
    setCrawlError("");
    setCrawling(true);
    setCrawlProgressOpen(true);

    try {
      const isLocal = isLocalAppRuntime();
      const worker = isLocal ? null : await checkLocalCrawlWorker();

      if (!isLocal && !worker) {
        throw new Error(
          "このPCのworkerが起動していません。master-crawl-worker.exe を起動してから再度実行してください。"
        );
      }

      const body: Record<string, unknown> = {
        action: "resume_job",
        jobId: crawlJobId,
      };

      if (!isLocal && worker) {
        body.assignedWorkerId = worker.workerId;
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

    const previousCrawlJobId = crawlJobId ?? loadActiveCrawlJobId();

    stopCrawlElapsedTracking();
    clearCrawlStatusPolling();
    deleteCrawlElapsedMs(previousCrawlJobId);
    setCrawling(true);
    setCrawlConfirmOpen(false);
    setCrawlPreviewOpen(false);
    setCrawlResumeConfirmOpen(false);
    setCrawlPreviewRows([]);
    setCrawlPreviewTotalCount(0);
    setCrawlPreviewPage(1);
    setCrawlSelectedChanges({});
    setCrawlPreviewTab("candidate");
    crawlPreviewRequestIdRef.current += 1;
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
      
      const isLocal = isLocalAppRuntime();
      const worker = isLocal ? null : await checkLocalCrawlWorker();

      if (!isLocal && !worker) {
        throw new Error(
          "このPCのworkerが起動していません。master-crawl-worker.exe を起動してから再度実行してください。"
        );
      }

      const body: Record<string, unknown> = {
        action: "start_preview_job",
        targetScope: crawlTargetScope,
        selectedFields: getSelectedCrawlFields(crawlFieldSelections),
      };

      if (!isLocal && worker) {
        body.assignedWorkerId = worker.workerId;
      }

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
        cache: "no-store",
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
      saveCrawlElapsedMs(data.jobId, 0);
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
        cache: "no-store",
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
    const excludedCount = data.skipped ?? 0;

    setItemInspectionPreviewChanges(previewChanges);
    setItemInspectionExcludedPreviewRows([]);
    setItemInspectionExcludedTotalCount(excludedCount);
    setItemInspectionCheckedPreviewRowIds(
      Object.fromEntries(previewChanges.map((row) => [row.rowId, true]))
    );
    setItemInspectionPreviewTab("candidate");
    setItemInspectionPreviewPage(1);

    if (previewChanges.length > 0 || excludedCount > 0) {
      setItemInspectionPreviewConfirmOpen(true);
    } else {
      setItemInspectionPreviewConfirmOpen(false);
    }
  };

  const fetchItemInspectionExcludedPreviewPage = async (
    jobId: string,
    nextPage: number
  ) => {
    const res = await fetch("/api/master_data/item_inspection", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        action: "get_job_status",
        jobId,
        previewTab: "excluded",
        previewPage: nextPage,
        previewPageSize: ITEM_INSPECTION_PREVIEW_PAGE_SIZE,
      }),
    });

    const data = await readApiResponse(res);

    if (!res.ok || !data.ok) {
      throw new Error(data.error || "候補外の取得に失敗しました");
    }

    applyItemInspectionStatus(data);
    setItemInspectionExcludedPreviewRows(data.inspectionPreviewChanges || []);
    setItemInspectionExcludedTotalCount(data.previewTotal ?? 0);
    setItemInspectionPreviewPage(data.previewPage ?? nextPage);
  };

  const handleItemInspectionPreviewTabChange = async (
    nextTab: "candidate" | "excluded"
  ) => {
    setItemInspectionPreviewTab(nextTab);
    setItemInspectionPreviewPage(1);
    itemInspectionPreviewScrollRef.current?.scrollTo({ top: 0 });

    if (nextTab === "excluded" && itemInspectionJobId) {
      try {
        await fetchItemInspectionExcludedPreviewPage(itemInspectionJobId, 1);
      } catch (e) {
        setItemInspectionError(
          e instanceof Error ? e.message : "候補外取得でエラーが発生しました"
        );
      }
    }
  };

  const handleItemInspectionPreviewPageChange = async (nextPage: number) => {
    if (nextPage < 1 || nextPage > itemInspectionPreviewTotalPages) {
      return;
    }

    if (itemInspectionPreviewTab === "candidate") {
      setItemInspectionPreviewPage(nextPage);
      itemInspectionPreviewScrollRef.current?.scrollTo({ top: 0 });
      return;
    }

    if (!itemInspectionJobId) {
      return;
    }

    try {
      itemInspectionPreviewScrollRef.current?.scrollTo({ top: 0 });
    } catch (e) {
      setItemInspectionError(
        e instanceof Error ? e.message : "候補外取得でエラーが発生しました"
      );
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
    if (!itemInspectionJobId) {
      setItemInspectionPreviewConfirmOpen(false);
      setItemInspectionPreviewChanges([]);
      setItemInspectionExcludedPreviewRows([]);
      setItemInspectionExcludedTotalCount(0);
      setItemInspectionPreviewTab("candidate");
      setItemInspectionPreviewPage(1);
      setItemInspectionCheckedPreviewRowIds({});
      return;
    }

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
      setItemInspectionPreviewConfirmOpen(false);
      setItemInspectionPreviewChanges([]);
      setItemInspectionExcludedPreviewRows([]);
      setItemInspectionExcludedTotalCount(0);
      setItemInspectionPreviewTab("candidate");
      setItemInspectionPreviewPage(1);
      setItemInspectionCheckedPreviewRowIds({});
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

  const handleConfirmInspectionCancel = async () => {
    const target = inspectionCancelConfirmTarget;

    if (!target) return;

    setInspectionCancelConfirmTarget(null);

    if (target === "crawlProgress") {
      await handleCancelCrawl();
      return;
    }

    if (target === "crawlPreview") {
      handleCancelCrawlPreview();
      return;
    }

    if (
      target === "itemInspectionProgress" ||
      target === "itemInspectionPreview"
    ) {
      await handleCancelItemInspection();
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

  const handleResumeItemInspection = async () => {
    if (!itemInspectionJobId) return;

    setItemInspectionPreviewConfirmOpen(false);
    setItemInspectionPreviewChanges([]);
    setItemInspectionExcludedPreviewRows([]);
    setItemInspectionExcludedTotalCount(0);
    setItemInspectionPreviewTab("candidate");
    setItemInspectionPreviewPage(1);
    setItemInspectionCheckedPreviewRowIds({});
    setItemInspectionError("");
    setItemInspectionMessage("");
    setItemInspecting(true);
    setItemInspectionJobStatus("running");
    setItemInspectionProgressOpen(true);
    startItemInspectionElapsedTracking(false);

    try {
      const res = await fetch("/api/master_data/item_inspection", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          action: "resume_job",
          jobId: itemInspectionJobId,
        }),
      });

      const data = await readApiResponse(res);

      if (!res.ok || !data.ok) {
        throw new Error(data.error || "再開に失敗しました");
      }

      applyItemInspectionStatus(data);
      startItemInspectionStatusPolling(itemInspectionJobId);
    } catch (e) {
      clearItemInspectionStatusPolling();
      stopItemInspectionElapsedTracking();
      setItemInspecting(false);
      setItemInspectionProgressOpen(false);
      setItemInspectionError(
        e instanceof Error ? e.message : "再開処理でエラーが発生しました"
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

    if (!hasSelectedItemInspectionMethods) {
      return;
    }

    setItemInspecting(true);
    setItemInspectionMethodOpen(false);
    setItemInspectionPreviewConfirmOpen(false);
    setItemInspectionError("");
    setItemInspectionMessage("");
    setItemInspectionPreviewChanges([]);
    setItemInspectionExcludedPreviewRows([]);
    setItemInspectionExcludedTotalCount(0);
    setItemInspectionPreviewTab("candidate");
    setItemInspectionPreviewPage(1);
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
        throw new Error(data.error || "項目精査 開始に失敗しました");
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
        e instanceof Error ? e.message : "項目精査 開始でエラーが発生しました"
      );
    }
  };

  const handleApplyItemInspectionChanges = async () => {
    const targetChanges = itemInspectionPreviewChanges.filter(
      (row) =>
        row.action !== "review" &&
        itemInspectionCheckedPreviewRowIds[row.rowId] !== false
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
      setItemInspectionExcludedPreviewRows([]);
      setItemInspectionExcludedTotalCount(0);
      setItemInspectionPreviewTab("candidate");
      setItemInspectionPreviewPage(1);
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
    if (!dedupeTargetScope || !dedupeSelectedField || !dedupeMatchMethod) return;

    setDeduplicating(true);
    setDedupeMessage("");
    setDedupeError("");

    try {
      const params = new URLSearchParams();
      params.set("deleteMode", "dedupe");
      params.set("deleteScope", dedupeTargetScope);
      params.set("dedupeField", dedupeSelectedField);
      params.set("dedupeMatchMethod", dedupeMatchMethod);

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

  const usingVirtual = true;

  const activeSidebarPanelTitle = openSidebarPanel
    ? SIDEBAR_PANEL_TITLES[openSidebarPanel]
    : "";

  const sidebarPanelMaxWidthClass =
    openSidebarPanel === "csv"
      ? "max-w-[920px]"
      : openSidebarPanel === "inspection"
      ? "max-w-[900px]"
      : openSidebarPanel === "search" || openSidebarPanel === "list"
      ? "max-w-[760px]"
      : openSidebarPanel === "settings"
      ? "max-w-[1180px]"
      : openSidebarPanel === "theme"
      ? "max-w-[560px]"
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

  const blockingLoadingState = loginLoading
    ? {
        title: "ログイン中",
        message: "ログイン情報を確認しています",
      }
    : loading
    ? {
        title: "リスト読み込み中",
        message: "リスト情報を読み込んでいます",
      }
    : advancedLoading
    ? {
        title: "検索条件読み込み中",
        message: "検索候補を読み込んでいます",
      }
    : permissionLoading
    ? {
        title: "権限情報読み込み中",
        message: "権限情報を読み込んでいます",
      }
    : crawlPreviewLoading
    ? {
        title: "クローリング結果読み込み中",
        message: "クローリング結果確認を読み込んでいます",
      }
    : null;

  const topLeftNoticeItems: MasterDataNoticeItem[] = [];

  const addTopLeftTextNotice = (
    key: string,
    tone: MasterDataNoticeTone,
    title: string,
    message: string
  ) => {
    if (message.trim() === "") {
      return;
    }

    topLeftNoticeItems.push({
      key,
      tone,
      title,
      message,
      loading: false,
    });
  };

  const activeFilterLoading =
    openFilterKey !== null && draftColumnStates[openFilterKey]?.valueLoading;

  if (activeFilterLoading) {
    topLeftNoticeItems.push({
      key: "filter-values-loading",
      tone: "working",
      title: "フィルタ候補 読み込み中",
      message: "候補データを読み込んでいます",
      loading: true,
    });
  }

  if (advancedLoading) {
    topLeftNoticeItems.push({
      key: "advanced-filter-loading",
      tone: "working",
      title: "検索条件 読み込み中",
      message: "検索候補を読み込んでいます",
      loading: true,
    });
  }

  if (permissionLoading) {
    topLeftNoticeItems.push({
      key: "permission-loading",
      tone: "working",
      title: "権限情報 読み込み中",
      message: "従業員アカウントを読み込んでいます",
      loading: true,
    });
  }

  if (permissionSaving) {
    topLeftNoticeItems.push({
      key: "permission-saving",
      tone: "working",
      title: "権限 保存中",
      message: "権限設定を保存しています",
      loading: true,
    });
  }

  if (mynaviJobStatus === "running") {
    const mynaviRunningMessage =
      mynaviPhase === "collect_urls"
        ? `URL取得中${
            mynaviCurrentPageNumber > 0
              ? `：${mynaviCurrentPageNumber}ページ目`
              : ""
          }`
        : mynaviPhase === "scrape_details"
        ? `全項目取得中：${mynaviProcessedCount.toLocaleString()} / ${mynaviTotalUrls.toLocaleString()}`
        : "マイナビ新卒を処理しています";

    topLeftNoticeItems.push({
      key: "mynavi-running",
      tone: "working",
      title: `マイナビ新卒 ${getMynaviPhaseLabel(mynaviJobMode, mynaviPhase)}`,
      message: `${mynaviRunningMessage} / 成功 ${mynaviSuccessCount.toLocaleString()} 件 / 失敗 ${mynaviFailedCount.toLocaleString()} 件`,
      loading: true,
    });
  } else if (mynaviLoading) {
    topLeftNoticeItems.push({
      key: "mynavi-loading",
      tone: "working",
      title: "マイナビ新卒 確認中",
      message: "取得に必要な情報を確認しています",
      loading: true,
    });
  }

  if (importing) {
    topLeftNoticeItems.push({
      key: "csv-importing",
      tone: "working",
      title: "CSV投入中",
      message: "CSVを取り込んでいます",
      loading: true,
    });
  }

  if (exporting) {
    topLeftNoticeItems.push({
      key: "csv-exporting",
      tone: "working",
      title: "CSV抽出中",
      message: "CSVを保存しています",
      loading: true,
    });
  }

  if (csvTemplateSaving) {
    topLeftNoticeItems.push({
      key: "csv-template-saving",
      tone: "working",
      title: "CSVテンプレート保存中",
      message: "CSVテンプレートを保存しています",
      loading: true,
    });
  }

  if (previewCsvExporting) {
    topLeftNoticeItems.push({
      key: "preview-csv-exporting",
      tone: "working",
      title: "CSV抽出中",
      message: "結果確認画面のCSVを保存しています",
      loading: true,
    });
  }

  if (listDeleting) {
    topLeftNoticeItems.push({
      key: "list-deleting",
      tone: "working",
      title: "リスト削除中",
      message: "対象リストを削除しています",
      loading: true,
    });
  }

  if (itemDeleting) {
    topLeftNoticeItems.push({
      key: "item-deleting",
      tone: "working",
      title: "項目削除中",
      message: "選択した項目を削除しています",
      loading: true,
    });
  }

  if (deduplicating) {
    topLeftNoticeItems.push({
      key: "deduplicating",
      tone: "working",
      title: "重複削除中",
      message: "重複しているリストを整理しています",
      loading: true,
    });
  }

  if (crawling) {
    topLeftNoticeItems.push({
      key: "crawling",
      tone: "working",
      title: "クローリング処理中",
      message: "クローリング関連の処理を実行しています",
      loading: true,
    });
  }

  if (crawlPreviewLoading) {
    topLeftNoticeItems.push({
      key: "crawl-preview-loading",
      tone: "working",
      title: "クローリング結果 読み込み中",
      message: "クローリング結果確認を読み込んでいます",
      loading: true,
    });
  }

  if (itemInspecting) {
    topLeftNoticeItems.push({
      key: "item-inspecting",
      tone: "working",
      title: "項目精査中",
      message: "登録済みデータを精査しています",
      loading: true,
    });
  }

  addTopLeftTextNotice("mynavi-error", "error", "マイナビ新卒 エラー", mynaviError);
  addTopLeftTextNotice("import-error", "error", "CSV エラー", importError);
  addTopLeftTextNotice("list-delete-error", "error", "リスト削除 エラー", listDeleteError);
  addTopLeftTextNotice("item-delete-error", "error", "項目削除 エラー", itemDeleteError);
  addTopLeftTextNotice("dedupe-error", "error", "重複削除 エラー", dedupeError);
  addTopLeftTextNotice("crawl-error", "error", "クローリング エラー", crawlError);
  addTopLeftTextNotice("item-inspection-error", "error", "項目精査 エラー", itemInspectionError);
  addTopLeftTextNotice("permission-error", "error", "権限管理 エラー", permissionError);

  addTopLeftTextNotice("mynavi-message", "success", "マイナビ新卒", mynaviMessage);
  addTopLeftTextNotice("import-message", "success", "CSV", importMessage);
  addTopLeftTextNotice("list-delete-message", "success", "リスト削除", listDeleteMessage);
  addTopLeftTextNotice("item-delete-message", "success", "項目削除", itemDeleteMessage);
  addTopLeftTextNotice("dedupe-message", "success", "重複削除", dedupeMessage);
  addTopLeftTextNotice("crawl-message", "success", "クローリング", crawlMessage);
  addTopLeftTextNotice("item-inspection-message", "success", "項目精査", itemInspectionMessage);
  addTopLeftTextNotice("permission-message", "success", "権限管理", permissionMessage);

  const selectedCrawlFields = visibleCrawlConfirmFieldOptions
    .filter((field) => crawlFieldSelections[field.key])
    .map((field) => field.key);
  
  const hasSelectedCrawlFields = selectedCrawlFields.length > 0;

  const selectedItemDeleteFields = getSelectedItemDeleteFields(
    itemDeleteSelections
  );
  const hasSelectedItemDeleteFields = selectedItemDeleteFields.length > 0;

  const selectedItemDeleteLabels = COLUMN_DEFS.filter(
    (column) => itemDeleteSelections[column.key]
  ).map((column) => column.label);

  const selectedDedupeFieldLabel =
    COLUMN_DEFS.find((column) => column.key === dedupeSelectedField)?.label ?? "";

  const selectedDedupeMatchMethodLabel =
    dedupeMatchMethod === "exact" ? "完全一致" : "";

  const selectedItemInspectionFields = visibleItemInspectionColumnDefs
    .filter((column) => itemInspectionSelections[column.key])
    .map((column) => column.key);

  const selectedItemInspectionLabels = visibleItemInspectionColumnDefs
    .filter((column) => itemInspectionSelections[column.key])
    .map((column) => column.label);

  const hasSelectedItemInspectionFields =
    selectedItemInspectionFields.length > 0;

  const hasSelectedItemInspectionMethods =
    itemInspectionMethodSelections.representative_name_remove_non_name ||
    itemInspectionMethodSelections.representative_name_inspect_name;

  const itemInspectionCandidatePercent =
    itemInspectionProcessedCount === 0
      ? 0
      : Math.round(
          (itemInspectionUpdatedCount / itemInspectionProcessedCount) * 100
        );

  const averageItemInspectionMs =
    itemInspectionProcessedCount > 0
      ? Math.round(itemInspectionElapsedMs / itemInspectionProcessedCount)
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
          key={`master-list-${page}-${limit}`}
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
            <Cell title={row.permit_number || ""}><EmptyValue value={row.permit_number} /></Cell>
            <Cell title={row.memo || ""} className="whitespace-pre-wrap"><EmptyValue value={row.memo} /></Cell>
          </div>
        ))}
      </div>
    );
  }, [loading, rows]);

  return (
    <main
      data-theme={effectiveThemeMode}
      className={`app-responsive-root ${
        sidebarOpen ? "app-sidebar-open" : "app-sidebar-closed"
      } h-[100dvh] overflow-hidden bg-transparent text-slate-100`}
    >

      <MasterDataTopLeftNotices items={topLeftNoticeItems} />

      {blockingLoadingState && (
        <BlockingLoadingOverlay
          title={blockingLoadingState.title}
          message={blockingLoadingState.message}
        />
      )}
      {!screenReady || loginStatus === "checking" ? (
        <BlockingLoadingOverlay
          title="読み込み中"
          message="画面を準備しています"
        />
      ) : loginStatus !== "logged_in" ? (
        <div className="master-data-login-screen grid h-full min-h-0 grid-cols-1 overflow-hidden bg-white lg:grid-cols-2">
          <section className="flex min-h-[320px] flex-col items-center justify-center bg-[#05070d] px-8 py-10 text-center">
            <MasterDataBrandLogo className="h-auto w-[min(600px,82vw)] shrink-0 -translate-y-[40px]" />

            <h1 className="master-data-brand-title mt-8 text-[52px] leading-none md:text-[72px]">
              マスタデータ
            </h1>
          </section>

          <section className="flex min-h-[420px] items-center justify-center bg-white px-6 py-10 text-slate-900">
            <form
              onSubmit={handleLoginSubmit}
              className="w-full max-w-[440px]"
            >
              <div className="mb-8 flex justify-center">
                <MasterDataLoginWordmark className="h-auto w-[340px] translate-y-[40px]" />
              </div>

              <div className="space-y-5">
                <div>
                  <label className="mb-2 block text-sm font-semibold text-slate-700">
                    ID
                  </label>
                  <input
                    value={loginId}
                    onChange={(e) => setLoginId(e.target.value)}
                    disabled={loginLoading}
                    autoComplete="username"
                    className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 text-base text-slate-900 outline-none transition placeholder:text-slate-400 focus:border-sky-500 focus:ring-4 focus:ring-sky-500/10 disabled:bg-slate-100"
                    placeholder="IDを入力"
                  />
                </div>

                <div>
                  <label className="mb-2 block text-sm font-semibold text-slate-700">
                    パスワード
                  </label>

                  <div className="relative">
                    <input
                      type={loginPasswordVisible ? "text" : "password"}
                      value={loginPassword}
                      onChange={(e) => setLoginPassword(e.target.value)}
                      disabled={loginLoading}
                      autoComplete="current-password"
                      className="h-12 w-full rounded-2xl border border-slate-200 bg-white px-4 pr-12 text-base text-slate-900 outline-none transition placeholder:text-slate-400 focus:border-sky-500 focus:ring-4 focus:ring-sky-500/10 disabled:bg-slate-100"
                      placeholder="パスワードを入力"
                    />

                    <button
                      type="button"
                      onClick={() => setLoginPasswordVisible((prev) => !prev)}
                      disabled={loginLoading}
                      className="absolute right-3 top-1/2 inline-flex h-8 w-8 -translate-y-1/2 items-center justify-center rounded-lg text-slate-500 transition hover:bg-slate-100 hover:text-slate-800 disabled:opacity-40"
                      aria-label={
                        loginPasswordVisible
                          ? "パスワードを隠す"
                          : "パスワードを表示する"
                      }
                    >
                      {loginPasswordVisible ? (
                        <svg
                          viewBox="0 0 24 24"
                          fill="none"
                          stroke="currentColor"
                          strokeWidth="1.8"
                          className="h-5 w-5"
                        >
                          <path d="M3 3l18 18" />
                          <path d="M10.7 10.7a2 2 0 0 0 2.6 2.6" />
                          <path d="M9.5 5.3A9.5 9.5 0 0 1 12 5c5 0 8.5 4.5 9.5 7-0.4 1-1.3 2.4-2.7 3.7" />
                          <path d="M6.2 6.8C4.4 8.1 3.2 10.1 2.5 12c1 2.5 4.5 7 9.5 7 1.5 0 2.8-.4 4-1" />
                        </svg>
                      ) : (
                        <svg
                          viewBox="0 0 24 24"
                          fill="none"
                          stroke="currentColor"
                          strokeWidth="1.8"
                          className="h-5 w-5"
                        >
                          <path d="M2.5 12S6 5 12 5s9.5 7 9.5 7-3.5 7-9.5 7-9.5-7-9.5-7Z" />
                          <circle cx="12" cy="12" r="3" />
                        </svg>
                      )}
                    </button>
                  </div>
                </div>

                {loginError && (
                  <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
                    {loginError}
                  </div>
                )}

                <button
                  type="submit"
                  disabled={loginLoading}
                  className="h-12 w-full rounded-2xl bg-[#0b1326] px-5 text-sm font-semibold text-white shadow-[0_16px_36px_rgba(15,23,42,0.22)] transition hover:bg-[#111d38] disabled:opacity-50"
                >
                {loginLoading ? <LoadingText label="ログイン中" /> : "ログイン"}
                </button>
              </div>
            </form>
          </section>
        </div>
      ) : (
      <div
        className="mx-auto flex h-full min-w-0 max-w-[1880px] flex-col px-[var(--app-page-x)] py-[var(--app-page-y)] pl-[var(--app-content-left)] transition-all duration-300"
      >

        <aside
          className="fixed bottom-[var(--app-sidebar-gap)] left-[var(--app-sidebar-left)] top-[var(--app-sidebar-gap)] z-40 w-[var(--app-sidebar-width)] overflow-visible transition-all duration-300"
        >
          <div className="mb-2 flex h-[var(--app-logo-height)] w-full items-center justify-center overflow-hidden px-0">
            <MasterDataBrandLogo
              className={`h-[var(--app-logo-height)] shrink-0 ${
                sidebarOpen
                  ? "w-[var(--app-logo-width)]"
                  : "w-[var(--app-sidebar-width)]"
              }`}
            />
          </div>

          <div className="app-scrollbar max-h-[calc(100dvh-var(--app-sidebar-menu-offset))] overflow-y-auto rounded-[var(--app-radius-lg)] border border-sky-300/10 bg-gradient-to-b from-[#0b1326]/95 via-[#08101d]/92 to-[#050b14]/95 p-[var(--app-panel-pad-xs)] shadow-[0_24px_60px_rgba(0,0,0,0.38)] backdrop-blur-2xl">
            <div className="rounded-[var(--app-radius-md)] border border-white/10 bg-[#0b1326]/85 p-[var(--app-panel-pad)]">

              <div
                className={`mb-3 flex items-center ${
                  sidebarOpen ? "justify-between" : "justify-center"
                }`}
              >
                {sidebarOpen && (
                  <div className="min-w-0 px-1">
                    <div
                      className="master-data-brand-logo__wordmark bg-gradient-to-r from-sky-100 via-white to-cyan-200 bg-clip-text text-[13px] font-black uppercase tracking-[0.32em] text-transparent drop-shadow-[0_0_16px_rgba(56,189,248,0.35)]"
                      style={{
                        WebkitTextStroke: "0.28px rgba(255, 255, 255, 0.55)",
                        textShadow:
                          "0 0 1px rgba(255, 255, 255, 0.95), 0 0 14px rgba(56, 189, 248, 0.42)",
                      }}
                    >
                      NAVIGATION
                    </div>
                  </div>
                )}

                <button
                  type="button"
                  onClick={() => setSidebarOpen((prev) => !prev)}
                  className="inline-flex h-10 w-10 items-center justify-center rounded-2xl border border-sky-300/20 bg-[#0f172a]/90 text-sky-100 shadow-[0_0_20px_rgba(56,189,248,0.12)] transition hover:border-sky-300/40 hover:bg-sky-500/10"
                  title={sidebarOpen ? "メニューを閉じる" : "メニューを開く"}
                >
                  {sidebarOpen ? "‹" : "›"}
                </button>
              </div>

              <div className="space-y-2">
                {SIDEBAR_MENU_ITEMS.filter((item) => {
                  if (item.key === "search") {
                    return canUseAnyPermission(
                      ADVANCED_FILTER_BUTTONS.map((button) =>
                        getAdvancedFilterPermissionKey(button.key)
                      )
                    );
                  }

                  if (item.key === "list") {
                    return canUseAnyPermission([
                      "list.add",
                      "list.delete",
                      "list.itemDelete",
                      "list.dedupe",
                    ]);
                  }

                  if (item.key === "csv") {
                    return canUseAnyPermission([
                      "csv.import",
                      "csv.export",
                      "csv.template",
                    ]);
                  }

                  if (item.key === "inspection") {
                    return canUseCrawlPanel || canUseItemInspectionPanel;
                  }

                  return true;
                }).map((item) => {
                  const isActive = openSidebarPanel === item.key;

                  return (
                    <div key={item.key} className="space-y-2">
                      <button
                        type="button"
                        onClick={() => handleOpenSidebarPanel(item.key)}
                        className={`master-data-sidebar-menu-button group relative flex w-full items-center gap-2 overflow-hidden rounded-2xl border px-2 py-3 text-left text-[clamp(11px,0.85vw,14px)] font-semibold transition ${
                          isActive
                            ? "border-sky-400/40 bg-gradient-to-br from-sky-500/22 via-white/8 to-indigo-500/10 text-sky-100 shadow-[0_0_26px_rgba(56,189,248,0.16)]"
                            : "border-white/10 bg-gradient-to-br from-white/8 via-white/5 to-[#0b1220] text-slate-200 hover:border-sky-300/30 hover:bg-white/10"
                        } ${sidebarOpen ? "justify-start" : "justify-center"}`}
                      >
                        <span
                          className={`master-data-sidebar-menu-icon inline-flex h-[var(--app-menu-icon-size)] w-[var(--app-menu-icon-size)] shrink-0 items-center justify-center rounded-xl border transition ${
                            isActive
                              ? "border-sky-300/30 bg-sky-400/15 text-sky-100"
                              : "border-white/10 bg-[#0f172a] text-slate-200 group-hover:border-sky-300/30 group-hover:bg-sky-400/10"
                          }`}
                        >
                          <SidebarMenuIcon menuKey={item.key} />
                        </span>

                        {sidebarOpen && (
                          <span className="min-w-0 whitespace-nowrap leading-none">{item.label}</span>
                        )}
                      </button>

                      {item.key === "search" && (
                        <button
                          type="button"
                          onClick={() => {
                            setOpenSidebarPanel(null);
                            setPermissionListScopeSearchOpen(false);
                            setOpenFilterKey(null);
                            setOpenAdvancedFilterKey(null);
                            setAllFiltersClearConfirmTarget("main");
                            setAllFiltersClearConfirmOpen(true);
                          }}
                          className={`master-data-sidebar-filter-clear-button group flex w-full items-center gap-2 rounded-2xl border px-2 py-3 text-left text-[clamp(11px,0.85vw,14px)] font-semibold transition ${
                            allFiltersClearConfirmOpen
                              ? "border-sky-400/40 bg-gradient-to-br from-sky-500/20 via-white/8 to-indigo-500/10 text-sky-100 shadow-[0_0_24px_rgba(56,189,248,0.14)]"
                              : "border-white/10 bg-gradient-to-br from-white/8 via-white/5 to-[#0b1220] text-slate-200 hover:border-sky-300/30 hover:bg-white/10"
                          } ${sidebarOpen ? "justify-start" : "justify-center"}`}
                        >
                          <span
                            className={`master-data-sidebar-filter-clear-icon inline-flex h-[var(--app-menu-icon-size)] w-[var(--app-menu-icon-size)] shrink-0 items-center justify-center rounded-xl border text-base ${
                              allFiltersClearConfirmOpen
                                ? "border-sky-300/30 bg-sky-400/20 text-sky-100"
                                : "border-white/10 bg-[#0f172a] text-slate-200 group-hover:border-sky-300/30 group-hover:bg-sky-400/10"
                            }`}
                          >
                            ↺
                          </span>

                          {sidebarOpen && (
                            <span className="min-w-0 whitespace-nowrap leading-none">フィルタ解除</span>
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
          className="sticky top-0 z-30 mb-[var(--app-gap-md)] rounded-[var(--app-radius-lg)] border border-white/10 bg-[#08101d]/80 p-[var(--app-panel-pad-xs)] shadow-[0_24px_60px_rgba(0,0,0,0.35)] backdrop-blur-2xl"
        >
          <div className="rounded-[var(--app-radius-md)] border border-white/10 bg-[#0b1326]/85 p-[var(--app-panel-pad-lg)]">
            <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
              <div className="flex justify-start pl-0 sm:pl-6 xl:flex-1 xl:pl-24">
                <h1 className="master-data-brand-title text-left text-[30px] leading-none md:text-[42px]">
                  マスタデータ
                </h1>
              </div>

              <div className="grid grid-cols-2 gap-[var(--app-gap-sm)] xl:grid-cols-6">
                <div className="group relative flex min-h-[var(--app-stat-card-h)] flex-col items-center justify-center overflow-hidden rounded-2xl border border-sky-300/20 bg-gradient-to-br from-sky-500/15 via-white/5 to-[#0b1220] px-[var(--app-card-x)] py-[var(--app-card-y)] text-center shadow-[0_14px_34px_rgba(0,0,0,0.18)]">
                  <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(56,189,248,0.22),transparent_42%)] opacity-0 transition group-hover:opacity-100" />
                  <div className="relative text-xs font-semibold text-sky-100/80">総件数</div>
                  <div className="relative mt-1 text-lg font-bold text-white md:text-xl">
                    {total.toLocaleString()}件
                  </div>
                </div>

                <div className="group relative flex min-h-[var(--app-stat-card-h)] flex-col items-center justify-center overflow-visible rounded-2xl border border-indigo-300/20 bg-gradient-to-br from-indigo-500/15 via-white/5 to-[#0b1220] px-[var(--app-card-x)] py-[var(--app-card-y)] text-center shadow-[0_14px_34px_rgba(0,0,0,0.18)]">
                  <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(129,140,248,0.22),transparent_42%)] opacity-0 transition group-hover:opacity-100" />
                  <div className="relative text-xs font-semibold text-indigo-100/80">
                    現在ページ
                  </div>

                  <PageSelectDropdown
                    page={page}
                    totalPages={totalPages}
                    disabled={limit === "all"}
                    open={openPageDropdown === "mainHeader"}
                    onToggle={() =>
                      setOpenPageDropdown((current) =>
                        current === "mainHeader" ? null : "mainHeader"
                      )
                    }
                    onClose={() => setOpenPageDropdown(null)}
                    onSelect={(pageNumber) => {
                      setPage(pageNumber);
                      setOpenPageDropdown(null);
                    }}
                    className="h-10 min-w-[104px] text-sm"
                  />
                </div>

                <div className="group relative flex min-h-[var(--app-stat-card-h)] flex-col items-center justify-center overflow-hidden rounded-2xl border border-cyan-300/20 bg-gradient-to-br from-cyan-500/15 via-white/5 to-[#0b1220] px-[var(--app-card-x)] py-[var(--app-card-y)] text-center shadow-[0_14px_34px_rgba(0,0,0,0.18)]">
                  <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(34,211,238,0.22),transparent_42%)] opacity-0 transition group-hover:opacity-100" />
                  <div className="relative text-xs font-semibold text-cyan-100/80">
                    総ページ数
                  </div>
                  <div className="relative mt-1 text-lg font-bold text-white md:text-xl">
                    {totalPages}
                  </div>
                </div>

                <div className="group relative flex min-h-[var(--app-stat-card-h)] flex-col items-center justify-center overflow-visible rounded-2xl border border-emerald-300/20 bg-gradient-to-br from-emerald-500/15 via-white/5 to-[#0b1220] px-[var(--app-card-x)] py-[var(--app-card-y)] text-center shadow-[0_14px_34px_rgba(0,0,0,0.18)] xl:mr-8">
                  <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top_right,rgba(16,185,129,0.22),transparent_42%)] opacity-0 transition group-hover:opacity-100" />
                  <div className="relative text-xs font-semibold text-emerald-100/80">
                    表示件数
                  </div>

                  <div
                    className="relative mt-2"
                    onBlur={(e) => {
                      if (!e.currentTarget.contains(e.relatedTarget as Node | null)) {
                        setOpenPageSizeDropdown(null);
                      }
                    }}
                  >
                    <button
                      type="button"
                      onClick={() =>
                        setOpenPageSizeDropdown((current) =>
                          current === "main" ? null : "main"
                        )
                      }
                      className="group/size relative flex h-10 min-w-[104px] items-center justify-between gap-2 rounded-xl border border-emerald-300/30 bg-gradient-to-br from-emerald-400/15 via-[#0f172a] to-[#07111f] px-3 text-sm font-black text-slate-100 shadow-inner outline-none transition hover:border-emerald-300/50 hover:bg-emerald-500/15 focus:border-emerald-300/60 focus:ring-2 focus:ring-emerald-300/20"
                    >
                      <span className="font-black" style={{ fontWeight: 900 }}>
                        {pageSizeOptions.find((opt) => opt.value === limit)?.label ?? `${limit}件`}
                      </span>
                      <span className="text-xs font-black text-emerald-100 transition group-hover/size:translate-y-0.5">
                        ▾
                      </span>
                    </button>

                    {openPageSizeDropdown === "main" && (
                      <div className="absolute left-1/2 top-[calc(100%+8px)] z-[120] w-[118px] -translate-x-1/2 overflow-hidden rounded-2xl border border-emerald-300/25 bg-[#07111f]/98 p-1.5 shadow-[0_18px_44px_rgba(0,0,0,0.45)] backdrop-blur-xl">
                        {pageSizeOptions.map((opt) => {
                          const active = limit === opt.value;

                          return (
                            <button
                              key={opt.value}
                              type="button"
                              onMouseDown={(e) => e.preventDefault()}
                              onClick={() => {
                                setLimit(opt.value);
                                setPage(1);
                                setOpenPageSizeDropdown(null);
                              }}
                              className={`flex h-9 w-full items-center justify-center rounded-xl text-sm font-black transition ${
                                active
                                  ? "bg-gradient-to-r from-emerald-400 to-cyan-400 text-[#03131f] shadow-[0_0_18px_rgba(45,212,191,0.24)]"
                                  : "text-slate-100 hover:bg-emerald-400/12 hover:text-emerald-100"
                              }`}
                              style={{ fontWeight: 900 }}
                            >
                              {opt.label}
                            </button>
                          );
                        })}
                      </div>
                    )}
                  </div>
                </div>

                <div
                  className={`group relative flex min-h-[var(--app-stat-card-h)] items-center justify-center overflow-hidden rounded-2xl px-[var(--app-card-x)] py-[var(--app-card-y)] text-center shadow-[0_14px_34px_rgba(0,0,0,0.18)] ${getMasterDataAccountCardClass(
                    loginUser?.role
                  )}`}
                >
                  <div className="flex min-w-0 items-center justify-center gap-2">
                    <span
                      className={`inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-2xl border ${getMasterDataAccountIconClass(
                        loginUser?.role
                      )}`}
                    >
                      <svg
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="1.8"
                        className="h-5 w-5"
                      >
                        <path d="M20 21a8 8 0 0 0-16 0" />
                        <circle cx="12" cy="8" r="4" />
                      </svg>
                    </span>

                    <div className="min-w-0 text-left leading-tight">
                      <div
                        className="max-w-[120px] truncate text-sm font-bold text-white"
                        title={loginUser?.name ?? ""}
                      >
                        {loginUser?.name ?? "-"}
                      </div>

                      <div
                        className={`mt-1 inline-flex rounded-full border px-2 py-0.5 text-[11px] font-semibold ${getMasterDataRoleBadgeClass(
                          loginUser?.role
                        )}`}
                      >
                        {loginUser?.role ?? "-"}
                      </div>
                    </div>
                  </div>
                </div>

                <div className="flex min-h-[var(--app-stat-card-h)] items-center justify-center">
                  <button
                    type="button"
                    onClick={handleLogout}
                    className="group inline-flex h-11 items-center justify-center gap-2 rounded-2xl border border-rose-300/20 bg-gradient-to-br from-rose-500/18 via-white/5 to-[#0b1220] px-4 text-sm font-semibold text-rose-100 shadow-[0_14px_34px_rgba(0,0,0,0.18)] transition hover:border-rose-300/40 hover:bg-rose-500/10"
                  >
                    <svg
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2.8"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      className="h-4 w-4"
                    >
                      <path d="M10 17l5-5-5-5" />
                      <path d="M15 12H3" />
                      <path d="M21 3v18" />
                    </svg>
                    <span className="font-black" style={{ fontWeight: 900 }}>
                      ログアウト
                    </span>
                  </button>
                </div>
              </div>
            </div>
          </div>

        {openSidebarPanel &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
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

          {listAddSourceOpen &&
            typeof document !== "undefined" &&
            createPortal(
              <div
                className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
                onClick={() => setListAddSourceOpen(false)}
              >
                <div className="flex min-h-full items-center justify-center">
                  <div
                    className="flex w-full max-w-[640px] flex-col overflow-hidden rounded-2xl border border-emerald-300/15 bg-gradient-to-br from-[#0b1220]/98 via-[#0f172a]/95 to-[#07101f]/98 shadow-[0_24px_70px_rgba(0,0,0,0.5)] backdrop-blur-xl"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <div className="flex items-center justify-between gap-3 border-b border-white/10 bg-white/[0.03] px-4 py-4">
                      <div>
                        <div className="text-sm font-semibold text-slate-100">
                          リスト追加 項目選択
                        </div>
                        <div className="mt-1 text-xs text-slate-400">
                          追加するリストの取得元を選択してください
                        </div>
                      </div>

                      <button
                        type="button"
                        onClick={() => setListAddSourceOpen(false)}
                        className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                      >
                        ×
                      </button>
                    </div>

                    <div className="px-4 py-5">
                      <SelectionOptionCard
                        tone="emerald"
                        icon="＋"
                        title="マイナビ新卒"
                        description="マイナビ新卒から企業リストを取得して、管理対象に追加します"
                        badge="追加"
                        onClick={handleOpenMynaviYear}
                      />
                    </div>
                  </div>
                </div>
              </div>,
              document.body
            )}

        {mynaviYearOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => setMynaviYearOpen(false)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      マイナビ新卒 年卒選択
                    </div>

                    <button
                      type="button"
                      onClick={() => setMynaviYearOpen(false)}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="px-4 py-6">
                    <select
                      value={selectedMynaviGradYear}
                      onChange={(e) => setSelectedMynaviGradYear(e.target.value)}
                      className="h-11 w-full rounded-xl border border-white/10 bg-[#0f172a] px-3 text-sm text-slate-100 outline-none focus:border-sky-500"
                    >
                      {Array.from({ length: 99 }, (_, index) => {
                        const value = String(index + 1).padStart(2, "0");
                        return (
                          <option key={value} value={value}>
                            {value}
                          </option>
                        );
                      })}
                    </select>
                  </div>

                  <div className="flex justify-end gap-2 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setMynaviYearOpen(false)}
                      disabled={mynaviLoading}
                      className="h-10 min-w-[96px] rounded-xl bg-rose-600 px-5 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleConfirmMynaviGradYear}
                      disabled={mynaviLoading}
                      className="h-10 min-w-[96px] rounded-xl bg-sky-500 px-5 text-sm font-medium text-white transition hover:bg-sky-400 disabled:opacity-50"
                    >
                      はい
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {mynaviPageCountOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => setMynaviPageCountOpen(false)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[560px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      マイナビ新卒 ページ数選択
                    </div>

                    <button
                      type="button"
                      onClick={() => setMynaviPageCountOpen(false)}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="space-y-4 px-4 py-6">
                    <div className="text-sm text-slate-200">
                      全部で {mynaviTotalPages.toLocaleString()} ページあります
                    </div>

                    <select
                      value={selectedMynaviPageCount}
                      onChange={(e) => setSelectedMynaviPageCount(e.target.value)}
                      className="h-11 w-full rounded-xl border border-white/10 bg-[#0f172a] px-3 text-sm text-slate-100 outline-none focus:border-sky-500"
                    >
                      <option value="all">全ページ</option>
                      {mynaviPageCountOptions.map((value) => (
                        <option key={value} value={value}>
                          {value}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setMynaviPageCountOpen(false)}
                      className="h-10 flex-1 rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleStartMynaviJob}
                      disabled={mynaviLoading}
                      className="h-10 flex-1 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400 disabled:opacity-50"
                    >
                      はい
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {mynaviProgressOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]">
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[760px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      マイナビ新卒 進行状況
                    </div>

                    <button
                      type="button"
                      onClick={handleCancelMynaviJob}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="space-y-4 px-4 py-6">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                        <div className="text-xs text-slate-400">年卒</div>
                        <div className="mt-1 text-sm font-semibold text-slate-100">
                          {selectedMynaviGradYear}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                        <div className="text-xs text-slate-400">処理</div>
                        <div className="mt-1 text-sm font-semibold text-slate-100">
                          {getMynaviPhaseLabel(mynaviJobMode, mynaviPhase)}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                        <div className="text-xs text-slate-400">現在ページ</div>
                        <div className="mt-1 text-sm font-semibold text-slate-100">
                          {mynaviCurrentPageNumber > 0
                            ? mynaviCurrentPageNumber.toLocaleString()
                            : "-"}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                        <div className="text-xs text-slate-400">
                          {mynaviJobMode === "count_pages" ? "検出ページ数" : "取得URL数"}
                        </div>
                        <div className="mt-1 text-sm font-semibold text-slate-100">
                          {(mynaviJobMode === "count_pages"
                            ? mynaviDetectedTotalPages
                            : mynaviTotalUrls
                          ).toLocaleString()}
                        </div>
                      </div>
                    </div>

                    {mynaviJobMode === "count_pages" ? (
                      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                        <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                          <div className="text-xs text-slate-400">状況</div>
                          <div className="mt-1 text-sm font-semibold text-slate-100">
                            {getMynaviFieldLabel(mynaviCurrentField)}
                          </div>
                        </div>

                        <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                          <div className="text-xs text-slate-400">全ページ</div>
                          <div className="mt-1 text-sm font-semibold text-slate-100">
                            {mynaviTotalPages > 0 ? mynaviTotalPages.toLocaleString() : "-"}
                          </div>
                        </div>
                      </div>
                    ) : (
                      <>
                        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                          <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                            <div className="text-xs text-slate-400">完了社数</div>
                            <div className="mt-1 text-sm font-semibold text-slate-100">
                              {mynaviProcessedCount.toLocaleString()} / {mynaviTotalUrls.toLocaleString()}
                            </div>
                          </div>

                          <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                            <div className="text-xs text-slate-400">現在社数</div>
                            <div className="mt-1 text-sm font-semibold text-slate-100">
                              {mynaviCurrentCompanyIndex > 0
                                ? `${mynaviCurrentCompanyIndex.toLocaleString()}社目`
                                : "-"}
                            </div>
                          </div>

                          <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                            <div className="text-xs text-slate-400">成功件数</div>
                            <div className="mt-1 text-sm font-semibold text-slate-100">
                              {mynaviSuccessCount.toLocaleString()}
                            </div>
                          </div>

                          <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                            <div className="text-xs text-slate-400">失敗件数</div>
                            <div className="mt-1 text-sm font-semibold text-slate-100">
                              {mynaviFailedCount.toLocaleString()}
                            </div>
                          </div>
                        </div>

                        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                          <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                            <div className="text-xs text-slate-400">現在項目</div>
                            <div className="mt-1 text-sm font-semibold text-slate-100">
                              {getMynaviFieldLabel(mynaviCurrentField)}
                            </div>
                          </div>

                          <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                            <div className="text-xs text-slate-400">現在企業</div>
                            <div className="mt-1 break-all text-sm font-semibold text-slate-100">
                              {mynaviCurrentCompany && mynaviCurrentCompany.trim() !== ""
                                ? mynaviCurrentCompany
                                : "-"}
                            </div>
                          </div>
                        </div>
                      </>
                    )}
                  </div>

                  <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                    {mynaviJobMode === "count_pages" ? (
                      <button
                        type="button"
                        onClick={handleCancelMynaviJob}
                        className="h-10 w-full rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10"
                      >
                        閉じる
                      </button>
                    ) : (
                      <>
                        <button
                          type="button"
                          onClick={handleCancelMynaviJob}
                          className="h-10 flex-1 rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500"
                        >
                          中止
                        </button>

                        <button
                          type="button"
                          onClick={handlePauseMynaviJob}
                          className="h-10 flex-1 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400"
                        >
                          中断
                        </button>
                      </>
                    )}
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {mynaviResultOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]">
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[760px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      マイナビ新卒 結果確認
                    </div>

                    <button
                      type="button"
                      onClick={() => setMynaviResultOpen(false)}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="space-y-4 px-4 py-6">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                        <div className="text-xs text-slate-400">状態</div>
                        <div className="mt-1 text-sm font-semibold text-slate-100">
                          {mynaviJobStatus === "paused" ? "中断" : "完了"}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                        <div className="text-xs text-slate-400">年卒</div>
                        <div className="mt-1 text-sm font-semibold text-slate-100">
                          {selectedMynaviGradYear}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                        <div className="text-xs text-slate-400">全ページ数</div>
                        <div className="mt-1 text-sm font-semibold text-slate-100">
                          {mynaviTotalPages.toLocaleString()}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                        <div className="text-xs text-slate-400">取得対象ページ数</div>
                        <div className="mt-1 text-sm font-semibold text-slate-100">
                          {selectedMynaviPageCount === "all"
                            ? "全ページ"
                            : selectedMynaviPageCount}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                        <div className="text-xs text-slate-400">取得URL数</div>
                        <div className="mt-1 text-sm font-semibold text-slate-100">
                          {mynaviTotalUrls.toLocaleString()}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                        <div className="text-xs text-slate-400">完了社数</div>
                        <div className="mt-1 text-sm font-semibold text-slate-100">
                          {mynaviProcessedCount.toLocaleString()}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                        <div className="text-xs text-slate-400">成功件数</div>
                        <div className="mt-1 text-sm font-semibold text-slate-100">
                          {mynaviSuccessCount.toLocaleString()}
                        </div>
                      </div>

                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-3">
                        <div className="text-xs text-slate-400">失敗件数</div>
                        <div className="mt-1 text-sm font-semibold text-slate-100">
                          {mynaviFailedCount.toLocaleString()}
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setMynaviResultOpen(false)}
                      className="h-10 flex-1 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10"
                    >
                      閉じる
                    </button>

                    {mynaviJobStatus === "paused" && (
                      <button
                        type="button"
                        onClick={handleResumeMynaviJob}
                        className="h-10 flex-1 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400"
                      >
                        再開
                      </button>
                    )}

                    <button
                      type="button"
                      onClick={handleDownloadMynaviCsv}
                      className="h-10 flex-1 rounded-xl bg-emerald-600 px-3 text-sm font-medium text-white transition hover:bg-emerald-500"
                    >
                      CSV抽出
                    </button>
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => closeAdvancedFilterModal()}
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
                        onClick={() => closeAdvancedFilterModal()}
                        className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                      >
                        ×
                      </button>
                    </div>
                  </div>

                  <div className="flex-1 overflow-y-auto px-4 py-4">
                    {advancedLoading &&
                    activeAdvancedFilterKey !== "capital" &&
                    activeAdvancedFilterKey !== "employeeCount"
                      ? null
                      : renderAdvancedFilterContent()}
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

        {itemInspectionFieldOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => {
                if (itemInspecting) return;
                setItemInspectionFieldOpen(false);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="master-data-item-inspection-field-modal flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      項目精査 項目選択
                    </div>

                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() =>
                          setItemInspectionSelections(
                            createVisibleItemInspectionSelections(true)
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
                            createVisibleItemInspectionSelections(false)
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
                      {visibleItemInspectionColumnDefs.map((field) => (
                        <label
                          key={field.key}
                          className="master-data-blue-selection-option flex cursor-pointer items-center gap-3 rounded-xl border border-white/10 bg-white/5 px-3 py-3 text-sm text-slate-200 transition hover:border-cyan-300/40 hover:bg-cyan-500/10 hover:text-cyan-100 [body[data-app-theme='light']_&]:text-black [body[data-app-theme='light']_&:hover]:text-blue-700"
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
                          <span
                            className="app-fit-one-line min-w-0 flex-1"
                            style={{ fontSize: getSingleLineFitFontSize(field.label) }}
                          >
                            {field.label}
                          </span>
                        </label>
                      ))}
                    </div>
                  </div>

                  <div className="flex justify-center gap-3 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setItemInspectionFieldOpen(false)}
                      disabled={itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
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
                          ? "bg-sky-500/30 text-white/60"
                          : "bg-sky-500 hover:bg-sky-400"
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
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
                    <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-4">
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

                      <label className="flex items-center gap-3 rounded-xl border border-white/10 bg-white/5 px-3 py-3 text-sm text-slate-200">
                        <input
                          type="checkbox"
                          checked={
                            itemInspectionMethodSelections.representative_name_inspect_name
                          }
                          onChange={() =>
                            setItemInspectionMethodSelections((prev) => ({
                              ...prev,
                              representative_name_inspect_name:
                                !prev.representative_name_inspect_name,
                            }))
                          }
                          className="h-4 w-4 accent-cyan-500"
                        />
                        <span>氏名を精査</span>
                      </label>
                    </div>
                  </div>

                  <div className="flex justify-center gap-3 border-t border-white/10 px-4 py-4">
                    <button
                      type="button"
                      onClick={() => setItemInspectionMethodOpen(false)}
                      disabled={itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
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
                          ? "bg-sky-500/30 text-white/60"
                          : "bg-sky-500 hover:bg-sky-400"
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

        {itemInspectionProgressOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]">
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

                      <div className="app-progress-bar-frame mt-3 h-3 overflow-hidden rounded-full border border-white/20 bg-white/10">
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
                        <div className="text-xs text-slate-400">精査率</div>
                        <div className="mt-1 text-lg font-semibold text-slate-100">
                          {itemInspectionCandidatePercent}%
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
                      onClick={() => setInspectionCancelConfirmTarget("itemInspectionProgress")}
                      disabled={!itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
                    >
                      中止
                    </button>

                    <button
                      type="button"
                      onClick={handlePauseItemInspection}
                      disabled={!itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400 disabled:opacity-50"
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
            <div className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]">
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      項目精査 結果確認
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

                    {itemInspectionPreviewTab === "excluded" && (
                      <div className="mb-4 rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-slate-300">
                        候補外は確認用の表示です。反映対象にはなりません。
                      </div>
                    )}

                    {visibleItemInspectionPreviewTotalCount === 0 ? (
                      <div className="rounded-xl border border-white/10 bg-white/5 px-4 py-8 text-center text-sm text-slate-400">
                        反映対象はありません
                      </div>
                    ) : (
                      <>
                        <div className="mb-4 border-b border-white/10 pb-4">
                          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                            <div className="text-xs text-slate-400">
                              {itemInspectionPreviewTab === "candidate" ? "精査候補" : "候補外"} {visibleItemInspectionPreviewTotalCount.toLocaleString()}件中{" "}
                              {visibleItemInspectionPreviewTotalCount === 0
                                ? 0
                                : (itemInspectionPreviewPage - 1) *
                                    ITEM_INSPECTION_PREVIEW_PAGE_SIZE +
                                  1}
                              〜
                              {Math.min(
                                itemInspectionPreviewPage * ITEM_INSPECTION_PREVIEW_PAGE_SIZE,
                                visibleItemInspectionPreviewTotalCount
                              )}
                              件を表示
                            </div>

                            <div className="flex items-center gap-2">
                              <button
                                type="button"
                                onClick={() => {
                                  void handleItemInspectionPreviewTabChange("candidate");
                                }}
                                className={`h-8 rounded-lg px-3 text-xs font-medium transition ${
                                  itemInspectionPreviewTab === "candidate"
                                    ? "bg-sky-500 text-white"
                                    : "border border-white/10 bg-white/5 text-slate-200 hover:bg-white/10"
                                }`}
                              >
                                候補
                              </button>

                              <button
                                type="button"
                                onClick={() => {
                                  void handleItemInspectionPreviewTabChange("excluded");
                                }}
                                className={`h-8 rounded-lg px-3 text-xs font-medium transition ${
                                  itemInspectionPreviewTab === "excluded"
                                    ? "bg-sky-500 text-white"
                                    : "border border-white/10 bg-white/5 text-slate-200 hover:bg-white/10"
                                }`}
                              >
                                候補外
                              </button>

                              <button
                                type="button"
                                onClick={() => {
                                  void handleItemInspectionPreviewPageChange(
                                    itemInspectionPreviewPage - 1
                                  );
                                }}
                                disabled={
                                  itemInspectionPreviewPage === 1 || itemInspecting
                                }
                                className="h-8 rounded-lg border border-white/10 bg-white/5 px-3 text-xs text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                              >
                                前へ
                              </button>

                              <div className="text-xs text-slate-300">
                                {itemInspectionPreviewPage} / {itemInspectionPreviewTotalPages}
                              </div>

                              <button
                                type="button"
                                onClick={() => {
                                  void handleItemInspectionPreviewPageChange(
                                    itemInspectionPreviewPage + 1
                                  );
                                }}
                                disabled={
                                  itemInspectionPreviewPage >= itemInspectionPreviewTotalPages ||
                                  itemInspecting
                                }
                                className="h-8 rounded-lg border border-white/10 bg-white/5 px-3 text-xs text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                              >
                                次へ
                              </button>
                            </div>
                          </div>
                        </div>

                        <div
                          ref={itemInspectionPreviewScrollRef}
                          className="grid max-h-[60vh] grid-cols-1 gap-2 overflow-y-auto"
                        >
                          {visibleItemInspectionPreviewChanges.map((row) => (
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
                                    : row.action === "review"
                                    ? "(要確認)"
                                    : row.action === "none"
                                    ? "(候補外)"
                                    : row.afterValue || "-"}
                                </div>

                                <div className="mt-1 text-xs text-cyan-200">
                                  {row.reason}
                                </div>
                              </div>

                              <div className="shrink-0 rounded-lg border border-white/10 bg-[#0f172a] px-2 py-1 text-xs text-slate-200">
                                {row.action === "delete"
                                  ? "削除"
                                  : row.action === "review"
                                  ? "要確認"
                                  : row.action === "none"
                                  ? "候補外"
                                  : "更新"}
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
                      onClick={() => setInspectionCancelConfirmTarget("itemInspectionPreview")}
                      disabled={itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
                    >
                      中止
                    </button>

                    <button
                      type="button"
                      onClick={handleResumeItemInspection}
                      disabled={
                        itemInspecting ||
                        !itemInspectionJobId ||
                        itemInspectionRemainingCount <= 0
                      }
                      className="h-10 w-[120px] flex-none rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400 disabled:opacity-50"
                    >
                      再開
                    </button>

                    <button
                      type="button"
                      onClick={handleApplyItemInspectionChanges}
                      disabled={itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl bg-emerald-500 px-3 text-sm font-medium text-white transition hover:bg-emerald-400 disabled:opacity-50"
                    >
                      {itemInspecting ? <LoadingText label="保存中" /> : "保存"}
                    </button>

                    <button
                      type="button"
                      onClick={() => openPreviewCsvScope("item_inspection")}
                      disabled={itemInspecting}
                      className="h-10 w-[120px] flex-none rounded-xl bg-indigo-500 px-3 text-sm font-medium text-white transition hover:bg-indigo-400 disabled:opacity-50"
                    >
                      CSV抽出
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => {
                setCrawlScopeOpen(false);
                setCrawlTargetScope(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-amber-300/15 bg-gradient-to-br from-[#0b1220]/98 via-[#0f172a]/95 to-[#07101f]/98 shadow-[0_24px_70px_rgba(0,0,0,0.5)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 bg-white/[0.03] px-4 py-4">
                    <div>
                      <div className="text-sm font-semibold text-slate-100">
                        クローリング 対象選択
                      </div>
                      <div className="mt-1 text-xs text-slate-400">
                        クローリングするリスト範囲を選択してください
                      </div>
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

                  <div className="px-4 py-5">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <SelectionOptionCard
                        tone="amber"
                        icon="◎"
                        title="全てのリスト"
                        description="登録されている全リストを対象にします"
                        badge="全件"
                        className="master-data-crawl-scope-all-card"
                        onClick={() => {
                          setCrawlScopeOpen(false);
                          setCrawlTargetScope("all");
                          setCrawlConfirmOpen(true);
                        }}
                      />

                      <SelectionOptionCard
                        tone="sky"
                        icon="🔎"
                        title="絞り込みリストのみ"
                        description="検索・フィルタで絞り込んだ現在のリストだけを対象にします"
                        badge="条件あり"
                        onClick={() => {
                          setCrawlScopeOpen(false);
                          setCrawlTargetScope("filtered");
                          setCrawlConfirmOpen(true);
                        }}
                      />
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => {
                setItemInspectionScopeOpen(false);
                setItemInspectionTargetScope(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-cyan-300/15 bg-gradient-to-br from-[#0b1220]/98 via-[#0f172a]/95 to-[#07101f]/98 shadow-[0_24px_70px_rgba(0,0,0,0.5)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 bg-white/[0.03] px-4 py-4">
                    <div>
                      <div className="text-sm font-semibold text-slate-100">
                        項目精査 対象選択
                      </div>
                      <div className="mt-1 text-xs text-slate-400">
                        項目精査するリスト範囲を選択してください
                      </div>
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

                  <div className="px-4 py-5">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <SelectionOptionCard
                        tone="cyan"
                        icon="◎"
                        title="全てのリスト"
                        description="登録されている全リストを対象にします"
                        badge="全件"
                        onClick={() => {
                          setItemInspectionScopeOpen(false);
                          setItemInspectionTargetScope("all");
                          setItemInspectionFieldOpen(true);
                        }}
                      />

                      <SelectionOptionCard
                        tone="sky"
                        icon="🔎"
                        title="絞り込みリストのみ"
                        description="検索・フィルタで絞り込んだ現在のリストだけを対象にします"
                        badge="条件あり"
                        onClick={() => {
                          setItemInspectionScopeOpen(false);
                          setItemInspectionTargetScope("filtered");
                          setItemInspectionFieldOpen(true);
                        }}
                      />
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => {
                setDedupeScopeOpen(false);
                setDedupeTargetScope(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-violet-300/15 bg-gradient-to-br from-[#0b1220]/98 via-[#0f172a]/95 to-[#07101f]/98 shadow-[0_24px_70px_rgba(0,0,0,0.5)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 bg-white/[0.03] px-4 py-4">
                    <div>
                      <div className="text-sm font-semibold text-slate-100">
                        重複削除 対象選択
                      </div>
                      <div className="mt-1 text-xs text-slate-400">
                        重複削除するリスト範囲を選択してください
                      </div>
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

                  <div className="px-4 py-5">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <SelectionOptionCard
                        tone="violet"
                        icon="◎"
                        title="全てのリスト"
                        description="登録されている全リストを対象にします"
                        badge="全件"
                        className="master-data-dedupe-scope-all-card"
                        onClick={() => {
                          setDedupeScopeOpen(false);
                          setDedupeTargetScope("all");
                          setDedupeSelectedField(null);
                          setDedupeMatchMethod(null);
                          setDedupeFieldOpen(true);
                        }}
                      />

                      <SelectionOptionCard
                        tone="sky"
                        icon="🔎"
                        title="絞り込みリストのみ"
                        description="検索・フィルタで絞り込んだ現在のリストだけを対象にします"
                        badge="条件あり"
                        onClick={() => {
                          setDedupeScopeOpen(false);
                          setDedupeTargetScope("filtered");
                          setDedupeSelectedField(null);
                          setDedupeMatchMethod(null);
                          setDedupeFieldOpen(true);
                        }}
                      />
                    </div>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

                {dedupeFieldOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => {
                setDedupeFieldOpen(false);
                setDedupeMethodOpen(false);
                setDedupeTargetScope(null);
                setDedupeSelectedField(null);
                setDedupeMatchMethod(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-violet-300/15 bg-gradient-to-br from-[#0b1220]/98 via-[#0f172a]/95 to-[#07101f]/98 shadow-[0_24px_70px_rgba(0,0,0,0.5)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 bg-white/[0.03] px-4 py-4">
                    <div>
                      <div className="text-sm font-semibold text-slate-100">
                        重複削除 項目選択
                      </div>
                      <div className="mt-1 text-xs text-slate-400">
                        重複判定に使う項目を選択してください
                      </div>
                    </div>

                    <button
                      type="button"
                      onClick={() => {
                        setDedupeFieldOpen(false);
                        setDedupeMethodOpen(false);
                        setDedupeTargetScope(null);
                        setDedupeSelectedField(null);
                        setDedupeMatchMethod(null);
                      }}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="space-y-5 px-4 py-5">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
                      {COLUMN_DEFS.map((column) => (
                        <button
                          key={column.key}
                          type="button"
                          onClick={() => {
                            setDedupeSelectedField(column.key);
                            setDedupeMatchMethod(null);
                            setDedupeFieldOpen(false);
                            setDedupeMethodOpen(true);
                          }}
                          className="min-h-[58px] rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-left text-sm font-semibold text-slate-200 transition hover:border-violet-300/30 hover:bg-violet-500/10"
                        >
                          {column.label}
                        </button>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

        {dedupeMethodOpen &&
          dedupeSelectedField &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => {
                setDedupeMethodOpen(false);
                setDedupeTargetScope(null);
                setDedupeSelectedField(null);
                setDedupeMatchMethod(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[560px] flex-col overflow-hidden rounded-2xl border border-violet-300/15 bg-gradient-to-br from-[#0b1220]/98 via-[#0f172a]/95 to-[#07101f]/98 shadow-[0_24px_70px_rgba(0,0,0,0.5)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 bg-white/[0.03] px-4 py-4">
                    <div>
                      <div className="text-sm font-semibold text-slate-100">
                        重複削除 方法確認
                      </div>
                      <div className="mt-1 text-xs text-slate-400">
                        {selectedDedupeFieldLabel} の重複判定方法を選択してください
                      </div>
                    </div>

                    <button
                      type="button"
                      onClick={() => {
                        setDedupeMethodOpen(false);
                        setDedupeTargetScope(null);
                        setDedupeSelectedField(null);
                        setDedupeMatchMethod(null);
                      }}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="px-4 py-5">
                    <button
                      type="button"
                      onClick={() => {
                        setDedupeMatchMethod("exact");
                        setDedupeMethodOpen(false);
                        setDedupeConfirmOpen(true);
                      }}
                      className="h-11 w-full rounded-xl bg-sky-500 px-5 text-sm font-medium text-white transition hover:bg-sky-400"
                    >
                      完全一致
                    </button>
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => {
                setItemDeleteScopeOpen(false);
                setItemDeleteTarget(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-cyan-300/15 bg-gradient-to-br from-[#0b1220]/98 via-[#0f172a]/95 to-[#07101f]/98 shadow-[0_24px_70px_rgba(0,0,0,0.5)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 bg-white/[0.03] px-4 py-4">
                    <div>
                      <div className="text-sm font-semibold text-slate-100">
                        項目削除 対象選択
                      </div>
                      <div className="mt-1 text-xs text-slate-400">
                        項目削除するリスト範囲を選択してください
                      </div>
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

                  <div className="px-4 py-5">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <SelectionOptionCard
                        tone="cyan"
                        icon="◎"
                        title="全てのリスト"
                        description="登録されている全リストを対象にします"
                        badge="全件"
                        onClick={() => {
                          setItemDeleteScopeOpen(false);
                          setItemDeleteTarget("all");
                          setItemDeleteFieldOpen(true);
                        }}
                      />

                      <SelectionOptionCard
                        tone="sky"
                        icon="🔎"
                        title="絞り込みリストのみ"
                        description="検索・フィルタで絞り込んだ現在のリストだけを対象にします"
                        badge="条件あり"
                        onClick={() => {
                          setItemDeleteScopeOpen(false);
                          setItemDeleteTarget("filtered");
                          setItemDeleteFieldOpen(true);
                        }}
                      />
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => {
                if (itemDeleting) return;
                setItemDeleteFieldOpen(false);
                setItemDeleteTarget(null);
              }}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="master-data-item-delete-field-modal flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      項目削除 項目選択
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
                          className="master-data-blue-selection-option flex cursor-pointer items-center gap-3 rounded-xl border border-white/10 bg-white/5 px-3 py-3 text-sm text-slate-200 transition hover:border-cyan-300/40 hover:bg-cyan-500/10 hover:text-cyan-100 [body[data-app-theme='light']_&]:text-black [body[data-app-theme='light']_&:hover]:text-blue-700"
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
                      className="h-10 w-[120px] flex-none rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
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
                          ? "bg-sky-500/30 text-white/60"
                          : "bg-sky-500 hover:bg-sky-400"
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
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
                    項目削除 確認
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
                      className="h-10 flex-1 rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleItemDelete}
                      disabled={itemDeleting}
                      className="h-10 flex-1 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400 disabled:opacity-50"
                    >
                      はい
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => setListDeleteScopeOpen(false)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-rose-300/15 bg-gradient-to-br from-[#0b1220]/98 via-[#0f172a]/95 to-[#07101f]/98 shadow-[0_24px_70px_rgba(0,0,0,0.5)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 bg-white/[0.03] px-4 py-4">
                    <div>
                      <div className="text-sm font-semibold text-slate-100">
                        リスト削除 対象選択
                      </div>
                      <div className="mt-1 text-xs text-slate-400">
                        削除するリスト範囲を選択してください
                      </div>
                    </div>

                    <button
                      type="button"
                      onClick={() => setListDeleteScopeOpen(false)}
                      className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                    >
                      ×
                    </button>
                  </div>

                  <div className="px-4 py-5">
                    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                      <SelectionOptionCard
                        tone="rose"
                        icon="◎"
                        title="全てのリスト"
                        description="登録されている全リストを対象にします"
                        badge="全件"
                        className="master-data-list-delete-scope-all-card"
                        onClick={() => {
                          setListDeleteScopeOpen(false);
                          setListDeleteConfirmTarget("all");
                        }}
                      />

                      <SelectionOptionCard
                        tone="sky"
                        icon="🔎"
                        title="絞り込みリストのみ"
                        description="検索・フィルタで絞り込んだ現在のリストだけを対象にします"
                        badge="条件あり"
                        onClick={() => {
                          setListDeleteScopeOpen(false);
                          setListDeleteConfirmTarget("filtered");
                        }}
                      />
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
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
                      className="h-10 flex-1 rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleListDelete}
                      disabled={listDeleting}
                      className="h-10 flex-1 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400 disabled:opacity-50"
                    >
                      はい
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
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
                      className="h-10 flex-1 rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500"
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
            <div className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]">
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

                      <div className="app-progress-bar-frame mt-3 h-3 overflow-hidden rounded-full border border-white/20 bg-white/10">
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

                      <div className="mt-4 grid grid-cols-2 gap-3">
                        <div className="rounded-lg bg-white/5 px-3 py-3">
                          <div className="text-xs text-slate-400">完了</div>
                          <div className="mt-1 text-lg font-semibold text-slate-100">
                            {crawlProcessedCount}
                          </div>
                        </div>

                        <div className="rounded-lg bg-white/5 px-3 py-3">
                          <div className="text-xs text-slate-400">候補率</div>
                          <div className="mt-1 text-lg font-semibold text-slate-100">
                            {crawlCandidatePercent}%
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
                      onClick={() => setInspectionCancelConfirmTarget("crawlProgress")}
                      disabled={!crawlJobId || crawling === false}
                      className="h-10 w-[120px] flex-none rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
                    >
                      中止
                    </button>

                    <button
                      type="button"
                      onClick={handlePauseCrawl}
                      disabled={!crawlJobId || crawlJobStatus !== "running" || crawling === false}
                      className="h-10 w-[120px] flex-none rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400 disabled:opacity-50"
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => !crawling && setCrawlConfirmOpen(false)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="master-data-crawl-field-modal flex w-full max-w-[960px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                    <div className="text-sm font-semibold text-slate-100">
                      クローリング 項目選択
                    </div>

                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={() => setCrawlFieldSelections(createVisibleCrawlFieldSelections(true))}
                        disabled={crawling}
                        className="h-9 rounded-xl border border-white/10 bg-white/5 px-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                      >
                        全選択
                      </button>

                      <button
                        type="button"
                        onClick={() => setCrawlFieldSelections(createVisibleCrawlFieldSelections(false))}
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
                      {visibleCrawlConfirmFieldOptions.map((field) => (
                        <label
                          key={field.key}
                          className="master-data-orange-selection-option flex cursor-pointer items-center gap-3 rounded-xl border border-white/10 bg-white/5 px-3 py-3 text-sm text-slate-200 transition hover:border-amber-300/40 hover:bg-amber-500/10 hover:text-amber-100 [body[data-app-theme='light']_&]:text-black [body[data-app-theme='light']_&:hover]:text-orange-700"
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
                      className="h-10 w-[120px] flex-none rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleCrawl}
                      disabled={crawling || !hasSelectedCrawlFields}
                      className={`h-10 w-[120px] flex-none rounded-xl px-3 text-sm font-medium text-white transition disabled:cursor-not-allowed disabled:opacity-100 ${
                        !hasSelectedCrawlFields
                          ? "bg-sky-500/30 text-white/60"
                          : "bg-sky-500 hover:bg-sky-400"
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

          {crawlPreviewOpen &&
            typeof document !== "undefined" &&
            createPortal(
              <div className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]">
                <div className="flex min-h-full items-center justify-center">
                  <div
                    className="flex w-full max-w-[1500px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
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
                          {crawlPreviewTab === "candidate"
                            ? "保存候補"
                            : crawlPreviewTab === "multiple"
                            ? "複数候補"
                            : "候補外"} {crawlPreviewTotalCount.toLocaleString()}件中{" "}
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
                            onClick={() => void handleChangeCrawlPreviewTab("candidate")}
                            disabled={crawlPreviewLoading}
                            className={`h-8 rounded-lg px-3 text-xs font-medium transition ${
                              crawlPreviewTab === "candidate"
                                ? "bg-sky-500 text-white"
                                : "border border-white/10 bg-white/5 text-slate-200 hover:bg-white/10"
                            }`}
                          >
                            候補
                          </button>

                          <button
                            type="button"
                            onClick={() => void handleChangeCrawlPreviewTab("multiple")}
                            disabled={crawlPreviewLoading}
                            className={`h-8 rounded-lg px-3 text-xs font-medium transition ${
                              crawlPreviewTab === "multiple"
                                ? "bg-sky-500 text-white"
                                : "border border-white/10 bg-white/5 text-slate-200 hover:bg-white/10"
                            }`}
                          >
                            複数候補
                          </button>

                          <button
                            type="button"
                            onClick={() => void handleChangeCrawlPreviewTab("excluded")}
                            disabled={crawlPreviewLoading}
                            className={`h-8 rounded-lg px-3 text-xs font-medium transition ${
                              crawlPreviewTab === "excluded"
                                ? "bg-sky-500 text-white"
                                : "border border-white/10 bg-white/5 text-slate-200 hover:bg-white/10"
                            }`}
                          >
                            候補外
                          </button>

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
                          </div>

                          <button
                            type="button"
                            onClick={() =>
                              void handleCrawlPreviewPageChange(crawlPreviewPage + 1)
                            }
                            disabled={
                              crawlPreviewPage >= crawlPreviewTotalPages || crawlPreviewLoading
                            }
                            className="h-8 rounded-lg border border-white/10 bg-white/5 px-3 text-xs text-slate-200 transition hover:bg-white/10 disabled:opacity-50"
                          >
                            次へ
                          </button>
                        </div>
                      </div>
                    </div>

                    <div
                      ref={crawlPreviewScrollRef}
                      className="max-h-[70vh] overflow-auto px-4 py-4 space-y-4"
                    >
                      {crawlPreviewLoading ? null : crawlPreviewRows.length === 0 ? (
                        <div className="px-4 py-12 text-center text-slate-500">
                          {crawlPreviewTab === "candidate"
                            ? "保存候補はありません"
                            : crawlPreviewTab === "multiple"
                            ? "複数候補はありません"
                            : "候補外はありません"}
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

                            <div className="mt-1 text-xs text-slate-300 break-all">
                              {row.source_row?.address || "-"}
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

                            {row.changes.length > 0 && (
                              <div className="mt-4 space-y-2">
                                <div className="grid min-w-[1120px] grid-cols-[72px_160px_minmax(360px,1fr)_minmax(460px,1.2fr)] gap-2">
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
                                      className="grid min-w-[1120px] grid-cols-[72px_160px_minmax(360px,1fr)_minmax(460px,1.2fr)] gap-2"
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
                                                    className="grid grid-cols-[18px_minmax(0,1fr)] items-start gap-2"
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
                            )}
                          </div>
                        ))
                      )}
                    </div>

                    <div className="flex justify-center gap-3 border-t border-white/10 px-4 py-4">
                      <button
                        type="button"
                        onClick={() => setInspectionCancelConfirmTarget("crawlPreview")}
                        disabled={crawling}
                        className="h-10 w-[120px] flex-none rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
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
                        className="crawl-preview-save-button h-10 w-[120px] flex-none rounded-xl bg-emerald-600 px-3 text-sm font-medium text-white transition hover:bg-emerald-500 disabled:opacity-50"
                      >
                        保存
                      </button>

                      <button
                        type="button"
                        onClick={() => openPreviewCsvScope("crawl")}
                        disabled={crawling}
                        className="crawl-preview-csv-button h-10 w-[120px] flex-none rounded-xl bg-indigo-500 px-3 text-sm font-medium text-white transition hover:bg-indigo-400 disabled:opacity-50"
                      >
                        CSV抽出
                      </button>
                    </div>
                  </div>
                </div>
              </div>,
              document.body
            )}

            {inspectionCancelConfirmTarget &&
              typeof document !== "undefined" &&
              createPortal(
                <div
                  className="app-modal-root fixed inset-0 z-[10020] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
                  onClick={() => setInspectionCancelConfirmTarget(null)}
                >
                  <div className="flex min-h-full items-center justify-center">
                    <div
                      className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                        中止確認
                      </div>

                      <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                        本当に中止しますか？
                        <br />
                        <span className="text-xs text-slate-400">
                          間違って中止しないよう、確認しています。
                        </span>
                      </div>

                      <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                        <button
                          type="button"
                          onClick={() => setInspectionCancelConfirmTarget(null)}
                          className="h-10 flex-1 rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500"
                        >
                          いいえ
                        </button>

                        <button
                          type="button"
                          onClick={handleConfirmInspectionCancel}
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

        {crawlResumeConfirmOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]">
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
                      className="h-10 flex-1 rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleResumeCrawl}
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

        {dedupeConfirmOpen &&
          typeof document !== "undefined" &&
          createPortal(
            <div
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => !deduplicating && setDedupeConfirmOpen(false)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                    重複削除 確認
                  </div>

                  <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                    {selectedDedupeFieldLabel || "選択した項目"}の
                    {selectedDedupeMatchMethodLabel || "選択した条件"}で重複データを削除します。
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
                      className="h-10 flex-1 rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
                    >
                      いいえ
                    </button>

                    <button
                      type="button"
                      onClick={handleDeduplicate}
                      disabled={deduplicating}
                      className="h-10 flex-1 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400 disabled:opacity-50"
                    >
                      はい
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
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
                      className="h-10 flex-1 rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500 disabled:opacity-50"
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
                      className="h-10 flex-1 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400 disabled:opacity-50"
                    >
                      はい
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
              className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
              onClick={() => !importing && setImportDuplicateConfirmOpen(false)}
            >
              <div className="flex min-h-full items-center justify-center">
                <div
                  className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                    CSV投入 重複削除 確認
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
                      重複削除する
                    </button>
                  </div>
                </div>
              </div>
            </div>,
            document.body
          )}

          {previewCsvScopeOpen &&
            typeof document !== "undefined" &&
            createPortal(
              <div
                className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
                onClick={() => {
                  setPreviewCsvScopeOpen(false);
                  setPreviewCsvSource(null);
                }}
              >
                <div className="flex min-h-full items-center justify-center">
                  <div
                    className="flex w-full max-w-[640px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <div className="flex items-center justify-between gap-3 border-b border-white/10 px-4 py-4">
                      <div className="text-sm font-semibold text-slate-100">
                        CSV抽出 対象選択
                      </div>

                      <button
                        type="button"
                        onClick={() => {
                          setPreviewCsvScopeOpen(false);
                          setPreviewCsvSource(null);
                        }}
                        className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                      >
                        ×
                      </button>
                    </div>

                    <div className="px-4 py-6 text-sm leading-7 text-slate-300">
                      CSVを抽出する対象を選択してください。
                      <br />
                      <span className="text-xs text-slate-400">
                        画面:
                        {previewCsvSource === "crawl"
                          ? " クローリング結果確認"
                          : " 項目精査 結果確認"}
                      </span>
                    </div>

                    <div className="grid gap-2 border-t border-white/10 px-4 py-4 sm:grid-cols-2">
                      <button
                        type="button"
                        onClick={() => {
                          setPreviewCsvMode("all");
                          setPreviewCsvScopeOpen(false);
                          setPreviewCsvConfirmOpen(true);
                        }}
                        className="h-10 rounded-xl bg-sky-500 px-3 text-sm font-medium text-white transition hover:bg-sky-400"
                      >
                        全てのリスト
                      </button>

                      <button
                        type="button"
                        onClick={() => {
                          setPreviewCsvMode("candidate");
                          setPreviewCsvScopeOpen(false);
                          setPreviewCsvConfirmOpen(true);
                        }}
                        className="h-10 rounded-xl bg-emerald-500 px-3 text-sm font-medium text-white transition hover:bg-emerald-400"
                      >
                        候補のみリスト
                      </button>
                    </div>
                  </div>
                </div>
              </div>,
              document.body
            )}

          {previewCsvConfirmOpen &&
            typeof document !== "undefined" &&
            createPortal(
              <div
                className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
                onClick={() => setPreviewCsvConfirmOpen(false)}
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
                        画面:
                        {previewCsvSource === "crawl"
                          ? " クローリング結果確認"
                          : " 項目精査 結果確認"}
                      </span>
                      <br />
                      <span className="text-xs text-slate-400">
                        対象:
                        {previewCsvMode === "all"
                          ? " 全てのリスト"
                          : " 候補のみリスト"}
                      </span>
                    </div>

                    <div className="flex gap-2 border-t border-white/10 px-4 py-4">
                      <button
                        type="button"
                        onClick={() => setPreviewCsvConfirmOpen(false)}
                        className="h-10 flex-1 rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500"
                      >
                        いいえ
                      </button>

                      <button
                        type="button"
                        onClick={() => void handlePreviewCsvExport()}
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

          {exportScopeOpen &&
            typeof document !== "undefined" &&
            createPortal(
              <div
                className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
                onClick={() => setExportScopeOpen(false)}
              >
                <div className="flex min-h-full items-center justify-center">
                  <div
                    className="flex w-full max-w-[1080px] flex-col overflow-hidden rounded-2xl border border-sky-300/15 bg-gradient-to-br from-[#0b1220]/98 via-[#0f172a]/95 to-[#07101f]/98 shadow-[0_24px_70px_rgba(0,0,0,0.5)] backdrop-blur-xl"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <div className="flex items-center justify-between gap-3 border-b border-white/10 bg-white/[0.03] px-4 py-4">
                      <div>
                        <div className="text-sm font-semibold text-slate-100">
                          CSV抽出 対象選択
                        </div>
                        <div className="mt-1 text-xs text-slate-400">
                          CSVを抽出するリスト範囲を選択してください
                        </div>
                      </div>

                      <button
                        type="button"
                        onClick={() => setExportScopeOpen(false)}
                        className="inline-flex h-8 w-8 items-center justify-center rounded-lg border border-white/10 bg-white/5 text-slate-300 transition hover:bg-white/10"
                      >
                        ×
                      </button>
                    </div>

                    <div className="px-4 py-5">
                      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                        <SelectionOptionCard
                          tone="sky"
                          icon="◎"
                          title="全てのリスト"
                          description="登録されている全リストをCSVに出力します"
                          badge="全件"
                          onClick={() => {
                            setExportMode("all");
                            setExportScopeOpen(false);
                            setExportConfirmOpen(true);
                          }}
                        />

                        <SelectionOptionCard
                          tone="emerald"
                          icon="🔎"
                          title="絞り込みリストのみ"
                          description="検索・フィルタで絞り込んだ現在のリストだけをCSVに出力します"
                          badge="条件あり"
                          onClick={() => {
                            setExportMode("filtered");
                            setExportScopeOpen(false);
                            setExportConfirmOpen(true);
                          }}
                        />
                      </div>
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
                className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
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
                        className="h-10 flex-1 rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500"
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
                className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
                onClick={() => setExportDuplicateConfirmOpen(false)}
              >
                <div className="flex min-h-full items-center justify-center">
                  <div
                    className="flex w-full max-w-[520px] flex-col overflow-hidden rounded-2xl border border-white/10 bg-[#0b1220]/95 shadow-[0_20px_60px_rgba(0,0,0,0.45)] backdrop-blur-xl"
                    onClick={(e) => e.stopPropagation()}
                  >
                    <div className="border-b border-white/10 px-4 py-4 text-sm font-semibold text-slate-100">
                      CSV抽出 重複削除 確認
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
                  className="app-modal-root fixed inset-0 z-[9999] overflow-y-auto bg-slate-950/70 p-[var(--app-modal-page-pad)]"
                  onClick={() => {
                    setAllFiltersClearConfirmOpen(false);
                    setAllFiltersClearConfirmTarget(null);
                  }}
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
                          onClick={() => {
                            setAllFiltersClearConfirmOpen(false);
                            setAllFiltersClearConfirmTarget(null);
                          }}
                          className="h-10 flex-1 rounded-xl bg-rose-600 px-3 text-sm font-medium text-white transition hover:bg-rose-500"
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

        <div className="flex min-h-0 w-full min-w-0 flex-1 flex-col overflow-hidden border border-white/10 bg-[#0b1326]/90 shadow-[0_24px_60px_rgba(0,0,0,0.35)]">
          <div ref={masterListScrollRef} className="app-scrollbar flex-1 overflow-auto">
            <div className="min-w-[5230px]">
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
                    canShowFilterButton={canShowListColumnFilters}
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

        <div className="mt-[var(--app-gap-lg)] shrink-0 flex flex-wrap items-center justify-center gap-[var(--app-gap-sm)] rounded-2xl border border-white/10 bg-gradient-to-br from-white/8 via-[#0b1326]/85 to-[#08101d]/90 p-2 shadow-[0_18px_44px_rgba(0,0,0,0.22)]">
          <button
            type="button"
            onClick={() => setPage(1)}
            disabled={page <= 1 || limit === "all"}
            className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-sky-300/30 hover:bg-sky-500/10 hover:text-sky-100 disabled:opacity-40"
          >
            最初
          </button>

          <button
            type="button"
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1 || limit === "all"}
            className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-sky-300/30 hover:bg-sky-500/10 hover:text-sky-100 disabled:opacity-40"
          >
            前へ
          </button>

          {pageNumbers[0] > 1 && (
            <span className="px-2 text-sm text-slate-500">...</span>
          )}

          {pageNumbers.map((n) => (
            <button
              key={`main-page-${n}`}
              type="button"
              onClick={() => setPage(n)}
              disabled={limit === "all"}
              className={`rounded-xl border px-4 py-2 text-sm font-semibold transition disabled:opacity-40 ${
                page === n
                  ? "border-sky-400/40 bg-sky-500/20 text-sky-100"
                  : "border-white/10 bg-white/5 text-slate-200 hover:border-sky-300/30 hover:bg-sky-500/10 hover:text-sky-100"
              }`}
            >
              {n}
            </button>
          ))}

          {pageNumbers[pageNumbers.length - 1] < totalPages && (
            <span className="px-2 text-sm text-slate-500">...</span>
          )}

          <button
            type="button"
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page >= totalPages || limit === "all"}
            className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-sky-300/30 hover:bg-sky-500/10 hover:text-sky-100 disabled:opacity-40"
          >
            次へ
          </button>

          <button
            type="button"
            onClick={() => setPage(totalPages)}
            disabled={page >= totalPages || limit === "all"}
            className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-semibold text-slate-200 transition hover:border-sky-300/30 hover:bg-sky-500/10 hover:text-sky-100 disabled:opacity-40"
          >
            最後
          </button>
        </div>
      </div>
      )}

      {/* テーマ：ライト */}
      <style jsx global>{`
        main[data-theme="light"] {
          background: #f8fafc;
          color: #0f172a;
        }

        body[data-app-theme="light"] {
          color: #0f172a;
        }

        .app-responsive-root {
          --app-sidebar-left: 24px;
          --app-sidebar-gap: 8px;
          --app-sidebar-width: 220px;
          --app-content-left: 248px;
          --app-logo-width: 340px;
          --app-sidebar-menu-offset: 190px;

          --app-page-x: 8px;
          --app-page-y: 12px;
          --app-panel-pad-xs: 6px;
          --app-panel-pad: 12px;
          --app-panel-pad-lg: 16px;
          --app-card-x: 12px;
          --app-card-y: 12px;
          --app-gap-sm: 8px;
          --app-gap-md: 16px;
          --app-gap-lg: 20px;
          --app-radius-lg: 28px;
          --app-radius-md: 24px;
          --app-stat-card-h: 72px;
          --app-menu-icon-size: 40px;
          --app-fit-font: clamp(11px, 0.82vw, 14px);
          --app-fit-font-sm: clamp(10px, 0.72vw, 12px);
          --app-fit-font-lg: clamp(15px, 1.3vw, 20px);
        }

        .app-responsive-root.app-sidebar-closed {
          --app-sidebar-width: 92px;
          --app-content-left: 120px;
          --app-logo-width: 280px;
        }

        .app-responsive-root button,
        .app-responsive-root label,
        .app-responsive-root input,
        .app-responsive-root select {
          font-size: var(--app-fit-font);
        }

        .app-responsive-root button span,
        .app-responsive-root label span {
          text-overflow: clip;
        }

        .app-responsive-root button,
        .app-responsive-root label {
          min-width: 0;
        }

        .app-responsive-root [class*="text-sm"] {
          font-size: var(--app-fit-font);
        }

        .app-responsive-root [class*="text-xs"] {
          font-size: var(--app-fit-font-sm);
        }

        .app-responsive-root [class*="text-lg"],
        .app-responsive-root [class*="text-xl"] {
          font-size: var(--app-fit-font-lg);
        }

        .app-modal-root {
          --app-modal-page-pad: clamp(6px, 1.2vw, 20px);
          --app-modal-panel-pad-x: clamp(10px, 1.2vw, 16px);
          --app-modal-panel-pad-y: clamp(10px, 1.2vw, 16px);
          --app-modal-font: clamp(12px, 0.95vw, 15px);
          --app-modal-font-sm: clamp(11px, 0.85vw, 13px);
          --app-modal-font-lg: clamp(15px, 1.2vw, 20px);
          --app-modal-radius: clamp(12px, 1.1vw, 16px);
          --app-modal-control-h: clamp(38px, 3.6vw, 44px);
          --app-modal-close-size: clamp(28px, 2.8vw, 32px);
          --app-modal-grid-min: 260px;
        }

        .app-modal-root,
        .app-modal-root * {
          box-sizing: border-box;
        }

        .app-modal-root > div {
          min-width: 0;
          padding: 0;
          align-items: center !important;
          justify-content: center !important;
        }

        .app-modal-root > div > div {
          min-width: 0 !important;
          max-height: calc(100dvh - var(--app-modal-page-pad) * 2) !important;
          overflow: auto !important;
          border-radius: var(--app-modal-radius) !important;
        }

        .app-modal-root > div > div > div {
          min-width: 0;
        }

        .app-modal-root > div > div > div:first-child,
        .app-modal-root > div > div > div:last-child {
          padding: var(--app-modal-panel-pad-y) var(--app-modal-panel-pad-x) !important;
        }

        .app-modal-root button,
        .app-modal-root label,
        .app-modal-root input,
        .app-modal-root select,
        .app-modal-root textarea {
          min-width: 0;
          font-size: var(--app-modal-font) !important;
          line-height: 1.25 !important;
        }

        .app-modal-root [class*="text-sm"] {
          font-size: var(--app-modal-font) !important;
        }

        .app-modal-root [class*="text-xs"] {
          font-size: var(--app-modal-font-sm) !important;
        }

        .app-modal-root [class*="text-lg"],
        .app-modal-root [class*="text-xl"] {
          font-size: var(--app-modal-font-lg) !important;
        }

        .app-modal-root button,
        .app-modal-root label {
          white-space: nowrap !important;
          overflow-wrap: normal !important;
          word-break: keep-all !important;
        }

        .app-modal-root button span,
        .app-modal-root label span,
        .app-modal-root .truncate,
        .app-modal-root [class*="truncate"],
        .app-modal-root .whitespace-nowrap,
        .app-modal-root [class*="whitespace-nowrap"] {
          overflow: visible !important;
          text-overflow: clip !important;
          white-space: nowrap !important;
          overflow-wrap: normal !important;
          word-break: keep-all !important;
        }

        .app-modal-root .app-fit-one-line {
          display: block;
          min-width: 0;
          max-width: 100%;
          white-space: nowrap !important;
          overflow: visible !important;
          text-overflow: clip !important;
          line-height: 1.1 !important;
        }

        .app-table-cell,
        .app-modal-root .app-table-cell {
          display: block !important;
          min-width: 0 !important;
          max-width: 100% !important;
          overflow: hidden !important;
          text-overflow: ellipsis !important;
          white-space: nowrap !important;
          overflow-wrap: normal !important;
          word-break: normal !important;
        }

        .app-modal-root .flex {
          min-width: 0;
        }

        .app-modal-root .items-center.justify-between {
          flex-wrap: nowrap;
        }

        .app-modal-root .grid[class*="grid-cols-"] {
          grid-template-columns: repeat(
            auto-fit,
            minmax(min(100%, var(--app-modal-grid-min)), 1fr)
          ) !important;
        }

        .app-modal-root .grid.grid-cols-3 {
          grid-template-columns: repeat(3, minmax(0, 1fr)) !important;
        }

        .app-modal-root .grid.grid-cols-2 {
          grid-template-columns: repeat(2, minmax(0, 1fr)) !important;
        }

        .app-modal-root .h-11,
        .app-modal-root .h-10,
        .app-modal-root .h-9 {
          min-height: var(--app-modal-control-h) !important;
          height: auto !important;
        }

        .app-modal-root .h-8.w-8,
        .app-modal-root .h-7.w-7 {
          width: var(--app-modal-close-size) !important;
          height: var(--app-modal-close-size) !important;
          min-width: var(--app-modal-close-size) !important;
          min-height: var(--app-modal-close-size) !important;
          flex-shrink: 0;
        }

        .app-modal-root .px-5,
        .app-modal-root .px-4,
        .app-modal-root .px-3 {
          padding-left: var(--app-modal-panel-pad-x) !important;
          padding-right: var(--app-modal-panel-pad-x) !important;
        }

        .app-modal-root .py-6,
        .app-modal-root .py-4,
        .app-modal-root .py-3 {
          padding-top: var(--app-modal-panel-pad-y) !important;
          padding-bottom: var(--app-modal-panel-pad-y) !important;
        }

        .app-modal-root .rounded-2xl,
        .app-modal-root .rounded-xl {
          border-radius: var(--app-modal-radius) !important;
        }

        @media (max-width: 900px) {
          .app-modal-root {
            --app-modal-page-pad: 8px;
            --app-modal-panel-pad-x: 10px;
            --app-modal-panel-pad-y: 10px;
            --app-modal-font: clamp(11px, 1.15vw, 14px);
            --app-modal-font-sm: clamp(10px, 1vw, 12px);
            --app-modal-font-lg: clamp(14px, 1.45vw, 18px);
            --app-modal-control-h: 38px;
            --app-modal-close-size: 28px;
            --app-modal-grid-min: 240px;
          }
        }

        @media (max-width: 700px) {
          .app-modal-root {
            --app-modal-page-pad: 6px;
            --app-modal-panel-pad-x: 8px;
            --app-modal-panel-pad-y: 8px;
            --app-modal-font: clamp(10px, 1.35vw, 13px);
            --app-modal-font-sm: clamp(9px, 1.15vw, 11px);
            --app-modal-font-lg: clamp(13px, 1.65vw, 16px);
            --app-modal-control-h: 36px;
            --app-modal-close-size: 26px;
            --app-modal-grid-min: 220px;
          }

          .app-modal-root > div {
            align-items: center !important;
            justify-content: center !important;
          }
        }

        @media (max-height: 700px) {
          .app-modal-root > div {
            align-items: center !important;
            justify-content: center !important;
          }
        }

        @media (max-width: 1280px) {
          .app-responsive-root.app-sidebar-open {
            --app-sidebar-left: 16px;
            --app-sidebar-width: 200px;
            --app-content-left: 224px;
            --app-logo-width: 300px;
            --app-sidebar-menu-offset: 176px;
            --app-panel-pad: 10px;
            --app-panel-pad-lg: 14px;
            --app-card-x: 10px;
            --app-card-y: 10px;
            --app-gap-md: 14px;
            --app-gap-lg: 16px;
          }

          .app-responsive-root.app-sidebar-closed {
            --app-sidebar-left: 16px;
            --app-sidebar-width: 84px;
            --app-content-left: 108px;
            --app-logo-width: 240px;
            --app-sidebar-menu-offset: 160px;
          }
        }

        @media (max-width: 1100px) {
          .app-responsive-root.app-sidebar-open {
            --app-sidebar-left: 10px;
            --app-sidebar-width: 176px;
            --app-content-left: 194px;
            --app-logo-width: 260px;
            --app-sidebar-menu-offset: 150px;
            --app-page-x: 6px;
            --app-page-y: 8px;
            --app-panel-pad-xs: 5px;
            --app-panel-pad: 8px;
            --app-panel-pad-lg: 10px;
            --app-card-x: 8px;
            --app-card-y: 8px;
            --app-radius-lg: 22px;
            --app-radius-md: 18px;
            --app-stat-card-h: 60px;
            --app-menu-icon-size: 36px;
            --app-fit-font: clamp(10px, 0.9vw, 13px);
            --app-fit-font-sm: clamp(9px, 0.78vw, 11px);
            --app-fit-font-lg: clamp(13px, 1.25vw, 17px);
          }

          .app-responsive-root.app-sidebar-closed {
            --app-sidebar-left: 10px;
            --app-sidebar-width: 76px;
            --app-content-left: 92px;
            --app-logo-width: 210px;
            --app-sidebar-menu-offset: 140px;
          }
        }

        @media (max-width: 900px) {
          .app-responsive-root.app-sidebar-open {
            --app-sidebar-left: 6px;
            --app-sidebar-width: 152px;
            --app-content-left: 164px;
            --app-logo-width: 220px;
            --app-sidebar-menu-offset: 126px;
            --app-page-x: 4px;
            --app-page-y: 6px;
            --app-panel-pad-xs: 4px;
            --app-panel-pad: 6px;
            --app-panel-pad-lg: 8px;
            --app-card-x: 6px;
            --app-card-y: 7px;
            --app-gap-sm: 6px;
            --app-gap-md: 10px;
            --app-gap-lg: 12px;
            --app-radius-lg: 18px;
            --app-radius-md: 15px;
            --app-stat-card-h: 54px;
            --app-menu-icon-size: 32px;
            --app-fit-font: clamp(9px, 1.05vw, 12px);
            --app-fit-font-sm: clamp(8px, 0.9vw, 10px);
            --app-fit-font-lg: clamp(12px, 1.45vw, 15px);
          }

          .app-responsive-root.app-sidebar-closed {
            --app-sidebar-left: 6px;
            --app-sidebar-width: 68px;
            --app-content-left: 78px;
            --app-logo-width: 180px;
            --app-sidebar-menu-offset: 118px;
          }

          .app-responsive-root .master-data-brand-title {
            font-size: clamp(22px, 3vw, 30px) !important;
          }
        }

        @media (max-width: 760px) {
          .app-responsive-root.app-sidebar-open {
            --app-sidebar-left: 4px;
            --app-sidebar-width: 134px;
            --app-content-left: 144px;
            --app-logo-width: 190px;
            --app-sidebar-menu-offset: 110px;
            --app-stat-card-h: 48px;
            --app-menu-icon-size: 28px;
            --app-fit-font: clamp(8px, 1.18vw, 11px);
            --app-fit-font-sm: clamp(7px, 1vw, 9px);
            --app-fit-font-lg: clamp(11px, 1.6vw, 14px);
          }

          .app-responsive-root.app-sidebar-closed {
            --app-sidebar-left: 4px;
            --app-sidebar-width: 60px;
            --app-content-left: 68px;
            --app-logo-width: 150px;
            --app-sidebar-menu-offset: 100px;
          }
        }

        .app-scrollbar,
        html,
        body,
        * {
          scrollbar-width: thin;
          scrollbar-color: rgba(56, 189, 248, 0.55) rgba(8, 16, 29, 0.45);
        }

        .app-scrollbar::-webkit-scrollbar,
        *::-webkit-scrollbar {
          width: 10px;
          height: 10px;
        }

        .app-scrollbar::-webkit-scrollbar-track,
        *::-webkit-scrollbar-track {
          background: rgba(8, 16, 29, 0.55);
          border-radius: 999px;
        }

        .app-scrollbar::-webkit-scrollbar-thumb,
        *::-webkit-scrollbar-thumb {
          background: linear-gradient(
            180deg,
            rgba(56, 189, 248, 0.72),
            rgba(148, 163, 184, 0.42)
          );
          border: 2px solid rgba(8, 16, 29, 0.85);
          border-radius: 999px;
        }

        .app-scrollbar::-webkit-scrollbar-thumb:hover,
        *::-webkit-scrollbar-thumb:hover {
          background: linear-gradient(
            180deg,
            rgba(125, 211, 252, 0.9),
            rgba(56, 189, 248, 0.58)
          );
        }

        main[data-theme="light"] .app-scrollbar,
        body[data-app-theme="light"] .app-scrollbar,
        body[data-app-theme="light"] * {
          scrollbar-color: rgba(14, 165, 233, 0.55) rgba(226, 232, 240, 0.85);
        }

        main[data-theme="light"] .app-scrollbar::-webkit-scrollbar-track,
        body[data-app-theme="light"] .app-scrollbar::-webkit-scrollbar-track,
        body[data-app-theme="light"] *::-webkit-scrollbar-track {
          background: rgba(226, 232, 240, 0.85);
        }

        main[data-theme="light"] .app-scrollbar::-webkit-scrollbar-thumb,
        body[data-app-theme="light"] .app-scrollbar::-webkit-scrollbar-thumb,
        body[data-app-theme="light"] *::-webkit-scrollbar-thumb {
          background: linear-gradient(
            180deg,
            rgba(14, 165, 233, 0.68),
            rgba(100, 116, 139, 0.35)
          );
          border: 2px solid rgba(248, 250, 252, 0.95);
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

        main[data-theme="light"] button[class~="bg-sky-500"],
        body[data-app-theme="light"] button[class~="bg-sky-500"],
        main[data-theme="light"] button[class~="bg-emerald-500"],
        body[data-app-theme="light"] button[class~="bg-emerald-500"],
        main[data-theme="light"] button[class~="bg-amber-500"],
        body[data-app-theme="light"] button[class~="bg-amber-500"],
        main[data-theme="light"] button[class~="bg-rose-500"],
        body[data-app-theme="light"] button[class~="bg-rose-500"] {
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

        /* ライトモード専用：暗いグラデーションUIを見やすく補正 */
        main[data-theme="light"] [class*="bg-gradient-to-br"]:not([class*="bg-clip-text"]),
        body[data-app-theme="light"] [class*="bg-gradient-to-br"]:not([class*="bg-clip-text"]),
        main[data-theme="light"] [class*="bg-gradient-to-b"]:not([class*="bg-clip-text"]),
        body[data-app-theme="light"] [class*="bg-gradient-to-b"]:not([class*="bg-clip-text"]) {
          background-color: #ffffff !important;
          background-image: linear-gradient(
            135deg,
            rgba(255, 255, 255, 0.98),
            rgba(248, 250, 252, 0.96),
            rgba(226, 232, 240, 0.88)
          ) !important;
          border-color: rgba(15, 23, 42, 0.14) !important;
          box-shadow: 0 14px 34px rgba(15, 23, 42, 0.10) !important;
        }

        main[data-theme="light"] [class*="bg-gradient-to-r"]:not([class*="bg-clip-text"]),
        body[data-app-theme="light"] [class*="bg-gradient-to-r"]:not([class*="bg-clip-text"]) {
          background-color: #ffffff !important;
          background-image: linear-gradient(
            90deg,
            rgba(255, 255, 255, 0.98),
            rgba(241, 245, 249, 0.96),
            rgba(226, 232, 240, 0.88)
          ) !important;
          border-color: rgba(15, 23, 42, 0.14) !important;
        }

        main[data-theme="light"] [class*="bg-[#07111f]"],
        body[data-app-theme="light"] [class*="bg-[#07111f]"],
        main[data-theme="light"] [class*="bg-[#05070d]"],
        body[data-app-theme="light"] [class*="bg-[#05070d]"],
        main[data-theme="light"] [class*="bg-[#050b14]"],
        body[data-app-theme="light"] [class*="bg-[#050b14]"] {
          background: #ffffff !important;
        }

        /* ライトモード専用：テーマの「ダーク」マークを見やすくする */
        main[data-theme="light"] .theme-dark-mode-icon,
        body[data-app-theme="light"] .theme-dark-mode-icon {
          background: #0f172a !important;
          border-color: rgba(15, 23, 42, 0.45) !important;
          color: #ffffff !important;
        }

        /* ライトモード専用：サイドメニュー・カード・ポップアップ内の色文字を濃くする */
        main[data-theme="light"] [class*="text-sky-100"],
        body[data-app-theme="light"] [class*="text-sky-100"] {
          color: #075985 !important;
        }

        main[data-theme="light"] [class*="text-cyan-100"],
        body[data-app-theme="light"] [class*="text-cyan-100"] {
          color: #0e7490 !important;
        }

        main[data-theme="light"] [class*="text-emerald-100"],
        body[data-app-theme="light"] [class*="text-emerald-100"] {
          color: #047857 !important;
        }

        main[data-theme="light"] [class*="text-amber-100"],
        body[data-app-theme="light"] [class*="text-amber-100"] {
          color: #92400e !important;
        }

        main[data-theme="light"] [class*="text-rose-100"],
        body[data-app-theme="light"] [class*="text-rose-100"] {
          color: #be123c !important;
        }

        main[data-theme="light"] [class*="text-indigo-100"],
        body[data-app-theme="light"] [class*="text-indigo-100"] {
          color: #4338ca !important;
        }

        main[data-theme="light"] [class*="text-violet-100"],
        body[data-app-theme="light"] [class*="text-violet-100"] {
          color: #6d28d9 !important;
        }

        /* ライトモード専用：NAVIGATIONの文字を白飛びしない色にする */
        main[data-theme="light"] .master-data-brand-logo__wordmark[class*="bg-gradient-to-r"][class*="text-transparent"],
        body[data-app-theme="light"] .master-data-brand-logo__wordmark[class*="bg-gradient-to-r"][class*="text-transparent"] {
          background-image: linear-gradient(90deg, #0f172a, #2563eb, #0e7490) !important;
          color: transparent !important;
          -webkit-text-stroke: 0.28px rgba(15, 23, 42, 0.35) !important;
          text-shadow:
            0 1px 0 rgba(255, 255, 255, 0.90),
            0 0 12px rgba(14, 165, 233, 0.22) !important;
        }

        /* ライトモード専用：ページ番号の選択中ボタンは青で分かりやすく残す */
        main[data-theme="light"] button[class*="from-sky-500"][class*="to-cyan-500"],
        body[data-app-theme="light"] button[class*="from-sky-500"][class*="to-cyan-500"] {
          background-color: #0284c7 !important;
          background-image: linear-gradient(135deg, #0284c7, #06b6d4) !important;
          color: #ffffff !important;
          border-color: rgba(2, 132, 199, 0.45) !important;
          box-shadow: 0 0 22px rgba(14, 165, 233, 0.24) !important;
        }

        /* ライトモード専用：表示件数ドロップダウンの選択中ボタン */
        main[data-theme="light"] button[class*="from-emerald-400"][class*="to-cyan-400"],
        body[data-app-theme="light"] button[class*="from-emerald-400"][class*="to-cyan-400"] {
          background-color: #14b8a6 !important;
          background-image: linear-gradient(90deg, #34d399, #22d3ee) !important;
          color: #03131f !important;
          border-color: rgba(20, 184, 166, 0.45) !important;
        }

        /* ライトモード専用：現在ページ・表示件数ドロップダウンの選択中ボタン */
        main[data-theme="light"] button[class*="from-emerald-400"][class*="to-cyan-400"],
        body[data-app-theme="light"] button[class*="from-emerald-400"][class*="to-cyan-400"],
        main[data-theme="light"] button[class*="from-indigo-400"][class*="to-cyan-400"],
        body[data-app-theme="light"] button[class*="from-indigo-400"][class*="to-cyan-400"] {
          background-color: #99f6e4 !important;
          background-image: linear-gradient(90deg, #a7f3d0, #a5f3fc) !important;
          color: #064e3b !important;
          border-color: rgba(20, 184, 166, 0.22) !important;
          box-shadow: 0 0 14px rgba(45, 212, 191, 0.14) !important;
        }

        /* ライトモード専用：現在ページドロップダウンを表示件数ドロップダウンと同じ色にする */
        main[data-theme="light"] button[class*="from-indigo-400"][class*="to-cyan-400"],
        body[data-app-theme="light"] button[class*="from-indigo-400"][class*="to-cyan-400"] {
          background-color: #14b8a6 !important;
          background-image: linear-gradient(90deg, #34d399, #22d3ee) !important;
          color: #03131f !important;
          border-color: rgba(20, 184, 166, 0.45) !important;
          box-shadow: 0 0 18px rgba(45, 212, 191, 0.24) !important;
        }

        /* ライトモード専用：現在ページドロップダウンを表示件数ドロップダウンと同じ薄い色にする */
        main[data-theme="light"] button[class*="from-indigo-400"][class*="to-cyan-400"],
        body[data-app-theme="light"] button[class*="from-indigo-400"][class*="to-cyan-400"] {
          background-color: #99f6e4 !important;
          background-image: linear-gradient(90deg, #a7f3d0, #a5f3fc) !important;
          color: #064e3b !important;
          border-color: rgba(20, 184, 166, 0.22) !important;
          box-shadow: 0 0 14px rgba(45, 212, 191, 0.14) !important;
        }

        main[data-theme="light"] button[class*="hover:text-indigo-100"],
        body[data-app-theme="light"] button[class*="hover:text-indigo-100"] {
          color: #047857 !important;
        }

        main[data-theme="light"] button[class*="hover:bg-indigo-400/12"]:hover,
        body[data-app-theme="light"] button[class*="hover:bg-indigo-400/12"]:hover {
          background-color: rgba(52, 211, 153, 0.12) !important;
          color: #047857 !important;
        }

        main[data-theme="light"] [class*="border-indigo-300/25"][class*="bg-[#07111f]/98"],
        body[data-app-theme="light"] [class*="border-indigo-300/25"][class*="bg-[#07111f]/98"] {
          border-color: rgba(20, 184, 166, 0.25) !important;
        }

        /* ライトモード専用：表示件数ドロップダウン本体 */
        main[data-theme="light"] [class*="bg-[#07111f]/98"],
        body[data-app-theme="light"] [class*="bg-[#07111f]/98"] {
          background: rgba(255, 255, 255, 0.98) !important;
          border-color: rgba(15, 23, 42, 0.14) !important;
          box-shadow: 0 18px 44px rgba(15, 23, 42, 0.18) !important;
        }

        /* ライトモード専用：モーダル内の見出し・入力欄・候補リストの背景を明るくする */
        body[data-app-theme="light"] .app-modal-root > div > div {
          background: rgba(255, 255, 255, 0.98) !important;
          border-color: rgba(15, 23, 42, 0.14) !important;
          box-shadow: 0 24px 70px rgba(15, 23, 42, 0.18) !important;
        }

        body[data-app-theme="light"] .app-modal-root input,
        body[data-app-theme="light"] .app-modal-root select,
        body[data-app-theme="light"] .app-modal-root textarea {
          background: #ffffff !important;
          color: #0f172a !important;
          border-color: rgba(15, 23, 42, 0.16) !important;
        }

        body[data-app-theme="light"] .app-modal-root input::placeholder,
        body[data-app-theme="light"] .app-modal-root textarea::placeholder {
          color: #94a3b8 !important;
        }

        /* ライトモード専用：ホバー時も白飛び・黒つぶれしないようにする */
        main[data-theme="light"] [class*="hover:bg-sky-500/10"]:hover,
        body[data-app-theme="light"] [class*="hover:bg-sky-500/10"]:hover,
        main[data-theme="light"] [class*="hover:bg-emerald-500/10"]:hover,
        body[data-app-theme="light"] [class*="hover:bg-emerald-500/10"]:hover,
        main[data-theme="light"] [class*="hover:bg-rose-500/10"]:hover,
        body[data-app-theme="light"] [class*="hover:bg-rose-500/10"]:hover,
        main[data-theme="light"] [class*="hover:bg-cyan-500/10"]:hover,
        body[data-app-theme="light"] [class*="hover:bg-cyan-500/10"]:hover,
        main[data-theme="light"] [class*="hover:bg-violet-500/10"]:hover,
        body[data-app-theme="light"] [class*="hover:bg-violet-500/10"]:hover {
          background-color: rgba(14, 165, 233, 0.08) !important;
        }

        /* ライトモード専用：表示件数の数字を黒くする */
        main[data-theme="light"] button[class*="group/size"],
        body[data-app-theme="light"] button[class*="group/size"],
        main[data-theme="light"] button[class*="group/size"] *,
        body[data-app-theme="light"] button[class*="group/size"] * {
          color: #0f172a !important;
        }

        /* ライトモード専用：ログアウト文字・矢印を黒くする */
        main[data-theme="light"] button[class*="from-rose-500/18"][class*="hover:bg-rose-500/10"],
        body[data-app-theme="light"] button[class*="from-rose-500/18"][class*="hover:bg-rose-500/10"],
        main[data-theme="light"] button[class*="from-rose-500/18"][class*="hover:bg-rose-500/10"] *,
        body[data-app-theme="light"] button[class*="from-rose-500/18"][class*="hover:bg-rose-500/10"] * {
          color: #0f172a !important;
          stroke: currentColor !important;
        }

        /* ライトモード専用：NAVIGATIONを黒くする */
        main[data-theme="light"] .master-data-brand-logo__wordmark[class*="text-transparent"],
        body[data-app-theme="light"] .master-data-brand-logo__wordmark[class*="text-transparent"] {
          background-image: none !important;
          color: #0f172a !important;
          -webkit-text-fill-color: #0f172a !important;
          -webkit-text-stroke: 0 !important;
          text-shadow: none !important;
        }

        /* ライトモード専用：左メニューの折り畳みボタンを黒くする */
        main[data-theme="light"] button[title="メニューを閉じる"],
        body[data-app-theme="light"] button[title="メニューを閉じる"],
        main[data-theme="light"] button[title="メニューを開く"],
        body[data-app-theme="light"] button[title="メニューを開く"] {
          color: #0f172a !important;
        }

        /* ライトモード専用：左メニュー項目のホバーを見えるようにする */
        main[data-theme="light"] .master-data-sidebar-menu-button[class*="hover:bg-white/10"]:hover,
        body[data-app-theme="light"] .master-data-sidebar-menu-button[class*="hover:bg-white/10"]:hover {
          background-color: #e0f2fe !important;
          background-image: linear-gradient(
            135deg,
            rgba(224, 242, 254, 0.98),
            rgba(219, 234, 254, 0.96),
            rgba(186, 230, 253, 0.88)
          ) !important;
          border-color: rgba(2, 132, 199, 0.42) !important;
          color: #075985 !important;
          box-shadow: 0 10px 24px rgba(14, 165, 233, 0.16) !important;
        }

        main[data-theme="light"] .master-data-sidebar-menu-button[class*="hover:bg-white/10"]:hover .master-data-sidebar-menu-icon,
        body[data-app-theme="light"] .master-data-sidebar-menu-button[class*="hover:bg-white/10"]:hover .master-data-sidebar-menu-icon {
          background: rgba(14, 165, 233, 0.16) !important;
          border-color: rgba(2, 132, 199, 0.42) !important;
          color: #075985 !important;
        }

        /* ライトモード専用：進捗バーの外枠を見えるようにする */
        main[data-theme="light"] .app-progress-bar-frame,
        body[data-app-theme="light"] .app-progress-bar-frame {
          border-color: rgba(15, 23, 42, 0.45) !important;
          background: rgba(15, 23, 42, 0.08) !important;
        }

        /* ライトモード専用：フィルタ解除の文字・マークを黒くする */
        main[data-theme="light"] button[class*="from-rose-500/14"],
        body[data-app-theme="light"] button[class*="from-rose-500/14"],
        main[data-theme="light"] button[class*="from-rose-500/14"] *,
        body[data-app-theme="light"] button[class*="from-rose-500/14"] * {
          color: #0f172a !important;
        }

        /* ライトモード専用：CSV抽出アイコンをCSV投入と同じくらい見やすくする */
        main[data-theme="light"] [class*="text-teal-100"],
        body[data-app-theme="light"] [class*="text-teal-100"] {
          color: #047857 !important;
        }

        /* ライトモード専用：確認ポップアップの「いいえ / はい」だけ白文字に戻す */
        main[data-theme="light"] button[class~="bg-rose-600"],
        body[data-app-theme="light"] button[class~="bg-rose-600"],
        main[data-theme="light"] button[class~="bg-rose-600"] *,
        body[data-app-theme="light"] button[class~="bg-rose-600"] *,
        main[data-theme="light"] button[class~="bg-sky-500"],
        body[data-app-theme="light"] button[class~="bg-sky-500"],
        main[data-theme="light"] button[class~="bg-sky-500"] *,
        body[data-app-theme="light"] button[class~="bg-sky-500"] * {
          color: #ffffff !important;
        }

      /* ライトモード専用：クローリング結果確認の「保存」「CSV抽出」ボタン文字だけ白にする */
        main[data-theme="light"] .crawl-preview-save-button,
        body[data-app-theme="light"] .crawl-preview-save-button,
        main[data-theme="light"] .crawl-preview-save-button span,
        body[data-app-theme="light"] .crawl-preview-save-button span,
        main[data-theme="light"] .crawl-preview-csv-button,
        body[data-app-theme="light"] .crawl-preview-csv-button,
        main[data-theme="light"] .crawl-preview-csv-button span,
        body[data-app-theme="light"] .crawl-preview-csv-button span {
          color: #ffffff !important;
        }

        /* ライトモード専用：左メニューの「フィルタ解除」マーク背景だけ白にする */
        main[data-theme="light"] button[class*="from-rose-500/14"] > span:first-child,
        body[data-app-theme="light"] button[class*="from-rose-500/14"] > span:first-child {
          background: #ffffff !important;
          background-image: none !important;
          border-color: rgba(15, 23, 42, 0.14) !important;
          color: #0f172a !important;
        }

        /* ライトモード専用：フィルタ解除・ログアウトのホバーを見えるようにする */
        main[data-theme="light"] button[class*="from-rose-500/14"]:hover,
        body[data-app-theme="light"] button[class*="from-rose-500/14"]:hover,
        main[data-theme="light"] button[class*="from-rose-500/18"][class*="hover:bg-rose-500/10"]:hover,
        body[data-app-theme="light"] button[class*="from-rose-500/18"][class*="hover:bg-rose-500/10"]:hover {
          background-color: #ffe4e6 !important;
          background-image: linear-gradient(
            135deg,
            rgba(255, 228, 230, 0.98),
            rgba(254, 205, 211, 0.92),
            rgba(251, 113, 133, 0.18)
          ) !important;
          border-color: rgba(225, 29, 72, 0.45) !important;
          color: #881337 !important;
          box-shadow: 0 10px 24px rgba(225, 29, 72, 0.16) !important;
        }

        main[data-theme="light"] button[class*="from-rose-500/14"]:hover *,
        body[data-app-theme="light"] button[class*="from-rose-500/14"]:hover *,
        main[data-theme="light"] button[class*="from-rose-500/18"][class*="hover:bg-rose-500/10"]:hover *,
        body[data-app-theme="light"] button[class*="from-rose-500/18"][class*="hover:bg-rose-500/10"]:hover * {
          color: #881337 !important;
          stroke: currentColor !important;
        }

        main[data-theme="light"] button[class*="from-rose-500/14"]:hover > span:first-child,
        body[data-app-theme="light"] button[class*="from-rose-500/14"]:hover > span:first-child {
          background: rgba(225, 29, 72, 0.14) !important;
          border-color: rgba(225, 29, 72, 0.42) !important;
          color: #881337 !important;
        }

        /* ライトモード専用：左側の「フィルタ解除」のホバーを見えるようにする */
        main[data-theme="light"] .master-data-sidebar-filter-clear-button:hover,
        body[data-app-theme="light"] .master-data-sidebar-filter-clear-button:hover {
          background-color: #e0f2fe !important;
          background-image: linear-gradient(
            135deg,
            rgba(224, 242, 254, 0.98),
            rgba(219, 234, 254, 0.96),
            rgba(186, 230, 253, 0.88)
          ) !important;
          border-color: rgba(2, 132, 199, 0.42) !important;
          color: #075985 !important;
          box-shadow: 0 10px 24px rgba(14, 165, 233, 0.16) !important;
        }

        main[data-theme="light"] .master-data-sidebar-filter-clear-button:hover .master-data-sidebar-filter-clear-icon,
        body[data-app-theme="light"] .master-data-sidebar-filter-clear-button:hover .master-data-sidebar-filter-clear-icon {
          background: rgba(14, 165, 233, 0.16) !important;
          border-color: rgba(2, 132, 199, 0.42) !important;
          color: #075985 !important;
        }

        /* ライトモード専用：ポップアップ内ボタンのホバーを漏れなく見えるようにする */
        body[data-app-theme="light"] .app-modal-root button:not(:disabled):hover,
        body[data-app-theme="light"] .app-modal-root label[class*="hover:bg-"]:hover {
          background-color: #e0f2fe !important;
          background-image: linear-gradient(
            135deg,
            rgba(224, 242, 254, 0.98),
            rgba(219, 234, 254, 0.96),
            rgba(186, 230, 253, 0.88)
          ) !important;
          border-color: rgba(2, 132, 199, 0.42) !important;
          color: #075985 !important;
          box-shadow: 0 10px 24px rgba(14, 165, 233, 0.16) !important;
        }

        body[data-app-theme="light"] .app-modal-root button:not(:disabled):hover *,
        body[data-app-theme="light"] .app-modal-root label[class*="hover:bg-"]:hover * {
          color: #075985 !important;
          stroke: currentColor !important;
        }

        /* ライトモード専用：テーマの「ライト・ダーク」マークはホバーしても色を変えない */
        main[data-theme="light"] .app-modal-root button:not(:disabled):hover .theme-light-mode-icon,
        body[data-app-theme="light"] .app-modal-root button:not(:disabled):hover .theme-light-mode-icon {
          background: rgba(251, 191, 36, 0.15) !important;
          background-image: none !important;
          border-color: rgba(252, 211, 77, 0.35) !important;
          color: #92400e !important;
          stroke: currentColor !important;
          box-shadow: none !important;
        }

        main[data-theme="light"] .app-modal-root button:not(:disabled):hover .theme-dark-mode-icon,
        body[data-app-theme="light"] .app-modal-root button:not(:disabled):hover .theme-dark-mode-icon {
          background: #0f172a !important;
          background-image: none !important;
          border-color: rgba(15, 23, 42, 0.45) !important;
          color: #ffffff !important;
          stroke: currentColor !important;
          box-shadow: none !important;
        }

        /* ライトモード専用：ポップアップ内の緑系ボタンは緑でホバーさせる */
        body[data-app-theme="light"] .app-modal-root button[class*="emerald"]:not(:disabled):hover,
        body[data-app-theme="light"] .app-modal-root label[class*="emerald"]:hover {
          background-color: #d1fae5 !important;
          background-image: linear-gradient(
            135deg,
            rgba(209, 250, 229, 0.98),
            rgba(167, 243, 208, 0.92),
            rgba(52, 211, 153, 0.22)
          ) !important;
          border-color: rgba(5, 150, 105, 0.42) !important;
          color: #047857 !important;
          box-shadow: 0 10px 24px rgba(16, 185, 129, 0.16) !important;
        }

        body[data-app-theme="light"] .app-modal-root button[class*="emerald"]:not(:disabled):hover *,
        body[data-app-theme="light"] .app-modal-root label[class*="emerald"]:hover * {
          color: #047857 !important;
          stroke: currentColor !important;
        }

        /* ライトモード専用：ポップアップ内の赤系ボタンは赤でホバーさせる */
        body[data-app-theme="light"] .app-modal-root button[class*="rose"]:not(:disabled):hover {
          background-color: #ffe4e6 !important;
          background-image: linear-gradient(
            135deg,
            rgba(255, 228, 230, 0.98),
            rgba(254, 205, 211, 0.92),
            rgba(251, 113, 133, 0.20)
          ) !important;
          border-color: rgba(225, 29, 72, 0.45) !important;
          color: #881337 !important;
          box-shadow: 0 10px 24px rgba(225, 29, 72, 0.16) !important;
        }

        body[data-app-theme="light"] .app-modal-root button[class*="rose"]:not(:disabled):hover * {
          color: #881337 !important;
          stroke: currentColor !important;
        }

        /* ライトモード専用：ポップアップ内の黄・水色・紫系ボタンもホバーを見えるようにする */
        body[data-app-theme="light"] .app-modal-root button[class*="amber"]:not(:disabled):hover {
          background-color: #fef3c7 !important;
          background-image: linear-gradient(
            135deg,
            rgba(254, 243, 199, 0.98),
            rgba(253, 230, 138, 0.92),
            rgba(245, 158, 11, 0.20)
          ) !important;
          border-color: rgba(217, 119, 6, 0.42) !important;
          color: #92400e !important;
          box-shadow: 0 10px 24px rgba(245, 158, 11, 0.16) !important;
        }

        body[data-app-theme="light"] .app-modal-root button[class*="cyan"]:not(:disabled):hover {
          background-color: #cffafe !important;
          background-image: linear-gradient(
            135deg,
            rgba(207, 250, 254, 0.98),
            rgba(165, 243, 252, 0.92),
            rgba(34, 211, 238, 0.22)
          ) !important;
          border-color: rgba(8, 145, 178, 0.42) !important;
          color: #0e7490 !important;
          box-shadow: 0 10px 24px rgba(34, 211, 238, 0.16) !important;
        }

        body[data-app-theme="light"] .app-modal-root button[class*="violet"]:not(:disabled):hover {
          background-color: #ede9fe !important;
          background-image: linear-gradient(
            135deg,
            rgba(237, 233, 254, 0.98),
            rgba(221, 214, 254, 0.92),
            rgba(139, 92, 246, 0.20)
          ) !important;
          border-color: rgba(124, 58, 237, 0.42) !important;
          color: #6d28d9 !important;
          box-shadow: 0 10px 24px rgba(139, 92, 246, 0.16) !important;
        }

        /* ライトモード専用：「CSVテンプレート」とテーマの「ダーク」を重複削除と同じ紫色でホバーさせる */
        body[data-app-theme="light"] .app-modal-root .master-data-csv-template-button:not(:disabled):hover,
        body[data-app-theme="light"] .app-modal-root .theme-dark-mode-button:not(:disabled):hover {
          background-color: #ede9fe !important;
          background-image: linear-gradient(
            135deg,
            rgba(237, 233, 254, 0.98),
            rgba(221, 214, 254, 0.92),
            rgba(139, 92, 246, 0.20)
          ) !important;
          border-color: rgba(124, 58, 237, 0.42) !important;
          color: #6d28d9 !important;
          box-shadow: 0 10px 24px rgba(139, 92, 246, 0.16) !important;
        }

        body[data-app-theme="light"] .app-modal-root .master-data-csv-template-button:not(:disabled):hover *,
        body[data-app-theme="light"] .app-modal-root .theme-dark-mode-button:not(:disabled):hover * {
          color: #6d28d9 !important;
          stroke: currentColor !important;
        }

        /* ライトモード専用：指定ボタンのホバー時の文字色だけ個別指定 */
        body[data-app-theme="light"] .app-modal-root .theme-light-mode-button:not(:disabled):hover > div > div:last-child,
        body[data-app-theme="light"] .app-modal-root .theme-light-mode-button:not(:disabled):hover > div > div:last-child * {
          color: #78350f !important;
        }

        /* ライトモード専用：項目削除・クローリング・項目精査の項目選択ポップアップは通常時の文字を黒に固定 */
        body[data-app-theme="light"] .app-modal-root .master-data-item-delete-field-modal,
        body[data-app-theme="light"] .app-modal-root .master-data-item-delete-field-modal *,
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-field-modal,
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-field-modal *,
        body[data-app-theme="light"] .app-modal-root .master-data-item-inspection-field-modal,
        body[data-app-theme="light"] .app-modal-root .master-data-item-inspection-field-modal * {
          color: #000000 !important;
        }

        /* ライトモード専用：項目選択ポップアップ下部の「いいえ / はい」だけ白文字に戻す */
        body[data-app-theme="light"] .app-modal-root .master-data-item-delete-field-modal button[class*="bg-rose-600"],
        body[data-app-theme="light"] .app-modal-root .master-data-item-delete-field-modal button[class*="bg-rose-600"] *,
        body[data-app-theme="light"] .app-modal-root .master-data-item-delete-field-modal button[class*="bg-sky-500"],
        body[data-app-theme="light"] .app-modal-root .master-data-item-delete-field-modal button[class*="bg-sky-500"] *,
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-field-modal button[class*="bg-rose-600"],
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-field-modal button[class*="bg-rose-600"] *,
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-field-modal button[class*="bg-sky-500"],
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-field-modal button[class*="bg-sky-500"] *,
        body[data-app-theme="light"] .app-modal-root .master-data-item-inspection-field-modal button[class*="bg-rose-600"],
        body[data-app-theme="light"] .app-modal-root .master-data-item-inspection-field-modal button[class*="bg-rose-600"] *,
        body[data-app-theme="light"] .app-modal-root .master-data-item-inspection-field-modal button[class*="bg-sky-500"],
        body[data-app-theme="light"] .app-modal-root .master-data-item-inspection-field-modal button[class*="bg-sky-500"] * {
          color: #ffffff !important;
        }

        /* ライトモード専用：項目削除 項目選択・項目精査 項目選択の選択項目ホバーを青にする */
        body[data-app-theme="light"] .app-modal-root .master-data-item-delete-field-modal label.master-data-blue-selection-option:hover,
        body[data-app-theme="light"] .app-modal-root .master-data-item-inspection-field-modal label.master-data-blue-selection-option:hover {
          background-color: #dbeafe !important;
          background-image: none !important;
          border-color: #2563eb !important;
          color: #1d4ed8 !important;
        }

        body[data-app-theme="light"] .app-modal-root .master-data-item-delete-field-modal label.master-data-blue-selection-option:hover *,
        body[data-app-theme="light"] .app-modal-root .master-data-item-inspection-field-modal label.master-data-blue-selection-option:hover * {
          color: #1d4ed8 !important;
        }

        /* ライトモード専用：クローリング 項目選択の選択項目ホバーをオレンジにする */
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-field-modal label.master-data-orange-selection-option:hover,
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-field-modal label.master-data-orange-selection-option[class*="hover:bg-"]:hover {
          background-color: #ffedd5 !important;
          background-image: none !important;
          border-color: #f97316 !important;
          color: #c2410c !important;
          box-shadow: 0 10px 24px rgba(249, 115, 22, 0.16) !important;
        }

        body[data-app-theme="light"] .app-modal-root .master-data-crawl-field-modal label.master-data-orange-selection-option:hover *,
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-field-modal label.master-data-orange-selection-option[class*="hover:bg-"]:hover * {
          color: #c2410c !important;
          stroke: currentColor !important;
        }

        body[data-app-theme="light"] .app-modal-root .master-data-crawl-menu-button:not(:disabled):hover > div:not(:first-child),
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-menu-button:not(:disabled):hover > div:not(:first-child) * {
          color: #92400e !important;
        }

        body[data-app-theme="light"] .app-modal-root .master-data-dedupe-menu-button:not(:disabled):hover > div:not(:first-child),
        body[data-app-theme="light"] .app-modal-root .master-data-dedupe-menu-button:not(:disabled):hover > div:not(:first-child) * {
          color: #6d28d9 !important;
        }

        /* ライトモード専用：リスト削除 対象選択のマーク・全件を通常時も赤色にする */
        body[data-app-theme="light"] .app-modal-root .master-data-list-delete-scope-all-card .master-data-selection-option-icon,
        body[data-app-theme="light"] .app-modal-root .master-data-list-delete-scope-all-card .master-data-selection-option-badge {
          color: #be123c !important;
          stroke: currentColor !important;
        }

        /* ライトモード専用：対象選択の「全てのリスト」だけホバー文字色を変更する */
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-scope-all-card:not(:disabled):hover .master-data-selection-option-title {
          color: #92400e !important;
        }

        body[data-app-theme="light"] .app-modal-root .master-data-dedupe-scope-all-card:not(:disabled):hover .master-data-selection-option-title {
          color: #6d28d9 !important;
        }

        /* ライトモード専用：対象選択のマーク・全件・説明文はホバーしても今の色を維持する */
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-scope-all-card:not(:disabled):hover .master-data-selection-option-icon,
        body[data-app-theme="light"] .app-modal-root .master-data-crawl-scope-all-card:not(:disabled):hover .master-data-selection-option-badge {
          color: #92400e !important;
          stroke: currentColor !important;
        }

        body[data-app-theme="light"] .app-modal-root .master-data-dedupe-scope-all-card:not(:disabled):hover .master-data-selection-option-icon,
        body[data-app-theme="light"] .app-modal-root .master-data-dedupe-scope-all-card:not(:disabled):hover .master-data-selection-option-badge {
          color: #6d28d9 !important;
          stroke: currentColor !important;
        }

        body[data-app-theme="light"] .app-modal-root .master-data-crawl-scope-all-card:not(:disabled):hover .master-data-selection-option-description,
        body[data-app-theme="light"] .app-modal-root .master-data-dedupe-scope-all-card:not(:disabled):hover .master-data-selection-option-description {
          color: #64748b !important;
        }

      /* ライトモード専用：読み込み中の背景を明るいぼかし表示にする */
        body[data-app-theme="light"] .master-data-loading-overlay {
          background: rgba(248, 250, 252, 0.58) !important;
        }

        body[data-app-theme="light"] .master-data-loading-card {
          border-color: rgba(15, 23, 42, 0.12) !important;
          background: rgba(255, 255, 255, 0.72) !important;
          box-shadow: 0 24px 80px rgba(15, 23, 42, 0.22) !important;
        }

        /* ライトモード専用：ログイン画面だけはダークモードと同じ背景を維持する */
        main[data-theme="light"] .master-data-login-screen [class*="bg-[#05070d]"],
        body[data-app-theme="light"] .master-data-login-screen [class*="bg-[#05070d]"] {
          background: #05070d !important;
        }

        main[data-theme="light"] .master-data-login-screen button[type="submit"][class*="bg-[#0b1326]"],
        body[data-app-theme="light"] .master-data-login-screen button[type="submit"][class*="bg-[#0b1326]"] {
          background: #0b1326 !important;
          color: #ffffff !important;
        }

        main[data-theme="light"] .master-data-login-screen button[type="submit"][class*="bg-[#0b1326]"]:hover,
        body[data-app-theme="light"] .master-data-login-screen button[type="submit"][class*="bg-[#0b1326]"]:hover {
          background: #111d38 !important;
          color: #ffffff !important;
        }

        main[data-theme="light"] .master-data-login-screen button[type="submit"][class*="bg-[#0b1326]"] *,
        body[data-app-theme="light"] .master-data-login-screen button[type="submit"][class*="bg-[#0b1326]"] * {
          color: #ffffff !important;
        }

        /* ライトモード専用：ログイン画面のロゴ色をダークモードと同じにする */
        main[data-theme="light"] .master-data-login-screen .master-data-brand-logo,
        body[data-app-theme="light"] .master-data-login-screen .master-data-brand-logo {
          --mdb-gold-1: #f8e7c5 !important;
          --mdb-gold-2: #ddb879 !important;
          --mdb-gold-3: #b98542 !important;
          --mdb-gold-text: #f1d4a4 !important;
          filter:
            drop-shadow(0 18px 36px rgba(0, 0, 0, 0.34))
            drop-shadow(0 4px 14px rgba(247, 227, 191, 0.18)) !important;
        }

        /* ライトモード専用：ログイン画面の「マスタデータ」文字色をダークモードと同じにする */
        main[data-theme="light"] .master-data-login-screen .master-data-brand-title,
        body[data-app-theme="light"] .master-data-login-screen .master-data-brand-title {
          color: #f1d4a4 !important;
          text-shadow:
            0 14px 32px rgba(0, 0, 0, 0.30),
            0 2px 10px rgba(247, 227, 191, 0.12) !important;
        }

        .master-data-top-left-notices {
          position: fixed;
          top: 20px;
          left: 20px;
          z-index: 10080;
          display: flex;
          width: min(420px, calc(100vw - 40px));
          max-height: calc(100dvh - 40px);
          flex-direction: column;
          gap: 12px;
          overflow-y: auto;
          pointer-events: none;
          scrollbar-width: none;
          -ms-overflow-style: none;
        }

        .master-data-top-left-notices::-webkit-scrollbar {
          display: none;
        }

        .master-data-notice {
          display: flex;
          align-items: flex-start;
          gap: 12px;
          border-radius: 18px;
          padding: 14px 16px;
          border: 1px solid rgba(255, 255, 255, 0.12);
          background:
            linear-gradient(135deg, rgba(15, 23, 42, 0.92), rgba(2, 6, 23, 0.82)),
            radial-gradient(circle at top left, rgba(197, 155, 90, 0.22), transparent 42%);
          box-shadow:
            0 18px 46px rgba(0, 0, 0, 0.38),
            inset 0 1px 0 rgba(255, 255, 255, 0.08);
          color: #f8fafc;
          backdrop-filter: blur(18px);
          transform-origin: top left;
          animation: master-data-notice-in 0.42s cubic-bezier(0.2, 0.9, 0.2, 1) both;
        }

        .master-data-notice-icon {
          display: inline-flex;
          width: 34px;
          height: 34px;
          flex: 0 0 auto;
          align-items: center;
          justify-content: center;
          border-radius: 14px;
          font-size: 15px;
          font-weight: 900;
          color: #f8fafc;
          background: rgba(255, 255, 255, 0.08);
          border: 1px solid rgba(255, 255, 255, 0.12);
        }

        .master-data-notice-title {
          color: #f8fafc;
          font-size: 13px;
          font-weight: 900;
          letter-spacing: 0.04em;
          line-height: 1.45;
        }

        .master-data-notice-message {
          margin-top: 4px;
          color: rgba(226, 232, 240, 0.88);
          font-size: 12px;
          font-weight: 600;
          line-height: 1.6;
          word-break: break-word;
        }

        .master-data-notice--exiting {
          pointer-events: none;
          animation: master-data-notice-out 0.42s cubic-bezier(0.4, 0, 1, 1) forwards;
        }

        @keyframes master-data-notice-in {
          0% {
            opacity: 0;
            transform: translateX(-18px) translateY(-6px) scale(0.96);
            filter: blur(8px);
          }

          70% {
            opacity: 1;
            transform: translateX(3px) translateY(0) scale(1.01);
            filter: blur(0);
          }

          100% {
            opacity: 1;
            transform: translateX(0) translateY(0) scale(1);
            filter: blur(0);
          }
        }

        @keyframes master-data-notice-out {
          0% {
            opacity: 1;
            transform: translateX(0) translateY(0) scale(1);
            filter: blur(0);
          }

          100% {
            opacity: 0;
            transform: translateX(-24px) translateY(-8px) scale(0.94);
            filter: blur(8px);
          }
        }

        .master-data-notice--working {
          border-color: rgba(197, 155, 90, 0.32);
        }

        .master-data-notice--success {
          border-color: rgba(52, 211, 153, 0.32);
        }

        .master-data-notice--success .master-data-notice-icon {
          color: #bbf7d0;
          background: rgba(16, 185, 129, 0.16);
          border-color: rgba(52, 211, 153, 0.28);
        }

        .master-data-notice--error {
          border-color: rgba(251, 113, 133, 0.36);
        }

        .master-data-notice--error .master-data-notice-icon {
          color: #fecdd3;
          background: rgba(244, 63, 94, 0.16);
          border-color: rgba(251, 113, 133, 0.30);
        }

        .master-data-notice-spinner {
          width: 16px;
          height: 16px;
          border-radius: 999px;
          border: 2px solid rgba(248, 250, 252, 0.24);
          border-top-color: #c59b5a;
          animation: master-data-notice-spin 0.78s linear infinite;
        }

        @keyframes master-data-notice-spin {
          to {
            transform: rotate(360deg);
          }
        }

        body[data-app-theme="light"] .master-data-notice {
          border-color: rgba(15, 23, 42, 0.12);
          background:
            linear-gradient(135deg, rgba(255, 255, 255, 0.96), rgba(248, 250, 252, 0.92)),
            radial-gradient(circle at top left, rgba(197, 155, 90, 0.18), transparent 44%);
          box-shadow:
            0 18px 46px rgba(15, 23, 42, 0.18),
            inset 0 1px 0 rgba(255, 255, 255, 0.86);
          color: #0f172a;
        }

        body[data-app-theme="light"] .master-data-notice-title {
          color: #0f172a;
        }

        body[data-app-theme="light"] .master-data-notice-message {
          color: #475569;
        }

        body[data-app-theme="light"] .master-data-notice-icon {
          color: #0f172a;
          background: rgba(15, 23, 42, 0.05);
          border-color: rgba(15, 23, 42, 0.10);
        }

        body[data-app-theme="light"] .master-data-notice--success .master-data-notice-icon {
          color: #047857;
          background: rgba(16, 185, 129, 0.12);
          border-color: rgba(16, 185, 129, 0.22);
        }

        body[data-app-theme="light"] .master-data-notice--error .master-data-notice-icon {
          color: #be123c;
          background: rgba(244, 63, 94, 0.10);
          border-color: rgba(244, 63, 94, 0.20);
        }

        body[data-app-theme="light"] .master-data-notice-spinner {
          border-color: rgba(15, 23, 42, 0.16);
          border-top-color: #b98542;
        }

        /* ライトモード専用：通知メッセージの文字を見やすくする */
        main[data-theme="light"] [class*="text-rose-200"],
        body[data-app-theme="light"] [class*="text-rose-200"] {
          color: #be123c !important;
        }

        main[data-theme="light"] [class*="text-emerald-200"],
        body[data-app-theme="light"] [class*="text-emerald-200"] {
          color: #047857 !important;
        }

        main[data-theme="light"] [class*="text-sky-200"],
        body[data-app-theme="light"] [class*="text-sky-200"] {
          color: #0369a1 !important;
        }

        main[data-theme="light"] [class*="text-amber-200"],
        body[data-app-theme="light"] [class*="text-amber-200"] {
          color: #b45309 !important;
        }

        main[data-theme="light"] [class*="text-cyan-200"],
        body[data-app-theme="light"] [class*="text-cyan-200"] {
          color: #0e7490 !important;
        }

        main[data-theme="light"] [class*="text-violet-200"],
        body[data-app-theme="light"] [class*="text-violet-200"] {
          color: #6d28d9 !important;
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