export type CrawlExtractedOffice = {
  office_name: string | null;
  company: string | null;
  phone_candidates: string[];
  fax_candidates: string[];
  email_candidates: string[];
  zipcode_candidates: string[];
  address_candidates: string[];
};

export type CrawlExtractedFields = {
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
  representative_name_raw: string | null;
  representative_name_reason: string | null;
  representative_title: string | null;
  capital: string | null;
  employee_count: string | null;
  business_content: string | null;
  permit_number: string | null;
  offices: CrawlExtractedOffice[];
};

export type CrawlSelectableFieldKey =
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
  | "business_content"
  | "worker_dispatch_license"
  | "paid_job_placement_license";

const DEFAULT_CRAWL_SELECTABLE_FIELDS: CrawlSelectableFieldKey[] = [
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
  "worker_dispatch_license",
  "paid_job_placement_license",
];

function hasSelectedCrawlField(
  selectedFieldSet: Set<CrawlSelectableFieldKey>,
  field: CrawlSelectableFieldKey
) {
  return selectedFieldSet.has(field);
}

type LinkItem = {
  url: string;
  text: string;
  sameOrigin: boolean;
};

type PageData = {
  requestedUrl: string;
  finalUrl: string;
  html: string;
  text: string;
  structuredText: string;
  title: string;
  h1: string;
  links: LinkItem[];
};

type BestValue = {
  value: string;
  score: number;
};

type CrawlRuntimeOptions = {
  shouldStop?: () => boolean;
};

const CRAWL_PAUSED_ERROR_MESSAGE = "__MASTER_DATA_CRAWL_PAUSED__";

const FETCH_RETRY_DELAYS_MS = [0, 2000, 5000] as const;

const FETCH_RETRYABLE_STATUS_SET = new Set([
  403, 408, 425, 429, 500, 502, 503, 504, 521, 522, 523, 524,
]);

const HOST_COOLDOWN_UNTIL_MAP = new Map<string, number>();
const HOST_FAILURE_COUNT_MAP = new Map<string, number>();

const TEMPORARY_BLOCK_PAGE_REGEX =
  /(too many requests|temporarily unavailable|service unavailable|access denied|forbidden|just a moment|verify you are human|attention required|enable javascript and cookies|unusual traffic|automated queries|security check|request blocked|アクセスが集中|アクセスが拒否|アクセス制限|しばらくしてから|ご利用を制限)/i;

function isRetryableFetchStatus(status: number) {
  return FETCH_RETRYABLE_STATUS_SET.has(status);
}

function getUrlHost(url: string) {
  try {
    return new URL(url).host.toLowerCase();
  } catch {
    return null;
  }
}

async function sleepCrawl(ms: number, runtimeOptions?: CrawlRuntimeOptions) {
  const endAt = Date.now() + Math.max(ms, 0);

  while (Date.now() < endAt) {
    throwIfCrawlShouldStop(runtimeOptions);

    const remaining = endAt - Date.now();
    await new Promise((resolve) =>
      setTimeout(resolve, Math.min(remaining, 200))
    );
  }
}

async function waitForHostCooldown(
  url: string,
  runtimeOptions?: CrawlRuntimeOptions
) {
  const host = getUrlHost(url);
  if (!host) return;

  const cooldownUntil = HOST_COOLDOWN_UNTIL_MAP.get(host) ?? 0;
  const waitMs = cooldownUntil - Date.now();

  if (waitMs > 0) {
    await sleepCrawl(waitMs, runtimeOptions);
  }
}

function markHostTemporaryFailure(url: string, attemptIndex: number) {
  const host = getUrlHost(url);
  if (!host) return;

  const failureCount = (HOST_FAILURE_COUNT_MAP.get(host) ?? 0) + 1;
  HOST_FAILURE_COUNT_MAP.set(host, failureCount);

  const cooldownMs = Math.min(
    15000,
    2000 * Math.max(failureCount, attemptIndex + 1)
  );

  const nextUntil = Date.now() + cooldownMs;
  const currentUntil = HOST_COOLDOWN_UNTIL_MAP.get(host) ?? 0;

  if (nextUntil > currentUntil) {
    HOST_COOLDOWN_UNTIL_MAP.set(host, nextUntil);
  }
}

function markHostFetchSuccess(url: string) {
  const host = getUrlHost(url);
  if (!host) return;

  HOST_FAILURE_COUNT_MAP.delete(host);
  HOST_COOLDOWN_UNTIL_MAP.delete(host);
}

function looksLikeTemporaryBlockPage(content: string) {
  return TEMPORARY_BLOCK_PAGE_REGEX.test(normalizeSpace(content));
}

const WORKER_DISPATCH_LICENSE_REGEX =
  /派\s*[0-9０-９]{2}\s*[-－ー―]\s*[0-9０-９]{6}/i;

const PAID_JOB_PLACEMENT_LICENSE_REGEX =
  /[0-9０-９]{2}\s*[-－ー―]\s*ユ\s*[-－ー―]\s*[0-9０-９]{6}/i;

function hasSelectedPermitFields(
  selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  return (
    hasSelectedCrawlField(selectedFieldSet, "worker_dispatch_license") ||
    hasSelectedCrawlField(selectedFieldSet, "paid_job_placement_license")
  );
}

function extractPermitNumberCategory(text: string) {
  const normalized = text.normalize("NFKC");

  const hasWorkerDispatch = WORKER_DISPATCH_LICENSE_REGEX.test(normalized);
  const hasPaidPlacement = PAID_JOB_PLACEMENT_LICENSE_REGEX.test(normalized);

  if (hasWorkerDispatch && hasPaidPlacement) {
    return "労働者派遣・有料職業紹介";
  }

  if (hasWorkerDispatch) {
    return "労働者派遣";
  }

  if (hasPaidPlacement) {
    return "有料職業紹介";
  }

  return null;
}

function throwIfCrawlShouldStop(runtimeOptions?: CrawlRuntimeOptions) {
  if (runtimeOptions?.shouldStop?.()) {
    throw new Error(CRAWL_PAUSED_ERROR_MESSAGE);
  }
}

const CONTACT_KEYWORDS =
  /(お問い合わせ|お問合せ|お問い合わせ先|お問合せ先|連絡先|contact|inquiry|consult|相談|資料請求|フォーム|form|mail)/i;

const COMPANY_KEYWORDS =
  /(会社概要|企業情報|会社案内|会社情報|法人概要|企業概要|事務所概要|事業所案内|店舗案内|拠点情報|アクセス|所在地|outline|profile|company|about|corporate|information)/i;

const BUSINESS_KEYWORDS =
  /(事業内容|業務内容|営業内容|取扱業務|取扱内容|取扱商品|サービス|service|business|業容|事業概要|サービス内容)/i;

const STAFF_KEYWORDS =
  /(代表|代表者|社長|会長|役員|理事長|所長|センター長|学院長|校長|学長|施設長|室長|ご挨拶|あいさつ|メッセージ|greeting|message|president|ceo|director|chief|officer|executive)/i;

const RECRUIT_KEYWORDS =
  /(採用|求人|募集要項|recruit|career|job|jobs|entry)/i;

const NEWS_BLOG_KEYWORDS =
  /(お知らせ|新着|news|blog|ブログ|column|コラム|topics|トピックス|works|実績|case|事例|seminar|セミナー|event|イベント|voice|story|success|diary|interview|インタビュー|ひとりごと|卒業生|在校生|コーチ|先生)/i;

const HTML_PAGE_DENY_EXT =
  /\.(zip|jpg|jpeg|png|gif|svg|webp|doc|docx|xls|xlsx|ppt|pptx)$/i;

const PDF_PAGE_REGEX = /\.pdf(?:$|\?)/i;

const REPRESENTATIVE_TRAILING_TITLE_REGEX =
  /\s*(?:代表取締役(?:社長|会長)?|取締役社長|取締役|代表社員|代表理事|理事長|社長|会長|CEO|COO|CFO|CTO|常務取締役?|専務取締役?|執行役員(?:専務|常務)?|常務|専務|相談役|名誉相談役|所長|センター長|学院長|校長|学長|施設長|室長|代表)\s*$/i;

const REPRESENTATIVE_LEADING_LABEL_REGEX =
  /^(?:代表者氏名|代表氏名|代表者名?|代表者|代表取締役(?:社長|会長)?|取締役社長|代表社員|代表理事|理事長|社長|会長|所長|センター長|学院長|校長|学長|施設長|室長|役員(?!一覧|紹介))\s*[:：]?\s*/i;

const REPRESENTATIVE_INLINE_TITLE_REGEX =
  /(代表取締役社長|代表取締役会長|代表取締役|取締役社長|代表社員|代表理事|理事長|社長|会長|CEO|COO|CFO|CTO|代表|所長|センター長|学院長|校長|学長|施設長|室長)(?!から)/i;

const REPRESENTATIVE_STRONG_PAGE_REGEX =
  /(会社概要(?:・沿革)?|会社案内|会社情報|企業情報|法人概要|企業概要|会社データ|会社紹介|会社基本情報|基本情報|outline|profile|company|corporate|about|gaiyou|overview|information)/i;

const REPRESENTATIVE_GREETING_PAGE_REGEX =
  /(代表挨拶|社長挨拶|理事長挨拶|所長挨拶|ご挨拶|トップメッセージ|topmessage|greeting|message|president)/i;

const REPRESENTATIVE_WEAK_PAGE_REGEX =
  /(役員一覧|officer|executive|member|staff)/i;

const REPRESENTATIVE_DENY_PAGE_REGEX =
  /(recruit|career|job|jobs|entry|採用|新卒|中途|blog|news|topics|column|interview|voice|story|success|diary|shop|shopinfo|campus|店舗|商品|製品|service|faq|contact)/i;

const REPRESENTATIVE_NON_NAME_REGEX =
  /(会社概要|会社情報|企業情報|法人概要|企業概要|基本情報|会社データ|会社紹介|採用|人事|営業|問い合わせ|お問い合わせ|連絡先|所在地|住所|アクセス|資本金|従業員数|事業内容|サービス|商品|製品|一覧|ブログ|ニュース|お知らせ|沿革|理念|方針|インタビュー|スタッフ|店舗|工場|営業所)/i;

const REPRESENTATIVE_COMPANY_REGEX =
  /株式会社|有限会社|合同会社|合資会社|合名会社|御中/i;

const REPRESENTATIVE_NAME_BODY_REGEX =
  /^[\p{sc=Han}\p{sc=Katakana}\p{sc=Hiragana}々ヶヵー]{2,20}(?:\s+[\p{sc=Han}\p{sc=Katakana}\p{sc=Hiragana}々ヶヵー]{1,20})?$/u;

type RepresentativeCandidate = {
  value: string;
  score: number;
};

function cleanRepresentativeCandidate(value: string) {
  const normalized = normalizeSpace(value)
    .replace(/[（(][^）)]*[）)]/g, " ")
    .replace(/[【】\[\]「」『』<>〈〉《》〔〕]/g, " ")
    .replace(REPRESENTATIVE_LEADING_LABEL_REGEX, "")
    .replace(
      /^(?:代表取締役(?:社長|会長)?|取締役社長|取締役|代表社員|代表理事|理事長|社長|会長|代表者?|代表|執行役員(?:専務|常務)?|常務取締役?|専務取締役?|常務|専務|相談役|名誉相談役|所長|センター長|学院長|校長|学長|施設長|室長|一級塗装技能士|二級塗装技能士|一級建築士|二級建築士|建築士|大工)\s*/i,
      ""
    )
    .replace(
      /\s*(?:代表取締役(?:社長|会長)?|取締役社長|取締役|代表社員|代表理事|理事長|社長|会長|代表者?|代表|執行役員(?:専務|常務)?|常務取締役?|専務取締役?|常務|専務|相談役|名誉相談役|所長|センター長|学院長|校長|学長|施設長|室長|一級塗装技能士|二級塗装技能士|一級建築士|二級建築士|建築士|大工)\s*$/i,
      ""
    )
    .replace(/\s*[／/].*$/, "")
    .replace(/\s+/g, " ")
    .trim();

  return normalized === "" ? null : normalized;
}

export function inspectRepresentativeNameValue(value: string | null) {
  const cleanedValue = cleanRepresentativeCandidate(value ?? "");

  return {
    cleanedValue,
    shouldUpdate: false,
    shouldDelete: false,
    shouldReview: false,
    reason: "",
  };
}

function normalizeRepresentativeName(
  value: string,
  _options?: { allowCompactSingleToken?: boolean }
) {
  return cleanRepresentativeCandidate(value);
}

function normalizeRepresentativeCandidateForBest(value: string) {
  return cleanRepresentativeCandidate(value);
}

const REPRESENTATIVE_OBVIOUS_NON_NAME_VALUES = new Set([
  "挨拶",
  "ご挨拶",
  "代表挨拶",
  "社長挨拶",
  "理事長挨拶",
  "トップメッセージ",
  "メッセージ",
  "氏名",
  "代表者",
  "代表者名",
  "代表者氏名",
  "代表",
  "社長",
  "会長",
  "理事長",
  "役員",
  "役員一覧",
  "会社概要",
  "会社情報",
  "企業情報",
  "基本情報",
]);

function isLikelyRepresentativePersonNameCandidate(value: string) {
  const cleaned = cleanRepresentativeCandidate(value);
  if (!cleaned) return false;

  const normalized = cleaned.normalize("NFKC");

  if (REPRESENTATIVE_OBVIOUS_NON_NAME_VALUES.has(normalized)) return false;
  if (REPRESENTATIVE_NON_NAME_REGEX.test(normalized)) return false;
  if (REPRESENTATIVE_COMPANY_REGEX.test(normalized)) return false;
  if (/[0-9０-９]/.test(normalized)) return false;
  if (normalized.length < 2 || normalized.length > 40) return false;

  return /^[\p{sc=Han}\p{sc=Katakana}\p{sc=Hiragana}々ヶヵーA-Za-z]+(?:\s+[\p{sc=Han}\p{sc=Katakana}\p{sc=Hiragana}々ヶヵーA-Za-z]+)?$/u.test(
    normalized
  );
}

function pushRepresentativeCandidate(
  candidateMap: Map<string, number>,
  rawValue: string | null | undefined,
  score: number
) {
  if (!rawValue) return;

  const cleaned = cleanRepresentativeCandidate(rawValue);
  if (!cleaned) return;
  if (!isLikelyRepresentativePersonNameCandidate(cleaned)) return;

  const current = candidateMap.get(cleaned);
  if (current == null || score > current) {
    candidateMap.set(cleaned, score);
  }
}

function collectRepresentativeCandidatesFromText(
  text: string,
  baseScore = 0
): RepresentativeCandidate[] {
  const candidateMap = new Map<string, number>();

  const lines = text
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "");

  const exactLabelOnlyPattern =
    /^(?:代表者氏名|代表氏名|代表者名?|代表者|代表取締役(?:社長|会長)?|取締役社長|代表社員|代表理事|理事長|社長|会長|所長|センター長|学院長|校長|学長|施設長|室長)$/i;

  const exactSameLinePattern =
    /^(?:代表者氏名|代表氏名|代表者名?|代表者|代表取締役(?:社長|会長)?|取締役社長|代表社員|代表理事|理事長|社長|会長|所長|センター長|学院長|校長|学長|施設長|室長)\s*[:：]?\s*(.+)$/i;

  const sameLineAfterTitlePattern =
    /^(?:代表取締役社長|代表取締役会長|代表取締役|取締役社長|代表社員|代表理事|理事長|会長|社長)\s*[:：/／]?\s*(.+)$/i;

  const sameLineBeforeTitlePattern =
    /^(.+?)\s+(?:代表取締役(?:社長|会長)?|取締役社長|取締役|代表社員|代表理事|理事長|社長|会長|代表)$/u;

  const titleOnlyPattern =
    /^(?:代表取締役(?:社長|会長)?|取締役社長|代表社員|代表理事|理事長|社長|会長|一級塗装技能士|二級塗装技能士|一級建築士|二級建築士|建築士|大工)(?:\s+(?:一級塗装技能士|二級塗装技能士|一級建築士|二級建築士|建築士|大工))*$/u;

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];

    const sameLine = line.match(exactSameLinePattern);
    if (sameLine?.[1]) {
      pushRepresentativeCandidate(candidateMap, sameLine[1], 1100 + baseScore);
    }

    if (exactLabelOnlyPattern.test(line)) {
      pushRepresentativeCandidate(candidateMap, lines[i + 1] ?? null, 1080 + baseScore);
      pushRepresentativeCandidate(candidateMap, lines[i + 2] ?? null, 1040 + baseScore);
      pushRepresentativeCandidate(candidateMap, lines[i + 3] ?? null, 1000 + baseScore);
      pushRepresentativeCandidate(candidateMap, lines[i + 4] ?? null, 960 + baseScore);
      pushRepresentativeCandidate(candidateMap, lines[i + 5] ?? null, 920 + baseScore);
    }

    const sameLineAfterTitle = line.match(sameLineAfterTitlePattern);
    if (sameLineAfterTitle?.[1]) {
      pushRepresentativeCandidate(candidateMap, sameLineAfterTitle[1], 980 + baseScore);
    }

    const sameLineBeforeTitle = line.match(sameLineBeforeTitlePattern);
    if (sameLineBeforeTitle?.[1]) {
      pushRepresentativeCandidate(candidateMap, sameLineBeforeTitle[1], 960 + baseScore);
    }

    if (titleOnlyPattern.test(line)) {
      pushRepresentativeCandidate(candidateMap, lines[i + 1] ?? null, 1060 + baseScore);
      pushRepresentativeCandidate(candidateMap, lines[i + 2] ?? null, 1020 + baseScore);
      pushRepresentativeCandidate(candidateMap, lines[i + 3] ?? null, 980 + baseScore);
      pushRepresentativeCandidate(candidateMap, lines[i + 4] ?? null, 940 + baseScore);
      pushRepresentativeCandidate(candidateMap, lines[i + 5] ?? null, 900 + baseScore);
    }
  }

  return Array.from(candidateMap.entries())
    .map(([value, score]) => ({ value, score }))
    .sort((a, b) => b.score - a.score);
}

function extractRepresentativeNameFromText(text: string) {
  const candidates = collectRepresentativeCandidatesFromText(text, 0);
  return candidates[0]?.value ?? null;
}

function getRepresentativePagePriorityBoost(page: PageData) {
  const target = decodeURIComponent(
    `${page.finalUrl} ${page.title} ${page.h1}`
  );

  let score = 0;

  if (REPRESENTATIVE_STRONG_PAGE_REGEX.test(target)) score += 400;
  if (/(?:当社|弊社|私たち|わたしたち|会社|企業|法人|事務所|株式会社[^\s　/]{0,30}|有限会社[^\s　/]{0,30}|合同会社[^\s　/]{0,30})について/i.test(target)) score += 260;
  if (REPRESENTATIVE_GREETING_PAGE_REGEX.test(target)) score += 240;
  if (REPRESENTATIVE_WEAK_PAGE_REGEX.test(target)) score += 120;
  if (REPRESENTATIVE_DENY_PAGE_REGEX.test(target)) score -= 320;

  return score;
}

function collectRepresentativeCandidates(page: PageData): RepresentativeCandidate[] {
  const candidateMap = new Map<string, number>();
  const pagePriorityBoost = getRepresentativePagePriorityBoost(page);
  const pairs = extractPairs(page.html);

  const representativePairValue = pickRepresentativePairValue(pairs);
  pushRepresentativeCandidate(
    candidateMap,
    representativePairValue,
    1250 + pagePriorityBoost
  );

  const representativeTextValue =
    extractSingleLineLabeledValue(page.structuredText, REPRESENTATIVE_NAME_LABELS) ??
    null;

  pushRepresentativeCandidate(
    candidateMap,
    representativeTextValue,
    1230 + pagePriorityBoost
  );

  const representativeStructuredTableValue =
    extractRepresentativeTableLikeValue(page.structuredText);

  pushRepresentativeCandidate(
    candidateMap,
    representativeStructuredTableValue,
    1220 + pagePriorityBoost
  );

  const representativeFlatTextValue =
    page.text !== page.structuredText
      ? extractSingleLineLabeledValue(page.text, REPRESENTATIVE_NAME_LABELS)
      : null;

  pushRepresentativeCandidate(
    candidateMap,
    representativeFlatTextValue,
    1190 + pagePriorityBoost
  );

  const representativeFlatTableValue =
    page.text !== page.structuredText
      ? extractRepresentativeTableLikeValue(page.text)
      : null;

  pushRepresentativeCandidate(
    candidateMap,
    representativeFlatTableValue,
    1180 + pagePriorityBoost
  );

  const textCandidates = collectRepresentativeCandidatesFromText(
    page.structuredText,
    pagePriorityBoost
  );

  for (const candidate of textCandidates) {
    pushRepresentativeCandidate(candidateMap, candidate.value, candidate.score);
  }

  if (page.text !== page.structuredText) {
    const flatTextCandidates = collectRepresentativeCandidatesFromText(
      page.text,
      pagePriorityBoost - 60
    );

    for (const candidate of flatTextCandidates) {
      pushRepresentativeCandidate(candidateMap, candidate.value, candidate.score);
    }
  }

  return Array.from(candidateMap.entries())
    .map(([value, score]) => ({ value, score }))
    .sort((a, b) => b.score - a.score);
}

const COMMON_CANDIDATE_PATHS = [
  "/company.html",
  "/company/",
  "/company/index.html",
  "/company/outline.html",
  "/company/profile.html",
  "/company/message.html",
  "/company/greeting.html",
  "/company/access.html",
  "/company/gaiyou.html",
  "/company/overview.html",
  "/company/about/",
  "/company/data/",
  "/company/numbers/",
  "/company/numbers.html",
  "/numbers/",
  "/numbers/index.html",
  "/recruit/",
  "/recruit/data/",
  "/recruit/company/",
  "/companyinfo.html",
  "/profile.html",
  "/about.html",
  "/about/",
  "/aboutus/",
  "/aboutus/company-outline/",
  "/corporate.html",
  "/corporate/",
  "/corporate/profile/",
  "/outline.html",
  "/gaiyou.html",
  "/overview.html",
  "/information.html",
  "/message.html",
  "/greeting.html",
  "/staff.html",
  "/member.html",
  "/office.html",
  "/access.html",
  "/contact.html",
  "/data/",
  "/data/index.html",
  "/about/chubu/index.html",
  "/chubu/office/greeting.html",
  "/domestic/chubu/office/greeting.html",
  "/office/greeting.html",
  "/aisatsu.html",
  "/greeting/president.html",
  "/president.html",
  "/president/message.html",
  "/ceo-message.html",
  "/topmessage.html",
  "/message/president.html",
  "/about/message.html",
  "/about/greeting.html",
  "/company/topmessage.html",
  "/company/president.html",
  "/corporate/message.html",
  "/corporate/greeting.html",
  "/about/outline/",
  "/about/company/",
  "/company/about/",
  "/company/info/",
  "/kaisya.html",
  "/kaisya/gaiyou.html",
  "/company/about.html",
  "/company/overview/",
  "/company/outline/",
  "/company/profile/",
  "/company/message/",
  "/company/greeting/",
  "/corporate/about/",
  "/corporate/outline/",
  "/corporate/company/",
  "/outline/",
  "/profile/",
  "/company/",
  "/kaisha/",
  "/company/base/",
];

const REPRESENTATIVE_OVERVIEW_PAGE_KEYWORDS =
  /(会社概要(?:・沿革)?|会社案内|会社情報|企業情報|法人概要|企業概要|会社データ|会社紹介|会社基本情報|基本情報|会社を知る|outline|profile|company|corporate|about|gaiyou|overview|information)/i;

const REPRESENTATIVE_FALLBACK_DENY_KEYWORDS =
  /(recruit|career|job|jobs|entry|新卒|中途|採用|blog|news|topics|column|interview|voice|story|success|diary|staff|member|members|社員紹介|社員インタビュー|shop|shopinfo|campus|店舗|商品|製品|service|faq|contact)/i;

function isRepresentativeOverviewPageTarget(target: string) {
  const normalized = decodeURIComponent(target);

  return (
    REPRESENTATIVE_OVERVIEW_PAGE_KEYWORDS.test(normalized) &&
    !REPRESENTATIVE_FALLBACK_DENY_KEYWORDS.test(normalized)
  );
}

function hasRepresentativePageSignals(page: PageData) {
  const pageTarget = decodeURIComponent(
    `${page.finalUrl} ${page.title} ${page.h1}`
  );

  if (
    /(代表者|代表者名|代表者氏名|代表取締役|取締役社長|社長|会長|理事長|代表挨拶|社長挨拶|理事長挨拶|トップメッセージ|greeting|message|president)/i.test(
      pageTarget
    )
  ) {
    return true;
  }

  if (
    REPRESENTATIVE_NAME_LABELS.some((regex) => regex.test(page.structuredText))
  ) {
    return true;
  }

  const pairs = extractPairs(page.html);
  return pairs.some((pair) =>
    REPRESENTATIVE_NAME_LABELS.some((regex) => regex.test(pair.label))
  );
}

function shouldProcessRepresentativePage(page: PageData) {
  const pageTarget = decodeURIComponent(
    `${page.finalUrl} ${page.title} ${page.h1}`
  );

  return (
    isRepresentativeOverviewPageTarget(pageTarget) ||
    hasRepresentativePageSignals(page)
  );
}

const ZIPCODE_LABELS = [
  /^郵便番号$/,
  /^〒$/,
  /^所在地〒$/,
  /^住所〒$/,
  /郵便番号/,
  /〒/,
];

const ADDRESS_LABELS = [
  /^所在地$/,
  /^住所$/,
  /^本社$/,
  /^本店$/,
  /^支店$/,
  /^営業所$/,
  /^工場$/,
  /^本社所在地$/,
  /^本店所在地$/,
  /^支店所在地$/,
  /^営業所所在地$/,
  /^工場所在地$/,
  /^事業所$/,
  /^事業所所在地$/,
  /^本社住所$/,
  /^本店住所$/,
  /^所在地・連絡先$/,
  /^アクセス$/,
  /所在地/,
  /住所/,
  /本社/,
  /本店/,
  /支店/,
  /営業所/,
  /工場/,
  /事業所/,
  /アクセス/,
];

const FAX_LABELS = [
  /^FAX$/i,
  /^FAX番号$/i,
  /^ＦＡＸ$/,
  /^ＦＡＸ番号$/,
  /fax/i,
  /ファックス/,
];

const ESTABLISHED_LABELS = [
  /^設立$/,
  /^創業$/,
  /^設立年月日$/,
  /^創立$/,
  /^設立日$/,
  /^会社設立$/,
  /^法人設立$/,
  /設立/,
  /創業/,
  /創立/,
];

const REPRESENTATIVE_NAME_LABELS = [
  /^代表者$/,
  /^代表者名$/,
  /^代表者氏名$/,
  /^代表氏名$/,
  /^代表取締役$/,
  /^代表取締役社長$/,
  /^代表取締役会長$/,
  /^取締役社長$/,
  /^代表社員$/,
  /^代表理事$/,
  /^理事長$/,
  /^社長$/,
  /^会長$/,
  /^代表$/,
  /^所長$/,
  /^センター長$/,
  /^学院長$/,
  /^校長$/,
  /^学長$/,
  /^施設長$/,
  /^室長$/,
  /代表者氏名/,
  /代表氏名/,
  /代表者/,
  /代表取締役/,
  /代表社員/,
  /代表理事/,
  /理事長/,
];

const CAPITAL_LABELS = [
  /^資本金$/,
  /^出資金$/,
  /^資本準備金$/,
  /^資本$/,
  /資本金/,
  /出資金/,
];

const EMPLOYEE_COUNT_LABELS = [
  /^従業員数$/,
  /^従業員$/,
  /^総従業員数$/,
  /^全従業員数$/,
  /^連結従業員数$/,
  /^単体従業員数$/,
  /^単独従業員数$/,
  /^個別従業員数$/,
  /^グループ従業員数$/,
  /^グループ社員数$/,
  /^グループ社員合計$/,
  /^社員数$/,
  /^社員合計$/,
  /^職員数$/,
  /^職員合計$/,
  /^スタッフ数$/,
  /^スタッフ人数$/,
  /^在籍スタッフ数$/,
  /^人数$/,
  /^総人数$/,
  /^在籍人数$/,
  /^人員構成$/,
  /^人員$/,
  /^人員数$/,
  /^総人員$/,
  /^メンバー数$/,
  /^就業人数$/,
  /^就業者数$/,
  /^従業員合計$/,
  /^常勤職員数$/,
  /^非常勤職員数$/,
  /^常勤社員数$/,
  /^非常勤社員数$/,
  /^従業員規模$/,
  /従業員数/,
  /従業員/,
  /総従業員数/,
  /全従業員数/,
  /連結従業員数/,
  /単体従業員数/,
  /単独従業員数/,
  /個別従業員数/,
  /グループ従業員数/,
  /グループ社員数/,
  /グループ社員合計/,
  /社員数/,
  /社員合計/,
  /職員数/,
  /職員合計/,
  /スタッフ数/,
  /スタッフ人数/,
  /在籍スタッフ数/,
  /人数/,
  /総人数/,
  /在籍人数/,
  /人員構成/,
  /人員数/,
  /人員/,
  /総人員/,
  /メンバー数/,
  /就業人数/,
  /就業者数/,
  /従業員合計/,
  /常勤職員数/,
  /非常勤職員数/,
  /常勤社員数/,
  /非常勤社員数/,
  /従業員規模/,
];

const EMPLOYEE_COUNT_PRIMARY_LABELS = [
  /^従業員数$/,
  /^総従業員数$/,
  /^全従業員数$/,
  /^連結従業員数$/,
  /^単体従業員数$/,
  /^単独従業員数$/,
  /^個別従業員数$/,
  /従業員数/,
];

const EMPLOYEE_COUNT_CONTEXT_REGEX =
  /(従業員|社員|職員|スタッフ|人員|メンバー|就業人数|就業者数|連結|単体|単独|個別|グループ社員|正社員|正職員|パート|アルバイト|契約社員|契約職員|派遣社員|派遣スタッフ|嘱託|常勤|非常勤)/i;

const EMPLOYEE_COUNT_DENY_REGEX =
  /(採用人数|募集人数|募集人員|採用予定人数|定員|参加人数|来場者数|利用者数|会員数|登録者数|フォロワー数|閲覧数|PV|座席数|病床数|車両数|台数|戸数|件数|店舗数|拠点数|事業所数|学校数|顧客数|取引先数|掲載社数|導入社数)/i;

const EMPLOYEE_COUNT_PAGE_KEYWORDS =
  /(従業員数|社員数|職員数|スタッフ数|人数|人員|人員数|総人員|従業員データ|社員データ|数字で見る|データで見る|会社データ|採用データ|就業人数|就業者数|staff|member|members|data|numbers|facts|ir|esg|sustainability|profile|outline)/i;

const EMPLOYEE_COUNT_OVERVIEW_PAGE_KEYWORDS =
  /(会社概要|企業情報|会社案内|会社情報|法人概要|企業概要|会社データ|会社基本情報|基本情報|company|corporate|about|outline|profile|overview|information)/i;

const EMPLOYEE_COUNT_SECTION_END_LABELS =
  /(会社名|商号|代表取締役|代表者|所在地|住所|電話番号|TEL|ＦＡＸ|FAX|従業員数|事業内容|設立|創業|資本金|アクセス|お問い合わせ|営業時間|受付時間|最寄りの交通機関|MAP)/i;

const BUSINESS_CONTENT_LABELS = [
  /^事業内容$/,
  /^業務内容$/,
  /^営業内容$/,
  /^サービス内容$/,
  /^事業概要$/,
  /^業容$/,
  /^取扱業務$/,
  /^取扱内容$/,
  /^取扱商品$/,
  /^主な事業$/,
  /^業務案内$/,
  /^サービス$/,
  /事業内容/,
  /業務内容/,
  /営業内容/,
  /サービス内容/,
  /事業概要/,
  /業容/,
];

const PHONE_LABELS = [
  /^電話番号$/i,
  /^電話$/i,
  /^TEL$/i,
  /^Tel$/i,
  /^tel$/i,
  /^本社電話番号$/i,
  /^代表電話$/i,
  /電話番号/i,
  /電話/i,
  /TEL/i,
];

const CONTACT_INFO_LABELS = [
  /^連絡先$/i,
  /^連絡先情報$/i,
  /連絡先/i,
];

const ADDRESS_STOP_WORDS =
  /(連絡先|アクセス方法|アクセス|TEL|Tel|tel|電話|FAX|Fax|fax|営業時間|定休日|MAP|Google|メール|E-mail|E-mai|Mail|お問合せ|お問い合わせ|Copyright|著作権|All Rights Reserved|業種|創業|設立|資本金|従業員数|取引先|Top Message|代表ご挨拶|Our Philosophy|Our Policy|History|沿革|Access|Contact|会社情報|事業紹介|設備紹介|お知らせ)/i;

const BUSINESS_STOP_WORDS =
  /(お問い合わせ|お問合せ|スタッフ紹介|仕事紹介|サービス紹介|NEWS|BLOG|COLUMN|会社概要|所在地|連絡先|TEL|FAX|営業時間|定休日)/i;

const BUSINESS_HINT_WORDS =
  /(事業|業務|販売|企画|請負|開発|分譲|売買|施工|設計|運営|介護|福祉|サービス|広告|製造|卸|小売|コンサル|代理店|保険|デイサービス)/i;

const PREFECTURE_REGEX_SOURCE =
  "(?:北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)";

const MUNICIPALITY_REGEX_SOURCE =
  "(?:[一-龠々ぁ-んァ-ヶー]+市[一-龠々ぁ-んァ-ヶー]*区?|[一-龠々ぁ-んァ-ヶー]+郡[一-龠々ぁ-んァ-ヶー]+町|[一-龠々ぁ-んァ-ヶー]+郡[一-龠々ぁ-んァ-ヶー]+村|[一-龠々ぁ-んァ-ヶー]+区|[一-龠々ぁ-んァ-ヶー]+町|[一-龠々ぁ-んァ-ヶー]+村)";

const ADDRESS_PREFIX_REGEX =
  /^(?:所在地|住所|本社所在地|本店所在地|支店所在地|営業所所在地|工場所在地|事業所所在地|本社住所|本店住所|本社|本店|支店|営業所|工場|事業所|(?:[^\s　]+(?:工場|支店|営業所|事業所)))\s*[:：]?\s*/i;

const ADDRESS_TRAILING_LABEL_REGEX =
  /\s*(?:営業部|総務部|生産部|管理部|品質管理部|技術部|開発部|工務部|経理部|人事部|企画部|購買部|物流部|業務部|連絡先|本社|本店|支店|営業所|工場|事業所)\s*$/i;

function normalizeDigits(value: string) {
  return value.replace(/[０-９]/g, (char) =>
    String.fromCharCode(char.charCodeAt(0) - 0xfee0)
  );
}

function normalizeSpace(value: string) {
  return value
    .replace(/\u00A0/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&#160;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/\r/g, " ")
    .replace(/\n/g, " ")
    .replace(/\t/g, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function stripHtml(html: string) {
  return normalizeSpace(
    html
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<!--[\s\S]*?-->/g, " ")
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(
        /<\/(p|div|section|article|li|ul|ol|tr|td|th|dd|dt|h1|h2|h3|h4|h5|h6)>/gi,
        "\n"
      )
      .replace(/<[^>]+>/g, " ")
  );
}

function firstMatch(text: string, regex: RegExp) {
  const matched = text.match(regex);
  return matched?.[1] ? normalizeSpace(matched[1]) : null;
}

function extractTitle(html: string) {
  return firstMatch(html, /<title[^>]*>([\s\S]*?)<\/title>/i) ?? "";
}

function extractH1(html: string) {
  return firstMatch(html, /<h1[^>]*>([\s\S]*?)<\/h1>/i) ?? "";
}

function extractMetaDescription(html: string) {
  return (
    firstMatch(
      html,
      /<meta[^>]+name=["']description["'][^>]+content=["']([\s\S]*?)["'][^>]*>/i
    ) ??
    firstMatch(
      html,
      /<meta[^>]+content=["']([\s\S]*?)["'][^>]+name=["']description["'][^>]*>/i
    ) ??
    ""
  );
}

function extractOgSiteName(html: string) {
  return (
    firstMatch(
      html,
      /<meta[^>]+property=["']og:site_name["'][^>]+content=["']([\s\S]*?)["'][^>]*>/i
    ) ??
    firstMatch(
      html,
      /<meta[^>]+content=["']([\s\S]*?)["'][^>]+property=["']og:site_name["'][^>]*>/i
    ) ??
    ""
  );
}

function extractJsonLdOrganizationName(html: string) {
  const scriptMatches = html.match(
    /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi
  );

  if (!scriptMatches) return "";

  for (const block of scriptMatches) {
    const name =
      firstMatch(
        block,
        /"@type"\s*:\s*"Organization"[\s\S]*?"name"\s*:\s*"([^"]+)"/i
      ) ??
      firstMatch(
        block,
        /"name"\s*:\s*"([^"]+)"[\s\S]*?"@type"\s*:\s*"Organization"/i
      );

    if (name) return name;
  }

  return "";
}

function normalizeSeedUrl(input: string) {
  const trimmed = normalizeSpace(input);
  if (trimmed === "") return null;

  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed;
  }

  return `https://${trimmed}`;
}

function normalizeUrlWithoutHash(url: string) {
  try {
    const parsed = new URL(url);
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return url;
  }
}

function extractLinks(html: string, baseUrl: string): LinkItem[] {
  const result: LinkItem[] = [];
  const seen = new Set<string>();
  const base = new URL(baseUrl);
  const regex = /<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;

  let match: RegExpExecArray | null = null;
  while ((match = regex.exec(html)) !== null) {
    const href = normalizeSpace(match[1] || "");
    const text = stripHtml(match[2] || "");

    if (!href || /^javascript:/i.test(href) || href.startsWith("#")) continue;
    if (/^(mailto:|tel:)/i.test(href)) continue;

    try {
      const resolved = normalizeUrlWithoutHash(new URL(href, base).toString());
      if (seen.has(resolved)) continue;
      seen.add(resolved);

      result.push({
        url: resolved,
        text,
        sameOrigin: new URL(resolved).origin === base.origin,
      });
    } catch {
      continue;
    }
  }

  return result;
}

function cleanCompanyName(value: string) {
  const normalized = normalizeSpace(value);
  if (!normalized) return null;

  const parts = normalized
    .split(/\s*[|｜]\s*|\s+[ー―_・]\s+/)
    .map((item) => normalizeSpace(item))
    .filter(
      (item) =>
        item !== "" &&
        !/^(top|home|ホーム|トップページ|latest news|最新情報|news|recruit|contact|会社情報|会社概要|企業情報|企業概要|アクセス|about|company|information)$/i.test(
          item
        )
    );

  const picked = parts.find((item) =>
    /株式会社|有限会社|合同会社|合資会社|合名会社|Inc\.?|INC\.?|Co\.\s*,?\s*Ltd\.?|CO\.\s*,?\s*LTD\.?/i.test(
      item
    )
  );

  return picked || parts[0] || null;
}

function extractCompanyNameFromText(text: string) {
  const normalized = normalizeSpace(text);
  if (!normalized) return null;

  const matches =
    normalized.match(
      /(?:株式会社|有限会社|合同会社|合資会社|合名会社)\s*[^\s　]{1,40}|[^\s　]{1,40}(?:株式会社|有限会社|合同会社|合資会社|合名会社)/g
    ) ?? [];

  for (const candidate of matches) {
    const cleaned = cleanCompanyName(candidate);
    if (cleaned) return cleaned;
  }

  return null;
}

function extractSingleLineLabeledValue(
  text: string,
  labelRegexList: RegExp[],
  _maxLength = 120
) {
  const lines = text
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "");

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];

    if (labelRegexList.some((regex) => regex.test(line))) {
      const sameLine = line.replace(
        new RegExp(
          `^(?:${buildLooseLabelPattern(labelRegexList)})\\s*[:：]?\\s*`,
          "i"
        ),
        ""
      );

      if (sameLine && sameLine !== line) {
        return normalizeSpace(sameLine);
      }

      const nextLine = lines[i + 1] ?? "";
      if (nextLine) {
        return nextLine;
      }
    }

    const matched = line.match(
      new RegExp(
        `^(?:${buildLooseLabelPattern(labelRegexList)})\\s*[:：]?\\s*(.+)$`,
        "i"
      )
    );

    if (matched?.[1]) {
      return normalizeSpace(matched[1]);
    }
  }

  return null;
}

function extractRepresentativeTableLikeValue(text: string) {
  const lines = text
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "");

  const labelPattern = buildLooseLabelPattern(REPRESENTATIVE_NAME_LABELS);

  for (const line of lines) {
    const matched = line.match(
      new RegExp(
        `^(?:${labelPattern})\\s*(?:\\||｜|¦|┃|‖|／|/|：|:)\\s*(.+)$`,
        "i"
      )
    );

    if (matched?.[1]) {
      return normalizeSpace(matched[1]);
    }
  }

  return null;
}

function extractEmployeeCountTableLikeValue(text: string) {
  const lines = text
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "");

  const labelPattern = buildLooseLabelPattern(EMPLOYEE_COUNT_LABELS);

  for (const line of lines) {
    const matched = line.match(
      new RegExp(
        `^(?:${labelPattern})\\s*(?:\\||｜|¦|┃|‖|／|/|：|:)\\s*(.+)$`,
        "i"
      )
    );

    if (matched?.[1]) {
      return normalizeSpace(matched[1]);
    }
  }

  return null;
}

function extractPrimaryEmployeeCountValueFromPairs(
  pairs: Array<{ label: string; value: string }>
) {
  for (const pair of pairs) {
    if (!EMPLOYEE_COUNT_PRIMARY_LABELS.some((regex) => regex.test(pair.label))) {
      continue;
    }

    const normalized = normalizeDigits(
      normalizeSpace(`${pair.label} ${pair.value}`)
    );

    const matched = normalized.match(/([0-9][0-9,]*)\s*(名|人)\b/i);
    if (matched?.[1] && matched?.[2]) {
      return `${matched[1]}${matched[2]}`;
    }
  }

  return null;
}

function extractPrimaryEmployeeCountValueFromText(text: string) {
  const lines = text
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "");

  const labelPattern = buildLooseLabelPattern(EMPLOYEE_COUNT_PRIMARY_LABELS);

  const sameLinePattern = new RegExp(
    `^(?:${labelPattern})\\s*(?:\\||｜|¦|┃|‖|／|/|：|:)?\\s*([0-9][0-9,]*)\\s*(名|人)\\b`,
    "i"
  );

  const labelOnlyPattern = new RegExp(
    `^(?:${labelPattern})\\s*(?:\\||｜|¦|┃|‖|／|/|：|:)?\\s*$`,
    "i"
  );

  for (let i = 0; i < lines.length; i += 1) {
    const currentLine = normalizeDigits(lines[i]);

    const sameLine = currentLine.match(sameLinePattern);
    if (sameLine?.[1] && sameLine?.[2]) {
      return `${sameLine[1]}${sameLine[2]}`;
    }

    if (!labelOnlyPattern.test(currentLine)) {
      continue;
    }

    for (let j = i + 1; j < Math.min(lines.length, i + 4); j += 1) {
      const nextLine = normalizeDigits(lines[j]);
      const nextMatched = nextLine.match(/^([0-9][0-9,]*)\s*(名|人)\b/i);

      if (nextMatched?.[1] && nextMatched?.[2]) {
        return `${nextMatched[1]}${nextMatched[2]}`;
      }
    }
  }

  return null;
}

function formatJapanesePhone(digits: string) {
  const normalized = digits.replace(/\D/g, "");

  if (!/^0\d{9,10}$/.test(normalized)) return null;

  if (/^(070|080|090|050|020)\d{8}$/.test(normalized)) {
    return `${normalized.slice(0, 3)}-${normalized.slice(3, 7)}-${normalized.slice(7)}`;
  }

  if (/^0120\d{6}$/.test(normalized)) {
    return `${normalized.slice(0, 4)}-${normalized.slice(4, 7)}-${normalized.slice(7)}`;
  }

  if (/^0570\d{6}$/.test(normalized)) {
    return `${normalized.slice(0, 4)}-${normalized.slice(4, 6)}-${normalized.slice(6)}`;
  }

  if (/^(03|06)\d{8}$/.test(normalized)) {
    return `${normalized.slice(0, 2)}-${normalized.slice(2, 6)}-${normalized.slice(6)}`;
  }

  if (normalized.length === 10) {
    return `${normalized.slice(0, 4)}-${normalized.slice(4, 6)}-${normalized.slice(6)}`;
  }

  if (normalized.length === 11) {
    return `${normalized.slice(0, 3)}-${normalized.slice(3, 7)}-${normalized.slice(7)}`;
  }

  return normalized;
}

function normalizePhone(value: string) {
  const normalized = normalizeDigits(normalizeSpace(value)).replace(/[()（）]/g, "-");
  if (!normalized) return null;

  const labeledMatch = normalized.match(
    /(?:TEL|Tel|tel|電話番号|電話|本社電話番号|代表電話|連絡先)\s*[:：.]?\s*(0[\d\s-]{8,13}\d)/i
  );
  if (labeledMatch?.[1]) {
    return formatJapanesePhone(labeledMatch[1]);
  }

  const candidates = Array.from(
    normalized.matchAll(/(^|[^\d])(0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4})(?=$|[^\d])/g),
    (match) => match[2]
  );

  for (const candidate of candidates) {
    const digits = candidate.replace(/\D/g, "");
    if (digits.length < 10 || digits.length > 11) continue;
    return formatJapanesePhone(digits);
  }

  return null;
}

function normalizeFax(value: string) {
  const normalized = normalizeDigits(normalizeSpace(value)).replace(/[()（）]/g, "-");
  if (!normalized) return null;

  const labeledMatch = normalized.match(
    /(?:FAX|ＦＡＸ|Fax|fax|ファックス)\s*[:：.]?\s*(0[\d\s-]{8,13}\d)/i
  );
  if (labeledMatch?.[1]) {
    return formatJapanesePhone(labeledMatch[1]);
  }

  if (/^[0-9\s-]+$/.test(normalized)) {
    return formatJapanesePhone(normalized);
  }

  return null;
}

function normalizeZipcode(value: string) {
  const normalized = normalizeDigits(normalizeSpace(value));
  if (!normalized) return null;

  const matched = normalized.match(/(?:〒\s*)?(\d{3})-?(\d{4})/);
  return matched ? `${matched[1]}-${matched[2]}` : null;
}

function pickAddressCore(value: string) {
  const normalized = normalizeDigits(normalizeSpace(value));
  if (!normalized) return null;

  const cleaned = normalized
    .replace(ADDRESS_PREFIX_REGEX, "")
    .replace(/(?:〒\s*)?\d{3}-?\d{4}\s*/g, "")
    .replace(/\s{2,}/g, " ")
    .trim();

  const stopSource =
    `${ADDRESS_STOP_WORDS.source}|営業部|総務部|生産部|管理部|品質管理部|技術部|開発部|工務部|経理部|人事部|企画部|購買部|物流部|業務部|本社|本店|支店|営業所|工場|事業所`;

  const prefectureMatch = cleaned.match(
    new RegExp(
      `((?:${PREFECTURE_REGEX_SOURCE}).{4,120}?)(?=(?:\\s*(?:${stopSource}))|$)`
    )
  );

  if (prefectureMatch?.[1]) {
    return prefectureMatch[1].replace(ADDRESS_TRAILING_LABEL_REGEX, "").trim();
  }

  const municipalityMatch = cleaned.match(
    new RegExp(
      `((?:${MUNICIPALITY_REGEX_SOURCE}).{4,120}?)(?=(?:\\s*(?:${stopSource}))|$)`
    )
  );

  if (municipalityMatch?.[1]) {
    return municipalityMatch[1].replace(ADDRESS_TRAILING_LABEL_REGEX, "").trim();
  }

  return cleaned.replace(ADDRESS_TRAILING_LABEL_REGEX, "").trim() || null;
}

function normalizeAddress(value: string) {
  const core = pickAddressCore(value);
  if (!core) return null;

  const cleaned = core
    .replace(/\s*(?:連絡先|アクセス方法|アクセス|TEL.*|Tel.*|tel.*|電話.*|FAX.*|Fax.*|fax.*|営業時間.*|定休日.*|MAP.*|Google.*|メール.*|E-mail.*|E-mai.*|Mail.*|お問合せ.*|お問い合わせ.*)$/i, "")
    .replace(ADDRESS_TRAILING_LABEL_REGEX, "")
    .replace(/\s{2,}/g, " ")
    .trim();

  if (!cleaned) return null;
  if (/@/.test(cleaned)) return null;
  if (/copyright|all rights reserved/i.test(cleaned)) return null;

  const addressLikePattern = new RegExp(
    `${PREFECTURE_REGEX_SOURCE}|${MUNICIPALITY_REGEX_SOURCE}`
  );

  if (!addressLikePattern.test(cleaned)) return null;

  return cleaned;
}

function extractAddressFromText(value: string) {
  const normalized = normalizeDigits(normalizeSpace(value));
  if (!normalized) return null;

  const withoutPrefix = normalized.replace(ADDRESS_PREFIX_REGEX, "").trim();

  const stopSource =
    `${ADDRESS_STOP_WORDS.source}|営業部|総務部|生産部|管理部|品質管理部|技術部|開発部|工務部|経理部|人事部|企画部|購買部|物流部|業務部|本社|本店|支店|営業所|工場|事業所`;

  const matched = withoutPrefix.match(
    new RegExp(
      `((?:〒\\s*)?\\d{3}-?\\d{4}\\s*)?((?:(?:${PREFECTURE_REGEX_SOURCE})|(?:${MUNICIPALITY_REGEX_SOURCE})).{4,120}?)(?=(?:\\s*(?:${stopSource}))|$)`
    )
  );

  if (!matched) return null;

  return normalizeAddress(`${matched[1] ?? ""} ${matched[2] ?? ""}`);
}

function normalizeEmail(value: string) {
  const normalized = normalizeSpace(value);
  const matched = normalized.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return matched ? matched[0] : null;
}

function stripHtmlKeepLineBreaks(html: string) {
  const decoded = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(
      /<\/(p|div|section|article|li|ul|ol|tr|td|th|dd|dt|h1|h2|h3|h4|h5|h6)>/gi,
      "\n"
    )
    .replace(/<[^>]+>/g, " ")
    .replace(/\u00A0/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&#160;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");

  return decoded
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "")
    .join("\n");
}

function uniqueNonEmpty(values: Array<string | null | undefined>) {
  const seen = new Set<string>();
  const result: string[] = [];

  for (const value of values) {
    const normalized = normalizeSpace(value ?? "");
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    result.push(normalized);
  }

  return result;
}

function normalizeEstablished(value: string) {
  const normalized = normalizeSpace(value);
  if (!normalized) return null;

  const matched = normalized.match(/([12][0-9]{3})[^0-9]{0,3}([0-9]{1,2})?/);
  if (!matched) return null;

  const year = matched[1];
  const month = matched[2] ? String(Number(matched[2])) : "";

  if (month) {
    return `${year}年${month}月`;
  }

  return `${year}年`;
}

type SimpleEmployeeCountCandidate = {
  value: string | null;
  sourceScore: number;
};

function hasEmployeeCountContext(value: string) {
  const normalized = normalizeDigits(normalizeSpace(value));
  if (!normalized) return false;
  return EMPLOYEE_COUNT_CONTEXT_REGEX.test(normalized);
}

function isLikelyEmployeeCountNoise(value: string) {
  const normalized = normalizeDigits(normalizeSpace(value));
  if (!normalized) return true;
  if (hasEmployeeCountContext(normalized)) return false;
  return EMPLOYEE_COUNT_DENY_REGEX.test(normalized);
}

function pushEmployeeCountCandidate(
  candidateMap: Map<string, number>,
  rawValue: string | null | undefined,
  score: number
) {
  if (!rawValue) return;
  if (isLikelyEmployeeCountNoise(rawValue)) return;

  const normalized = normalizeEmployeeCount(rawValue);
  if (!normalized) return;

  const current = candidateMap.get(normalized);
  if (current == null || score > current) {
    candidateMap.set(normalized, score);
  }
}

function collectEmployeeCountCandidatesFromText(
  text: string,
  baseScore = 0
): SimpleEmployeeCountCandidate[] {
  const candidateMap = new Map<string, number>();

  const lines = text
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "");

  const labelPattern = buildLooseLabelPattern(EMPLOYEE_COUNT_LABELS);

  const exactLabelOnlyPattern = new RegExp(`^(?:${labelPattern})$`, "i");
  const exactSameLinePattern = new RegExp(
    `^(?:${labelPattern})\\s*[:：]?\\s*(.+)$`,
    "i"
  );
  const exactTableLikePattern = new RegExp(
    `^(?:${labelPattern})\\s*(?:\\||｜|¦|┃|‖|／|/|：|:)\\s*(.+)$`,
    "i"
  );

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const next1 = lines[i + 1] ?? "";
    const next2 = lines[i + 2] ?? "";
    const next3 = lines[i + 3] ?? "";
    const next4 = lines[i + 4] ?? "";
    const next5 = lines[i + 5] ?? "";

    const sameLine = line.match(exactSameLinePattern);
    if (sameLine?.[1]) {
      pushEmployeeCountCandidate(
        candidateMap,
        `${line} ${sameLine[1]}`,
        1200 + baseScore
      );
    }

    const tableLike = line.match(exactTableLikePattern);
    if (tableLike?.[1]) {
      pushEmployeeCountCandidate(
        candidateMap,
        `${line} ${tableLike[1]}`,
        1190 + baseScore
      );
    }

    if (exactLabelOnlyPattern.test(line)) {
      pushEmployeeCountCandidate(
        candidateMap,
        `${line} ${next1}`,
        1180 + baseScore
      );
      pushEmployeeCountCandidate(
        candidateMap,
        `${line} ${next1} ${next2}`,
        1140 + baseScore
      );
      pushEmployeeCountCandidate(
        candidateMap,
        `${line} ${next1} ${next2} ${next3}`,
        1100 + baseScore
      );
      pushEmployeeCountCandidate(
        candidateMap,
        `${line} ${next2}`,
        1060 + baseScore
      );
      pushEmployeeCountCandidate(
        candidateMap,
        `${line} ${next3}`,
        1020 + baseScore
      );
      pushEmployeeCountCandidate(
        candidateMap,
        `${line} ${next4}`,
        980 + baseScore
      );
      pushEmployeeCountCandidate(
        candidateMap,
        `${line} ${next5}`,
        940 + baseScore
      );
    }

    const block2 = [line, next1].filter(Boolean).join(" ");
    const block3 = [line, next1, next2].filter(Boolean).join(" ");
    const block4 = [line, next1, next2, next3].filter(Boolean).join(" ");
    const block5 = [line, next1, next2, next3, next4].filter(Boolean).join(" ");

    if (hasEmployeeCountContext(line) && /\d/.test(normalizeDigits(line))) {
      pushEmployeeCountCandidate(candidateMap, line, 1040 + baseScore);
    }

    if (hasEmployeeCountContext(block2) && /\d/.test(normalizeDigits(block2))) {
      pushEmployeeCountCandidate(candidateMap, block2, 1020 + baseScore);
    }

    if (hasEmployeeCountContext(block3) && /\d/.test(normalizeDigits(block3))) {
      pushEmployeeCountCandidate(candidateMap, block3, 1000 + baseScore);
    }

    if (hasEmployeeCountContext(block4) && /\d/.test(normalizeDigits(block4))) {
      pushEmployeeCountCandidate(candidateMap, block4, 980 + baseScore);
    }

    if (hasEmployeeCountContext(block5) && /\d/.test(normalizeDigits(block5))) {
      pushEmployeeCountCandidate(candidateMap, block5, 960 + baseScore);
    }
  }

  return Array.from(candidateMap.entries())
    .map(([value, sourceScore]) => ({ value, sourceScore }))
    .sort((a, b) => b.sourceScore - a.sourceScore);
}

async function extractPdfTextFromArrayBuffer(buffer: ArrayBuffer) {
  try {
    const imported = await (
      new Function("return import('pdf-parse')")() as Promise<any>
    ).catch(() => null);

    const pdfParse = imported?.default ?? imported;
    if (typeof pdfParse !== "function") return null;

    const parsed = await pdfParse(Buffer.from(buffer));
    const text = String(parsed?.text ?? "");

    const normalized = text
      .split(/\r?\n/)
      .map((line) => normalizeSpace(line))
      .filter((line) => line !== "")
      .join("\n");

    return normalized || null;
  } catch {
    return null;
  }
}

function buildPdfPageData(
  requestedUrl: string,
  finalUrl: string,
  pdfText: string
): PageData {
  const title = decodeURIComponent(finalUrl.split("/").pop() || "document.pdf");

  return {
    requestedUrl,
    finalUrl,
    html: "",
    text: pdfText,
    structuredText: pdfText,
    title,
    h1: "",
    links: [],
  };
}

function escapeEmployeeCountRegex(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

const EMPLOYEE_COUNT_BOUNDARY_LABELS = [
  "会社名",
  "商号",
  "代表取締役",
  "代表者",
  "所在地",
  "住所",
  "電話番号",
  "TEL",
  "ＦＡＸ",
  "FAX",
  "従業員数",
  "従業員",
  "総従業員数",
  "全従業員数",
  "連結従業員数",
  "単体従業員数",
  "単独従業員数",
  "個別従業員数",
  "グループ従業員数",
  "グループ社員数",
  "グループ社員合計",
  "社員数",
  "社員合計",
  "職員数",
  "職員合計",
  "スタッフ数",
  "スタッフ人数",
  "在籍スタッフ数",
  "人数",
  "総人数",
  "在籍人数",
  "人員構成",
  "人員",
  "人員数",
  "総人員",
  "メンバー数",
  "就業人数",
  "就業者数",
  "従業員合計",
  "常勤職員数",
  "非常勤職員数",
  "常勤社員数",
  "非常勤社員数",
  "従業員規模",
  "事業内容",
  "設立",
  "創業",
  "資本金",
  "アクセス",
  "お問い合わせ",
  "営業時間",
  "受付時間",
  "最寄りの交通機関",
  "MAP",
] as const;

function buildEmployeeCountStructuredLines(text: string) {
  const originalLines = text
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "");

  const merged = normalizeSpace(originalLines.join(" "));
  if (!merged) {
    return originalLines;
  }

  const boundaryPattern = EMPLOYEE_COUNT_BOUNDARY_LABELS
    .map((label) => escapeEmployeeCountRegex(label))
    .join("|");

  const rebuiltLines = merged
    .split(new RegExp(`(?=(?:${boundaryPattern})\\s*[:：]?)`, "g"))
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "");

  return Array.from(new Set([...originalLines, ...rebuiltLines]));
}

function extractEmployeeCountInlineSnippet(text: string) {
  const merged = normalizeSpace(text);
  if (!merged) return null;

  const employeeLabelPattern = buildLooseLabelPattern(EMPLOYEE_COUNT_LABELS);

  const boundaryPattern = EMPLOYEE_COUNT_BOUNDARY_LABELS
    .map((label) => escapeEmployeeCountRegex(label))
    .join("|");

  const matched = merged.match(
    new RegExp(
      `((?:${employeeLabelPattern})\\s*[:：]?\\s*[\\s\\S]{0,80}?)(?=(?:${boundaryPattern})\\s*[:：]?|$)`,
      "i"
    )
  );

  return matched?.[1] ? normalizeSpace(matched[1]) : null;
}

function extractEmployeeCountSectionText(text: string) {
  const lines = buildEmployeeCountStructuredLines(text);

  const sections: string[] = [];
  const labelPattern = buildLooseLabelPattern(EMPLOYEE_COUNT_LABELS);

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];

    if (!EMPLOYEE_COUNT_LABELS.some((regex) => regex.test(line))) {
      continue;
    }

    const block: string[] = [line];

    for (let j = i + 1; j < Math.min(lines.length, i + 6); j += 1) {
      const nextLine = lines[j];

      if (
        nextLine !== "" &&
        EMPLOYEE_COUNT_SECTION_END_LABELS.test(nextLine) &&
        !EMPLOYEE_COUNT_LABELS.some((regex) => regex.test(nextLine))
      ) {
        break;
      }

      block.push(nextLine);
    }

    sections.push(block.join(" "));
  }

  const merged = normalizeSpace(lines.join(" "));
  const matched = merged.match(
    new RegExp(
      `((?:${labelPattern})\\s*[:：]?\\s*[\\s\\S]{0,60}?)(?=(?:${EMPLOYEE_COUNT_SECTION_END_LABELS.source})\\s*[:：]?|$)`,
      "i"
    )
  );

  if (matched?.[1]) {
    sections.push(normalizeSpace(matched[1]));
  }

  const inlineSnippet = extractEmployeeCountInlineSnippet(merged);
  if (inlineSnippet) {
    sections.push(inlineSnippet);
  }

  return Array.from(
    new Set(sections.map((section) => normalizeSpace(section)).filter(Boolean))
  );
}

function extractEmployeeCountFromPage(
  page: PageData,
  _sourceCompany?: string | null,
  _sourceAddress?: string | null
): SimpleEmployeeCountCandidate | null {
  const pageTarget = decodeURIComponent(
    `${page.finalUrl} ${page.title} ${page.h1}`
  );

  const boost =
    pageBoost(page.finalUrl) +
    (EMPLOYEE_COUNT_PAGE_KEYWORDS.test(pageTarget) ? 120 : 0) +
    (EMPLOYEE_COUNT_OVERVIEW_PAGE_KEYWORDS.test(pageTarget) ? 180 : 0);

  const pairs = extractPairs(page.html);
  const candidateMap = new Map<string, number>();

  const primaryPairValue = extractPrimaryEmployeeCountValueFromPairs(pairs);
  if (primaryPairValue) {
    pushEmployeeCountCandidate(
      candidateMap,
      `従業員数 ${primaryPairValue}`,
      1200 + boost
    );
  }

  const primaryStructuredValue = extractPrimaryEmployeeCountValueFromText(
    page.structuredText
  );
  if (primaryStructuredValue) {
    pushEmployeeCountCandidate(
      candidateMap,
      `従業員数 ${primaryStructuredValue}`,
      1180 + boost
    );
  }

  const primaryFlatValue =
    page.text !== page.structuredText
      ? extractPrimaryEmployeeCountValueFromText(page.text)
      : null;

  if (primaryFlatValue) {
    pushEmployeeCountCandidate(
      candidateMap,
      `従業員数 ${primaryFlatValue}`,
      1160 + boost
    );
  }

  const rebuiltStructuredText = buildEmployeeCountStructuredLines(
    page.structuredText
  ).join("\n");

  const primaryRebuiltValue = extractPrimaryEmployeeCountValueFromText(
    rebuiltStructuredText
  );
  if (primaryRebuiltValue) {
    pushEmployeeCountCandidate(
      candidateMap,
      `従業員数 ${primaryRebuiltValue}`,
      1120 + boost
    );
  }

  for (const pair of pairs) {
    if (!EMPLOYEE_COUNT_LABELS.some((regex) => regex.test(pair.label))) continue;

    pushEmployeeCountCandidate(
      candidateMap,
      `${pair.label} ${pair.value}`,
      340 + boost
    );
  }

  const labeledTextValue =
    extractSingleLineLabeledValue(page.structuredText, EMPLOYEE_COUNT_LABELS) ??
    (page.text !== page.structuredText
      ? extractSingleLineLabeledValue(page.text, EMPLOYEE_COUNT_LABELS)
      : null) ??
    extractSingleLineLabeledValue(rebuiltStructuredText, EMPLOYEE_COUNT_LABELS);

  if (labeledTextValue) {
    pushEmployeeCountCandidate(
      candidateMap,
      `従業員数 ${labeledTextValue}`,
      320 + boost
    );
  }

  const tableLikeValue =
    extractEmployeeCountTableLikeValue(page.structuredText) ??
    (page.text !== page.structuredText
      ? extractEmployeeCountTableLikeValue(page.text)
      : null) ??
    extractEmployeeCountTableLikeValue(rebuiltStructuredText);

  if (tableLikeValue) {
    pushEmployeeCountCandidate(
      candidateMap,
      `従業員数 ${tableLikeValue}`,
      330 + boost
    );
  }

  const inlineSnippet =
    extractEmployeeCountInlineSnippet(page.structuredText) ??
    (page.text !== page.structuredText
      ? extractEmployeeCountInlineSnippet(page.text)
      : null) ??
    extractEmployeeCountInlineSnippet(rebuiltStructuredText);

  if (inlineSnippet) {
    pushEmployeeCountCandidate(candidateMap, inlineSnippet, 380 + boost);
  }

  const sectionTexts = uniqueNonEmpty([
    ...extractEmployeeCountSectionText(page.structuredText),
    ...(page.text !== page.structuredText
      ? extractEmployeeCountSectionText(page.text)
      : []),
    ...extractEmployeeCountSectionText(rebuiltStructuredText),
  ]);

  for (const sectionText of sectionTexts) {
    pushEmployeeCountCandidate(candidateMap, sectionText, 360 + boost);

    const sectionCandidates = collectEmployeeCountCandidatesFromText(
      sectionText,
      boost
    );

    for (const candidate of sectionCandidates) {
      pushEmployeeCountCandidate(
        candidateMap,
        candidate.value,
        340 + candidate.sourceScore
      );
    }
  }

  const sourceTexts = uniqueNonEmpty([
    page.structuredText,
    page.text,
    rebuiltStructuredText,
  ]);

  sourceTexts.forEach((sourceText, sourceIndex) => {
    const sourceCandidates = collectEmployeeCountCandidatesFromText(
      sourceText,
      boost
    );

    const sourceBoost =
      sourceIndex === 0 ? 260 :
      sourceIndex === 1 ? 240 :
      220;

    for (const candidate of sourceCandidates) {
      pushEmployeeCountCandidate(
        candidateMap,
        candidate.value,
        sourceBoost + candidate.sourceScore
      );
    }
  });

  const footerText = stripHtmlKeepLineBreaks(extractFooterHtml(page.html));
  if (footerText) {
    const footerCandidates = collectEmployeeCountCandidatesFromText(
      footerText,
      boost
    );

    for (const candidate of footerCandidates) {
      pushEmployeeCountCandidate(
        candidateMap,
        candidate.value,
        220 + candidate.sourceScore
      );
    }

    const footerTableLikeValue = extractEmployeeCountTableLikeValue(footerText);
    if (footerTableLikeValue) {
      pushEmployeeCountCandidate(
        candidateMap,
        `従業員数 ${footerTableLikeValue}`,
        240 + boost
      );
    }
  }

  const bestCandidate = Array.from(candidateMap.entries())
    .map(([value, sourceScore]) => ({ value, sourceScore }))
    .sort((a, b) => b.sourceScore - a.sourceScore)[0];

  if (!bestCandidate) return null;

  return bestCandidate;
}

const OFFICE_HEADER_REGEX =
  /^(.{1,40}?(?:本社|本店|支店|営業所|工場|事業所|センター))(?:(?:\s+|　+)(.*))?$/;

const OFFICE_BLOCK_STOP_REGEX =
  /^(?:会社情報|業種|創業|設立|資本金|従業員数|取引先|Top Message|代表ご挨拶|Our Philosophy|経営理念|Our Policy|経営基本方針|History|沿革|Access|Contact|お見積|お気軽にご相談ください|事業紹介|設備紹介|お知らせ)$/i;

function mergeOfficeResults(offices: CrawlExtractedOffice[]) {
  const map = new Map<string, CrawlExtractedOffice>();

  for (const office of offices) {
    const key =
      office.office_name ||
      office.address_candidates[0] ||
      office.company ||
      `office-${map.size}`;

    const current = map.get(key);

    if (!current) {
      map.set(key, {
        office_name: office.office_name,
        company: office.company,
        phone_candidates: [...office.phone_candidates],
        fax_candidates: [...office.fax_candidates],
        email_candidates: [...office.email_candidates],
        zipcode_candidates: [...office.zipcode_candidates],
        address_candidates: [...office.address_candidates],
      });
      continue;
    }

    current.company = current.company ?? office.company;
    current.phone_candidates = uniqueNonEmpty([
      ...current.phone_candidates,
      ...office.phone_candidates,
    ]);
    current.fax_candidates = uniqueNonEmpty([
      ...current.fax_candidates,
      ...office.fax_candidates,
    ]);
    current.email_candidates = uniqueNonEmpty([
      ...current.email_candidates,
      ...office.email_candidates,
    ]);
    current.zipcode_candidates = uniqueNonEmpty([
      ...current.zipcode_candidates,
      ...office.zipcode_candidates,
    ]);
    current.address_candidates = uniqueNonEmpty([
      ...current.address_candidates,
      ...office.address_candidates,
    ]);
  }

  return Array.from(map.values());
}

function extractOfficeResults(
  pages: PageData[],
  companyName: string | null,
  fallback: Pick<CrawlExtractedFields, "phone" | "fax" | "email" | "zipcode" | "address">
) {
  const offices: CrawlExtractedOffice[] = [];

  for (const page of pages) {
    const lines = page.structuredText
      .split("\n")
      .map((line) => normalizeSpace(line))
      .filter((line) => line !== "");

    const blocks: Array<{ office_name: string; lines: string[] }> = [];
    let current: { office_name: string; lines: string[] } | null = null;

    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      const nextLine = lines[i + 1] ?? "";
      const matched = line.match(OFFICE_HEADER_REGEX);

      const looksLikeOfficeStart =
        !!matched &&
        [
          normalizeZipcode(line),
          normalizeZipcode(nextLine),
          extractAddressFromText(line),
          extractAddressFromText(nextLine),
        ].some(Boolean);

      if (looksLikeOfficeStart) {
        if (current) {
          blocks.push(current);
        }

        current = {
          office_name: normalizeSpace(matched[1]),
          lines: [line],
        };
        continue;
      }

      if (current && OFFICE_BLOCK_STOP_REGEX.test(line)) {
        blocks.push(current);
        current = null;
        continue;
      }

      if (current) {
        current.lines.push(line);
      }
    }

    if (current) {
      blocks.push(current);
    }

    for (const block of blocks) {
      const officeName = normalizeSpace(block.office_name);
      const company =
        companyName && officeName && !companyName.includes(officeName)
          ? `${companyName} ${officeName}`
          : companyName ?? officeName;

      const phoneCandidates = uniqueNonEmpty(
        block.lines
          .filter(
            (line) =>
              /(?:TEL|Tel|tel|電話)/i.test(line) &&
              !/(?:FAX|ＦＡＸ|Fax|fax)/i.test(line)
          )
          .map((line) => normalizePhone(line))
      );

      const faxCandidates = uniqueNonEmpty(
        block.lines
          .filter((line) => /(?:FAX|ＦＡＸ|Fax|fax)/i.test(line))
          .map((line) => normalizeFax(line))
      );

      const emailCandidates = uniqueNonEmpty(
        block.lines.map((line) => normalizeEmail(line))
      );

      const zipcodeCandidates = uniqueNonEmpty(
        block.lines.map((line) => normalizeZipcode(line))
      );

      const addressCandidates = uniqueNonEmpty(
        block.lines
          .map((line) => extractAddressFromText(line) ?? normalizeAddress(line))
          .filter((value): value is string => value !== null)
      ).slice(0, 1);

      if (
        phoneCandidates.length === 0 &&
        faxCandidates.length === 0 &&
        emailCandidates.length === 0 &&
        zipcodeCandidates.length === 0 &&
        addressCandidates.length === 0
      ) {
        continue;
      }

      offices.push({
        office_name: officeName,
        company,
        phone_candidates: phoneCandidates,
        fax_candidates: faxCandidates,
        email_candidates: emailCandidates,
        zipcode_candidates: zipcodeCandidates,
        address_candidates: addressCandidates,
      });
    }
  }

  const merged = mergeOfficeResults(offices);

  if (merged.length > 0) {
    return merged;
  }

  return [
    {
      office_name: null,
      company: companyName,
      phone_candidates: uniqueNonEmpty([fallback.phone]),
      fax_candidates: uniqueNonEmpty([fallback.fax]),
      email_candidates: uniqueNonEmpty([fallback.email]),
      zipcode_candidates: uniqueNonEmpty([fallback.zipcode]),
      address_candidates: uniqueNonEmpty([fallback.address]),
    },
  ].filter(
    (office) =>
      office.phone_candidates.length > 0 ||
      office.fax_candidates.length > 0 ||
      office.email_candidates.length > 0 ||
      office.zipcode_candidates.length > 0 ||
      office.address_candidates.length > 0
  );
}

function normalizeCapital(value: string) {
  const normalized = normalizeSpace(value);
  if (!normalized) return null;

  const matched = normalized.match(/[0-9,\.]+(?:億円|億|千万円|百万円|万円|円)/);
  return matched ? matched[0] : normalized;
}

function normalizeEmployeeCount(value: string) {
  const normalized = normalizeDigits(normalizeSpace(value));
  if (!normalized) return null;

  const text = normalized.replace(/[，]/g, ",");
  const PERSON_UNIT_PATTERN = "(?:名|人)";

  const PRIMARY_LABEL_PATTERN = buildLooseLabelPattern(
    EMPLOYEE_COUNT_PRIMARY_LABELS
  );

  const directPrimaryMatch = text.match(
    new RegExp(
      `(?:${PRIMARY_LABEL_PATTERN})\\s*(?:\\||｜|¦|┃|‖|／|/|：|:)?\\s*([0-9][0-9,]*)\\s*${PERSON_UNIT_PATTERN}`,
      "i"
    )
  );

  if (directPrimaryMatch?.[1]) {
    const directPrimaryCount = Number(
      directPrimaryMatch[1].replace(/,/g, "")
    );
    if (Number.isFinite(directPrimaryCount) && directPrimaryCount > 0) {
      return `${directPrimaryCount.toLocaleString()}名`;
    }
  }

  const directPersonOnlyMatch = text.match(
    new RegExp(`^([0-9][0-9,]*)\\s*${PERSON_UNIT_PATTERN}$`, "i")
  );

  if (directPersonOnlyMatch?.[1]) {
    const directPersonOnlyCount = Number(
      directPersonOnlyMatch[1].replace(/,/g, "")
    );
    if (Number.isFinite(directPersonOnlyCount) && directPersonOnlyCount > 0) {
      return `${directPersonOnlyCount.toLocaleString()}名`;
    }
  }

  const GROUP_PRIORITY_LABELS = [
    "連結",
    "consolidated",
    "CONSOLIDATED",
  ];

  const GROUP_FALLBACK_LABELS = [
    "単体",
    "単独",
    "個別",
    "individual",
    "INDIVIDUAL",
    "non-consolidated",
    "NON-CONSOLIDATED",
    "nonconsolidated",
    "NONCONSOLIDATED",
  ];

  const EMPLOYMENT_LABELS = [
    "正社員",
    "正職員",
    "社員",
    "職員",
    "パート",
    "アルバイト",
    "契約社員",
    "契約職員",
    "派遣社員",
    "派遣スタッフ",
    "嘱託",
    "嘱託社員",
    "臨時社員",
    "臨時職員",
    "常勤",
    "非常勤",
    "フルタイム",
    "短時間",
    "再雇用",
    "有期雇用",
    "無期雇用",
    "役員",
  ];

  const escapeRegex = (textValue: string) =>
    textValue.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

  const toCount = (numText: string | undefined) => {
    const num = Number((numText || "").replace(/,/g, ""));
    return Number.isFinite(num) ? num : null;
  };

  const formatCount = (num: number | null) =>
    num != null && num > 0 ? `${num.toLocaleString()}名` : null;

  const findPriorityCount = (labels: string[]) => {
    for (const label of labels) {
      const escaped = escapeRegex(label);

      const beforeMatch = text.match(
        new RegExp(
          `${escaped}[^0-9]{0,12}([0-9][0-9,]*)\\s*${PERSON_UNIT_PATTERN}`,
          "i"
        )
      );
      const beforeCount = toCount(beforeMatch?.[1]);
      if (beforeCount != null) return beforeCount;

      const afterMatch = text.match(
        new RegExp(
          `([0-9][0-9,]*)\\s*${PERSON_UNIT_PATTERN}[^0-9]{0,12}${escaped}`,
          "i"
        )
      );
      const afterCount = toCount(afterMatch?.[1]);
      if (afterCount != null) return afterCount;
    }

    return null;
  };

  const consolidatedCount = findPriorityCount(GROUP_PRIORITY_LABELS);
  if (consolidatedCount != null) {
    return formatCount(consolidatedCount);
  }

  const employmentPattern = new RegExp(
    `(?:${EMPLOYMENT_LABELS.map(escapeRegex).join("|")})[^0-9]{0,12}([0-9][0-9,]*)\\s*${PERSON_UNIT_PATTERN}`,
    "gi"
  );

  const employmentMatches = [...text.matchAll(employmentPattern)];

  if (employmentMatches.length >= 2) {
    const total = employmentMatches.reduce((sum, matched) => {
      const count = toCount(matched[1]);
      return sum + (count ?? 0);
    }, 0);

    if (total > 0) {
      return `${total.toLocaleString()}名`;
    }
  }

  const standaloneCount = findPriorityCount(GROUP_FALLBACK_LABELS);
  if (standaloneCount != null) {
    return formatCount(standaloneCount);
  }

  const exactLabelNumberOnlyMatch = text.match(
    new RegExp(
      `^(?:${buildLooseLabelPattern(EMPLOYEE_COUNT_LABELS)})\\s*[:：]?\\s*([0-9][0-9,]*)$`,
      "i"
    )
  );
  const exactLabelNumberOnlyCount = toCount(exactLabelNumberOnlyMatch?.[1]);
  if (exactLabelNumberOnlyCount != null) {
    return formatCount(exactLabelNumberOnlyCount);
  }

  const standalonePersonOnlyMatch = text.match(
    new RegExp(`^([0-9][0-9,]*)\\s*(?:名|人)$`, "i")
  );
  const standalonePersonOnlyCount = toCount(standalonePersonOnlyMatch?.[1]);
  if (standalonePersonOnlyCount != null) {
    return formatCount(standalonePersonOnlyCount);
  }

  const hasContext = hasEmployeeCountContext(text);

  if (!hasContext) {
    const pureNumber = text.match(/^[0-9][0-9,]*$/);
    if (pureNumber?.[0]) {
      const count = toCount(pureNumber[0]);
      return formatCount(count);
    }
    return null;
  }

  const personMatches = [
    ...text.matchAll(
      new RegExp(`([0-9][0-9,]*)\\s*${PERSON_UNIT_PATTERN}`, "g")
    ),
  ]
    .map((matched) => toCount(matched[1]))
    .filter((num): num is number => num != null);

  if (personMatches.length > 0) {
    return `${Math.max(...personMatches).toLocaleString()}名`;
  }

  const pureNumber = text.match(/^[0-9][0-9,]*$/);
  if (pureNumber?.[0]) {
    const count = toCount(pureNumber[0]);
    return formatCount(count);
  }

  return null;
}

function buildLooseLabelPattern(labelRegexList: RegExp[]) {
  return labelRegexList
    .map((regex) => regex.source.replace(/^\^/, "").replace(/\$$/, ""))
    .join("|");
}

function extractLabeledSectionText(
  text: string,
  labelRegexList: RegExp[],
  maxLength = 500
) {
  const normalized = normalizeSpace(text);
  if (!normalized) return null;

  const labelPattern = buildLooseLabelPattern(labelRegexList);
  if (!labelPattern) return null;

  const matched = normalized.match(
    new RegExp(
      `(?:${labelPattern})\\s*[:：]?\\s*([\\s\\S]{5,${maxLength}}?)(?=(?:${BUSINESS_STOP_WORDS.source})|$)`,
      "i"
    )
  );

  return matched?.[1] ? normalizeSpace(matched[1]) : null;
}

function isLikelyBusinessContent(value: string) {
  const normalized = normalizeSpace(value);
  if (!normalized) return false;
  if (normalized.length < 8) return false;

  if (
    /(お問い合わせ|お問合せ|スタッフ紹介|仕事紹介|サービス紹介|NEWS|BLOG|COLUMN|会社概要|所在地|連絡先)/i.test(
      normalized
    )
  ) {
    return false;
  }

  return BUSINESS_HINT_WORDS.test(normalized) || /[・●■◆]/.test(normalized);
}

function normalizeBusinessContent(value: string) {
  let normalized = normalizeSpace(value);
  if (!normalized) return null;

  normalized = normalized
    .replace(/^(事業内容|業務内容|営業内容|サービス内容|事業概要|業容|取扱業務|取扱内容|取扱商品|主な事業|業務案内|サービス)\s*[:：]?\s*/i, "")
    .replace(/\s*(?:所在地|連絡先|TEL|FAX|営業時間|定休日|お問い合わせ|お問合せ|会社概要).*$/i, "")
    .replace(/\s*・/g, "\n・")
    .replace(/\s*[●■◆◉○]\s*/g, "\n・")
    .replace(/\n{2,}/g, "\n")
    .trim();

  if (!isLikelyBusinessContent(normalized)) {
    return null;
  }

  return normalized.length > 300 ? `${normalized.slice(0, 300)}…` : normalized;
}

function hasHtmlForm(html: string) {
  return /<form\b[\s\S]*?<\/form>/i.test(html);
}

function extractFooterHtml(html: string) {
  const parts: string[] = [];

  const footerTag = html.match(/<footer[\s\S]*?<\/footer>/i)?.[0];
  if (footerTag) {
    parts.push(footerTag);
  }

  const footerBlocks = html.match(
    /<(?:div|section)[^>]+(?:id|class)=["'][^"']*(?:footer|foot|site-info|company-info|corp-info|contact)[^"']*["'][^>]*>[\s\S]*?<\/(?:div|section)>/gi
  );

  if (footerBlocks?.length) {
    parts.push(...footerBlocks);
  }

  return parts.join(" ");
}

function extractPairs(html: string) {
  const result: Array<{ label: string; value: string }> = [];

  const patterns = [
    /<tr[^>]*>\s*<(?:th|td)[^>]*>([\s\S]*?)<\/(?:th|td)>\s*<(?:th|td)[^>]*>([\s\S]*?)<\/(?:th|td)>[\s\S]*?<\/tr>/gi,
    /<dt[^>]*>([\s\S]*?)<\/dt>\s*<dd[^>]*>([\s\S]*?)<\/dd>/gi,
    /<(?:li|p|div|section)[^>]*>\s*([^<:：]{1,60})\s*[:：]\s*([\s\S]*?)<\/(?:li|p|div|section)>/gi,
    /<(?:div|p|li|section)[^>]*>\s*<(?:span|strong|b|div|dt|th)[^>]*>([\s\S]{1,60}?)<\/(?:span|strong|b|div|dt|th)>\s*[:：]?\s*<(?:span|em|strong|b|div|dd|td|p|li)[^>]*>([\s\S]*?)<\/(?:span|em|strong|b|div|dd|td|p|li)>/gi,
    /<(?:span|div|strong|b)[^>]+(?:class|id)=["'][^"']*(?:label|title|name|head|term|key)[^"']*["'][^>]*>([\s\S]{1,60}?)<\/(?:span|div|strong|b)>\s*<(?:span|div|p|dd|td)[^>]+(?:class|id)=["'][^"']*(?:value|data|body|desc|detail)[^"']*["'][^>]*>([\s\S]*?)<\/(?:span|div|p|dd|td)>/gi,
    /<(?:div|li|section|article)[^>]*>\s*<(?:div|p|span|strong|b)[^>]*>\s*([^<:：]{1,60})\s*<\/(?:div|p|span|strong|b)>\s*<(?:div|p|span|strong|b)[^>]*>\s*([\s\S]{1,240}?)\s*<\/(?:div|p|span|strong|b)>\s*<\/(?:div|li|section|article)>/gi,
    /<(?:div|section|article)[^>]*>\s*<(?:h2|h3|h4|p|div|span|strong|b)[^>]*>\s*([^<:：]{1,60})\s*<\/(?:h2|h3|h4|p|div|span|strong|b)>\s*(?:<a[^>]*>)?([\s\S]{1,240}?)(?:<\/a>)?\s*<\/(?:div|section|article)>/gi,
  ];

  for (const pattern of patterns) {
    let match: RegExpExecArray | null = null;
    while ((match = pattern.exec(html)) !== null) {
      const label = stripHtml(match[1] || "");
      const value = stripHtml(match[2] || "");

      if (!label || !value) continue;
      result.push({ label, value });
    }
  }

  return result;
}

function pickPairValue(
  pairs: Array<{ label: string; value: string }>,
  labelRegexList: RegExp[]
) {
  for (const pair of pairs) {
    if (labelRegexList.some((regex) => regex.test(pair.label))) {
      return pair.value;
    }
  }
  return null;
}

function pickRepresentativePairValue(
  pairs: Array<{ label: string; value: string }>
) {
  for (const pair of pairs) {
    if (!REPRESENTATIVE_NAME_LABELS.some((regex) => regex.test(pair.label))) {
      continue;
    }

    const candidate = normalizeRepresentativeCandidateForBest(pair.value);
    if (candidate) {
      return candidate;
    }
  }

  return null;
}

function addBest(
  best: Partial<Record<keyof CrawlExtractedFields, BestValue>>,
  key: keyof CrawlExtractedFields,
  value: string | null,
  score: number
) {
  if (!value) return;

  const current = best[key];
  if (!current || score > current.score) {
    best[key] = { value, score };
  }
}

function buildPageData(
  requestedUrl: string,
  finalUrl: string,
  html: string
): PageData {
  return {
    requestedUrl,
    finalUrl,
    html,
    text: stripHtml(html),
    structuredText: stripHtmlKeepLineBreaks(html),
    title: extractTitle(html),
    h1: extractH1(html),
    links: extractLinks(html, finalUrl),
  };
}

function buildOfficePageSnapshot(page: PageData): PageData {
  return {
    requestedUrl: page.requestedUrl,
    finalUrl: page.finalUrl,
    html: "",
    text: "",
    structuredText: page.structuredText,
    title: "",
    h1: "",
    links: [],
  };
}

function shouldCollectOfficePageSnapshot(page: PageData) {
  const target = decodeURIComponent(
    `${page.finalUrl} ${page.title} ${page.h1}`
  );
  const text = page.structuredText.slice(0, 4000);

  return (
    CONTACT_KEYWORDS.test(target) ||
    COMPANY_KEYWORDS.test(target) ||
    PHONE_LABELS.some((regex) => regex.test(text)) ||
    FAX_LABELS.some((regex) => regex.test(text)) ||
    ZIPCODE_LABELS.some((regex) => regex.test(text)) ||
    ADDRESS_LABELS.some((regex) => regex.test(text)) ||
    /@/.test(text) ||
    /〒\s*\d{3}-?\d{4}/.test(text)
  );
}

function pushOfficePageSnapshotIfNeeded(
  collectedPages: PageData[],
  page: PageData
) {
  if (!shouldCollectOfficePageSnapshot(page)) return;
  collectedPages.push(buildOfficePageSnapshot(page));
}

function shouldUseBrowserRenderedHtml(html: string, finalUrl = "") {
  const structuredText = stripHtmlKeepLineBreaks(html);

  const target = decodeURIComponent(
    `${finalUrl} ${structuredText.slice(0, 2000)} ${html.slice(0, 4000)}`
  );

  const hasClientRenderedSignals =
    /id=["']__next["']|id=["']root["']|id=["']app["']|data-reactroot|__NEXT_DATA__|_buildManifest|webpack|vite|nuxt/i.test(
      html
    );

  const hasTooLittleReadableText = structuredText.length < 200;

  const hasImportantPageHint =
    /(会社概要|企業情報|会社案内|会社情報|法人概要|企業概要|outline|profile|company|about|corporate|information|従業員数|従業員|社員数|職員数|スタッフ数|employee|staff|member|代表者|代表取締役|社長|会長|理事長|president|greeting|message|トップメッセージ|ご挨拶)/i.test(
      target
    );

  const hasThinImportantHtml =
    hasImportantPageHint && structuredText.length < 350;

  const hasRepresentativeHintButThinHtml =
    /(代表者|代表取締役|社長|会長|理事長|president|greeting|message|トップメッセージ|ご挨拶)/i.test(
      target
    ) && structuredText.length < 120;

  const hasBlockedOrPlaceholderHint =
    /(enable javascript|javascriptを有効|access denied|forbidden|cloudflare|security check|bot check|please wait|just a moment)/i.test(
      target
    ) && structuredText.length < 500;

  return (
    (hasClientRenderedSignals &&
      (
        hasTooLittleReadableText ||
        hasThinImportantHtml ||
        hasRepresentativeHintButThinHtml
      )) ||
    hasThinImportantHtml ||
    hasRepresentativeHintButThinHtml ||
    hasBlockedOrPlaceholderHint
  );
}

type BrowserPageLike = {
  setDefaultNavigationTimeout(timeout: number): void;
  setDefaultTimeout(timeout: number): void;
  goto(
    url: string,
    options: { waitUntil: "domcontentloaded"; timeout: number }
  ): Promise<unknown>;
  waitForLoadState(
    state: "networkidle",
    options: { timeout: number }
  ): Promise<unknown>;
  waitForTimeout(ms: number): Promise<unknown>;
  content(): Promise<string>;
  textContent(selector: string): Promise<string | null>;
  url(): string;
};

type BrowserLike = {
  newPage(options: {
    userAgent: string;
    locale: string;
  }): Promise<BrowserPageLike>;
  close(): Promise<void>;
};

type ChromiumLike = {
  launch(options: { headless: boolean }): Promise<BrowserLike>;
};

async function fetchPageWithBrowser(
  url: string,
  timeoutMs = 10000,
  runtimeOptions?: CrawlRuntimeOptions
): Promise<PageData | null> {
  throwIfCrawlShouldStop(runtimeOptions);

  let browser: BrowserLike | null = null;

  try {
    const imported = await (
      new Function("return import('playwright')")() as Promise<{
        chromium?: ChromiumLike;
      }>
    ).catch(() => null);

    const chromium = imported?.chromium;
    if (!chromium) return null;

    browser = await chromium.launch({ headless: true });

    const page = await browser.newPage({
      userAgent: "Mozilla/5.0 (compatible; MasterDataCrawler/1.0)",
      locale: "ja-JP",
    });

    page.setDefaultNavigationTimeout(timeoutMs);
    page.setDefaultTimeout(timeoutMs);

    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: timeoutMs,
    });

    await page
      .waitForLoadState("networkidle", {
        timeout: Math.min(timeoutMs, 4000),
      })
      .catch(() => {});

    await page.waitForTimeout(1500);

    throwIfCrawlShouldStop(runtimeOptions);

    const html = await page.content();
    const finalUrl = page.url() || url;

    const bodyText =
      (await page.textContent("body").catch(() => null)) ?? "";

    const normalizedBodyText = bodyText
      .split(/\r?\n/)
      .map((line) => normalizeSpace(line))
      .filter((line) => line !== "")
      .join("\n");

    const blockCheckText = [
      finalUrl,
      extractTitle(html),
      extractH1(html),
      normalizedBodyText,
    ].join("\n");

    if (looksLikeTemporaryBlockPage(blockCheckText)) {
      return null;
    }

    if (PDF_PAGE_REGEX.test(finalUrl) && normalizedBodyText !== "") {
      return buildPdfPageData(url, finalUrl, normalizedBodyText);
    }

    return buildPageData(url, finalUrl, html);
  } catch (error) {
    if (runtimeOptions?.shouldStop?.()) {
      throw new Error(CRAWL_PAUSED_ERROR_MESSAGE);
    }

    if (
      error instanceof Error &&
      error.name === "AbortError" &&
      runtimeOptions?.shouldStop?.()
    ) {
      throw new Error(CRAWL_PAUSED_ERROR_MESSAGE);
    }

    return null;
  } finally {
    if (browser) {
      await browser.close().catch(() => {});
    }
  }
}

async function fetchPage(
  url: string,
  timeoutMs = 10000,
  runtimeOptions?: CrawlRuntimeOptions
): Promise<PageData | null> {
  throwIfCrawlShouldStop(runtimeOptions);

  for (let attemptIndex = 0; attemptIndex < FETCH_RETRY_DELAYS_MS.length; attemptIndex += 1) {
    await waitForHostCooldown(url, runtimeOptions);

    const retryDelay = FETCH_RETRY_DELAYS_MS[attemptIndex];
    if (retryDelay > 0) {
      await sleepCrawl(retryDelay, runtimeOptions);
    }

    const controller = new AbortController();
    const timeoutId = setTimeout(() => {
      controller.abort();
    }, timeoutMs);

    const stopCheckId = setInterval(() => {
      if (runtimeOptions?.shouldStop?.()) {
        controller.abort();
      }
    }, 150);

    try {
      const response = await fetch(url, {
        redirect: "follow",
        signal: controller.signal,
        headers: {
          "User-Agent": "Mozilla/5.0 (compatible; MasterDataCrawler/1.0)",
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
          "Cache-Control": "no-cache",
          Pragma: "no-cache",
        },
        cache: "no-store",
      });

      throwIfCrawlShouldStop(runtimeOptions);

      if (!response.ok) {
        const browserPageData = await fetchPageWithBrowser(
          url,
          timeoutMs,
          runtimeOptions
        );

        if (browserPageData) {
          markHostFetchSuccess(browserPageData.finalUrl || url);
          return browserPageData;
        }

        if (
          isRetryableFetchStatus(response.status) &&
          attemptIndex < FETCH_RETRY_DELAYS_MS.length - 1
        ) {
          markHostTemporaryFailure(url, attemptIndex);
          continue;
        }

        return null;
      }

      const finalUrl = response.url || url;
      const contentType = response.headers.get("content-type") || "";

      if (
        contentType.toLowerCase().includes("application/pdf") ||
        PDF_PAGE_REGEX.test(finalUrl)
      ) {
        const pdfBuffer = await response.arrayBuffer();

        throwIfCrawlShouldStop(runtimeOptions);

        const pdfText = await extractPdfTextFromArrayBuffer(pdfBuffer);

        if (pdfText) {
          markHostFetchSuccess(finalUrl);
          return buildPdfPageData(url, finalUrl, pdfText);
        }

        const browserPageData = await fetchPageWithBrowser(
          finalUrl,
          timeoutMs,
          runtimeOptions
        );

        if (browserPageData) {
          markHostFetchSuccess(browserPageData.finalUrl || finalUrl);
          return browserPageData;
        }

        if (attemptIndex < FETCH_RETRY_DELAYS_MS.length - 1) {
          markHostTemporaryFailure(finalUrl, attemptIndex);
          continue;
        }

        return null;
      }

      if (!contentType.toLowerCase().includes("text/html")) {
        markHostFetchSuccess(finalUrl);
        return null;
      }

      const html = await response.text();

      throwIfCrawlShouldStop(runtimeOptions);

      const blockCheckText = [
        finalUrl,
        extractTitle(html),
        extractH1(html),
        stripHtmlKeepLineBreaks(html).slice(0, 2000),
      ].join("\n");

      if (looksLikeTemporaryBlockPage(blockCheckText)) {
        const browserPageData = await fetchPageWithBrowser(
          finalUrl,
          timeoutMs,
          runtimeOptions
        );

        if (browserPageData) {
          markHostFetchSuccess(browserPageData.finalUrl || finalUrl);
          return browserPageData;
        }

        if (attemptIndex < FETCH_RETRY_DELAYS_MS.length - 1) {
          markHostTemporaryFailure(finalUrl, attemptIndex);
          continue;
        }

        return null;
      }

      const rawPageData = buildPageData(url, finalUrl, html);

      if (shouldUseBrowserRenderedHtml(html, finalUrl)) {
        const browserPageData = await fetchPageWithBrowser(
          finalUrl,
          timeoutMs,
          runtimeOptions
        );

        if (browserPageData) {
          markHostFetchSuccess(browserPageData.finalUrl || finalUrl);
          return browserPageData;
        }
      }

      markHostFetchSuccess(finalUrl);
      return rawPageData;
    } catch (error) {
      if (runtimeOptions?.shouldStop?.()) {
        throw new Error(CRAWL_PAUSED_ERROR_MESSAGE);
      }

      if (
        error instanceof Error &&
        error.name === "AbortError" &&
        runtimeOptions?.shouldStop?.()
      ) {
        throw new Error(CRAWL_PAUSED_ERROR_MESSAGE);
      }

      const browserPageData = await fetchPageWithBrowser(
        url,
        timeoutMs,
        runtimeOptions
      );

      if (browserPageData) {
        markHostFetchSuccess(browserPageData.finalUrl || url);
        return browserPageData;
      }

      if (attemptIndex < FETCH_RETRY_DELAYS_MS.length - 1) {
        markHostTemporaryFailure(url, attemptIndex);
        continue;
      }

      return null;
    } finally {
      clearTimeout(timeoutId);
      clearInterval(stopCheckId);
    }
  }

  return null;
}

async function fetchTopPage(
  seedUrl: string,
  timeoutMs = 10000,
  runtimeOptions?: CrawlRuntimeOptions
): Promise<PageData | null> {
  throwIfCrawlShouldStop(runtimeOptions);

  const normalized = normalizeSeedUrl(seedUrl);
  if (!normalized) return null;

  const candidates = Array.from(
    new Set(
      /^https:\/\//i.test(normalized)
        ? [normalized, normalized.replace(/^https:\/\//i, "http://")]
        : /^http:\/\//i.test(normalized)
        ? [normalized, normalized.replace(/^http:\/\//i, "https://")]
        : [normalized]
    )
  );

  for (const candidate of candidates) {
    const page = await fetchPage(candidate, timeoutMs, runtimeOptions);
    if (page) return page;
  }

  return null;
}

type CrawlPageFocus =
  | "company_core"
  | "contact"
  | "representative"
  | "employee_count"
  | "business_content"
  | "permit";

function hasAnySelectedFields(
  selectedFieldSet: Set<CrawlSelectableFieldKey>,
  fields: CrawlSelectableFieldKey[]
) {
  return fields.some((field) => hasSelectedCrawlField(selectedFieldSet, field));
}

function getEnabledCrawlPageFocuses(
  selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  const focuses: CrawlPageFocus[] = [];

  if (
    hasAnySelectedFields(selectedFieldSet, [
      "company",
      "website_url",
      "established_date",
      "capital",
    ])
  ) {
    focuses.push("company_core");
  }

  if (
    hasSelectedOfficeFields(selectedFieldSet) ||
    hasSelectedCrawlField(selectedFieldSet, "form_url")
  ) {
    focuses.push("contact");
  }

  if (hasSelectedCrawlField(selectedFieldSet, "representative_name")) {
    focuses.push("representative");
  }

  if (hasSelectedCrawlField(selectedFieldSet, "employee_count")) {
    focuses.push("employee_count");
  }

  if (hasSelectedCrawlField(selectedFieldSet, "business_content")) {
    focuses.push("business_content");
  }

  if (hasSelectedPermitFields(selectedFieldSet)) {
    focuses.push("permit");
  }

  return focuses;
}

function shouldProcessFieldInFocus(
  selectedFieldSet: Set<CrawlSelectableFieldKey>,
  field: CrawlSelectableFieldKey,
  focus?: CrawlPageFocus
) {
  if (!hasSelectedCrawlField(selectedFieldSet, field)) return false;
  if (!focus) return true;

  switch (focus) {
    case "company_core":
      return (
        field === "company" ||
        field === "website_url" ||
        field === "established_date" ||
        field === "capital"
      );
    case "contact":
      return (
        field === "form_url" ||
        field === "phone" ||
        field === "fax" ||
        field === "email" ||
        field === "zipcode" ||
        field === "address"
      );
    case "representative":
      return field === "representative_name";
    case "employee_count":
      return field === "employee_count";
    case "business_content":
      return field === "business_content";
    default:
      return false;
  }
}

function shouldProcessPermitInFocus(
  selectedFieldSet: Set<CrawlSelectableFieldKey>,
  focus?: CrawlPageFocus
) {
  if (!hasSelectedPermitFields(selectedFieldSet)) return false;
  if (!focus) return true;
  return focus === "permit";
}

function getFocusScoreThreshold(focus: CrawlPageFocus) {
  switch (focus) {
    case "representative":
    case "employee_count":
      return -220;
    case "permit":
      return -120;
    default:
      return -20;
  }
}

function getFocusCandidatePageLimit(focus: CrawlPageFocus) {
  switch (focus) {
    case "representative":
      return 220;
    case "employee_count":
      return 260;
    case "business_content":
      return 40;
    case "contact":
      return 24;
    case "permit":
      return 80;
    case "company_core":
    default:
      return 24;
  }
}

function getFocusNestedCandidateLimit(focus: CrawlPageFocus) {
  switch (focus) {
    case "representative":
      return 120;
    case "employee_count":
      return 220;
    case "business_content":
      return 20;
    case "contact":
      return 16;
    case "permit":
      return 80;
    case "company_core":
    default:
      return 16;
  }
}

function getStrongCandidatePathsForFocus(focus: CrawlPageFocus) {
  switch (focus) {
    case "representative":
      return [
        "/company/",
        "/company.html",
        "/company/index.html",
        "/company/outline.html",
        "/company/profile.html",
        "/company/overview.html",
        "/company/message.html",
        "/company/greeting.html",
        "/company-profile/",
        "/company-profile.html",
        "/company-profile/index.html",
        "/message.html",
        "/greeting.html",
        "/president.html",
        "/topmessage.html",
      ];
    case "employee_count":
      return [
        "/company/",
        "/company",
        "/company/index.html",
        "/company.html",
        "/company/outline.html",
        "/company/profile.html",
        "/company/overview.html",
        "/company/outline/",
        "/company/profile/",
        "/company/overview/",
        "/company/data/",
        "/company/data.html",
        "/company/numbers/",
        "/company/numbers.html",
        "/company/info/",
        "/company/info.html",
        "/company-profile/",
        "/company-profile.html",
        "/company-profile/index.html",
        "/about/",
        "/about.html",
        "/about/company/",
        "/profile/",
        "/profile.html",
        "/profile/index.html",
        "/outline.html",
        "/overview.html",
        "/numbers/",
        "/numbers/index.html",
        "/data/",
        "/data/index.html",
      ];
    case "business_content":
      return [
        "/business/",
        "/business.html",
        "/service/",
        "/service.html",
        "/company/",
        "/company/profile.html",
        "/company/overview.html",
        "/about/",
      ];
    case "contact":
      return [
        "/contact/",
        "/contact.html",
        "/inquiry/",
        "/inquiry.html",
        "/access/",
        "/access.html",
        "/company/access.html",
        "/company/profile.html",
        "/company/overview.html",
      ];
    case "permit":
      return [
        "/company/",
        "/company/profile.html",
        "/company/overview.html",
        "/company/info/",
        "/license/",
        "/license.html",
        "/haken/",
        "/haken.html",
        "/jinzai/",
        "/jinzai.html",
      ];
    case "company_core":
    default:
      return [
        "/company/",
        "/company/index.html",
        "/company/outline.html",
        "/company/profile.html",
        "/company/overview.html",
        "/about/",
        "/about.html",
        "/profile/",
        "/profile.html",
        "/outline.html",
        "/overview.html",
      ];
  }
}

function shouldPreferActualLinksFirst(focus: CrawlPageFocus) {
  return (
    focus === "representative" ||
    focus === "employee_count" ||
    focus === "contact" ||
    focus === "business_content" ||
    focus === "permit"
  );
}

function getCandidateLinkScore(
  link: LinkItem,
  selectedFieldSet: Set<CrawlSelectableFieldKey>,
  focus: CrawlPageFocus
) {
  const target = decodeURIComponent(`${link.text} ${link.url}`);
  let score = 0;

  if (focus === "company_core") {
    if (COMPANY_KEYWORDS.test(target)) score += 220;
    if (
      /(会社概要|会社情報|企業情報|法人概要|企業概要|会社データ|会社基本情報|basic|outline|profile|company|corporate|about|gaiyou|overview|information)/i.test(
        target
      )
    ) {
      score += 260;
    }
    if (BUSINESS_KEYWORDS.test(target)) score += 60;
    if (CONTACT_KEYWORDS.test(target)) score += 30;
  }

  if (focus === "contact") {
    if (CONTACT_KEYWORDS.test(target)) score += 280;
    if (
      /(access|所在地|住所|本社|本店|支店|営業所|事業所|office|map)/i.test(
        target
      )
    ) {
      score += 220;
    }
    if (COMPANY_KEYWORDS.test(target)) score += 120;
  }

  if (focus === "representative") {
    if (COMPANY_KEYWORDS.test(target)) score += 140;
    if (STAFF_KEYWORDS.test(target)) score += 180;

    if (
      /(会社概要|会社情報|企業情報|法人概要|about|company|corporate|outline|profile|gaiyou|overview|information)/i.test(
        target
      )
    ) {
      score += 220;
    }

    if (
      /(代表挨拶|社長挨拶|理事長挨拶|所長挨拶|トップメッセージ|topmessage|greeting|message|president)/i.test(
        target
      )
    ) {
      score += 260;
    }

    if (isRepresentativeOverviewPageTarget(target)) {
      score += 520;
    }

    if (/(役員一覧|officer|executive)/i.test(target)) {
      score += 180;
    }

    if (
      /(代表|代表者|社長|会長|理事長|所長|センター長|学院長|校長|学長|施設長|室長|president|director|chief)/i.test(
        target
      )
    ) {
      score += 180;
    }

    if (
      /(staff|member|members|社員紹介|社員インタビュー|経営者略歴)/i.test(
        target
      ) &&
      !/(代表|代表者|社長|会長|理事長|president|director|chief|挨拶|メッセージ)/i.test(
        target
      )
    ) {
      score -= 120;
    }

    if (
      /(recruit|career|job|jobs|entry|新卒|中途|採用|shop|shopinfo|campus|店舗|商品|製品|service|faq)/i.test(
        target
      )
    ) {
      score -= 220;
    }

    if (
      /(blog|news|topics|column|interview|voice|story|success|diary)/i.test(
        target
      ) &&
      !/(代表|代表者|社長|会長|理事長|president|director|chief|挨拶|メッセージ)/i.test(
        target
      )
    ) {
      score -= 120;
    }

    if (
      /(?:当社|弊社|私たち|わたしたち|会社|企業|法人|事務所|株式会社[^\s　/]{0,30}|有限会社[^\s　/]{0,30}|合同会社[^\s　/]{0,30})について/i.test(
        target
      )
    ) {
      score += 300;
    }
  }

  if (focus === "employee_count") {
    if (EMPLOYEE_COUNT_OVERVIEW_PAGE_KEYWORDS.test(target)) score += 320;
    if (EMPLOYEE_COUNT_PAGE_KEYWORDS.test(target)) score += 300;
    if (COMPANY_KEYWORDS.test(target)) score += 120;
    if (BUSINESS_KEYWORDS.test(target)) score += 40;
  }

  if (focus === "business_content") {
    if (BUSINESS_KEYWORDS.test(target)) score += 320;
    if (
      /(service|services|business|事業|業務|サービス|営業内容|事業内容)/i.test(
        target
      )
    ) {
      score += 240;
    }
    if (COMPANY_KEYWORDS.test(target)) score += 140;
  }

  if (focus === "permit") {
    if (
      /(労働者派遣|有料職業紹介|許可番号|派遣|職業紹介|license|haken|jinzai)/i.test(
        target
      )
    ) {
      score += 340;
    }
    if (COMPANY_KEYWORDS.test(target)) score += 140;
    if (CONTACT_KEYWORDS.test(target)) score += 80;
  }

  if (focus !== "permit" && NEWS_BLOG_KEYWORDS.test(target)) score -= 120;
  if (focus !== "employee_count" && RECRUIT_KEYWORDS.test(target)) score -= 80;

  if (PDF_PAGE_REGEX.test(link.url)) {
    if (focus === "employee_count" && EMPLOYEE_COUNT_PAGE_KEYWORDS.test(target)) {
      score += 140;
    } else if (focus === "representative") {
      score += 20;
    } else {
      score -= 40;
    }
  }

  return score;
}

function pickCandidatePageUrls(
  topPage: PageData,
  selectedFieldSet: Set<CrawlSelectableFieldKey>,
  focus: CrawlPageFocus
) {
  const urls: string[] = [];
  const seen = new Set<string>();
  const base = new URL(topPage.finalUrl);

  const pushIfValid = (url: string) => {
    if (seen.has(url)) return;
    if (HTML_PAGE_DENY_EXT.test(url)) return;

    try {
      const parsed = new URL(url);
      if (parsed.origin !== base.origin) return;
      seen.add(url);
      urls.push(url);
    } catch {
      return;
    }
  };

  const strongPaths = getStrongCandidatePathsForFocus(focus);
  const sortedLinks = [...topPage.links]
    .map((link) => ({
      url: link.url,
      score: getCandidateLinkScore(link, selectedFieldSet, focus),
    }))
    .filter((item) => item.score > getFocusScoreThreshold(focus))
    .sort((a, b) => b.score - a.score);

  const scoredCommonPaths = [...COMMON_CANDIDATE_PATHS, ...strongPaths]
    .map((path) => {
      try {
        const url = new URL(path, base).toString();
        return {
          url,
          score: getCandidateLinkScore(
            {
              url,
              text: path,
              sameOrigin: true,
            },
            selectedFieldSet,
            focus
          ),
        };
      } catch {
        return null;
      }
    })
    .filter((item): item is { url: string; score: number } => item !== null)
    .sort((a, b) => b.score - a.score);

  if (shouldPreferActualLinksFirst(focus)) {
    for (const item of sortedLinks) {
      pushIfValid(item.url);
    }
    for (const item of scoredCommonPaths) {
      pushIfValid(item.url);
    }
  } else {
    for (const item of scoredCommonPaths) {
      pushIfValid(item.url);
    }
    for (const item of sortedLinks) {
      pushIfValid(item.url);
    }
  }

  return urls.slice(0, getFocusCandidatePageLimit(focus));
}

function hasSelectedOfficeFields(
  selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  return (
    hasSelectedCrawlField(selectedFieldSet, "phone") ||
    hasSelectedCrawlField(selectedFieldSet, "fax") ||
    hasSelectedCrawlField(selectedFieldSet, "email") ||
    hasSelectedCrawlField(selectedFieldSet, "zipcode") ||
    hasSelectedCrawlField(selectedFieldSet, "address")
  );
}

function isRepresentativeOnlyMode(
  _selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  return false;
}

function getRepresentativeEnoughScore(
  _selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  return 980;
}

function getCandidatePageLimit(
  selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  const needsRepresentative = hasSelectedCrawlField(
    selectedFieldSet,
    "representative_name"
  );
  const needsEmployeeCount = hasSelectedCrawlField(
    selectedFieldSet,
    "employee_count"
  );
  const needsPermit = hasSelectedPermitFields(selectedFieldSet);

  if (selectedFieldSet.size === 1) {
    if (hasSelectedCrawlField(selectedFieldSet, "form_url")) return 12;
    if (needsRepresentative) return 100;
    if (needsEmployeeCount) return 140;
    if (needsPermit) return 24;
    if (hasSelectedOfficeFields(selectedFieldSet)) return 20;
    return 16;
  }

  if (needsRepresentative && needsEmployeeCount) {
    return 160;
  }

  if (needsRepresentative) {
    return 100;
  }

  if (needsEmployeeCount) {
    return 140;
  }

  if (needsPermit) {
    return 32;
  }

  if (selectedFieldSet.size <= 3 && !hasSelectedOfficeFields(selectedFieldSet)) {
    return 24;
  }

  return 30;
}

function hasHighConfidenceEmployeeCount(
  best: Partial<Record<keyof CrawlExtractedFields, BestValue>>
) {
  return (
    !!best.employee_count?.value &&
    (best.employee_count?.score ?? 0) >= 380
  );
}

function canStopFetchingAdditionalPages(
  best: Partial<Record<keyof CrawlExtractedFields, BestValue>>,
  selectedFieldSet: Set<CrawlSelectableFieldKey>,
  focus: CrawlPageFocus
) {
  switch (focus) {
    case "company_core":
      return (
        (!hasSelectedCrawlField(selectedFieldSet, "company") ||
          !!best.company?.value) &&
        (!hasSelectedCrawlField(selectedFieldSet, "website_url") ||
          !!best.website_url?.value) &&
        (!hasSelectedCrawlField(selectedFieldSet, "established_date") ||
          !!best.established_date?.value) &&
        (!hasSelectedCrawlField(selectedFieldSet, "capital") ||
          !!best.capital?.value)
      );

    case "contact":
      return (
        (!hasSelectedCrawlField(selectedFieldSet, "form_url") ||
          !!best.form_url?.value) &&
        (!hasSelectedCrawlField(selectedFieldSet, "phone") ||
          !!best.phone?.value) &&
        (!hasSelectedCrawlField(selectedFieldSet, "fax") ||
          !!best.fax?.value) &&
        (!hasSelectedCrawlField(selectedFieldSet, "email") ||
          !!best.email?.value) &&
        (!hasSelectedCrawlField(selectedFieldSet, "zipcode") ||
          !!best.zipcode?.value) &&
        (!hasSelectedCrawlField(selectedFieldSet, "address") ||
          !!best.address?.value)
      );

    case "representative":
      return false;

    case "employee_count":
      return false;

    case "permit":
      return false;

    case "business_content":
      return (
        !hasSelectedCrawlField(selectedFieldSet, "business_content") ||
        !!best.business_content?.value
      );

    default:
      return false;
  }
}

function pickNestedCandidatePageUrls(
  page: PageData,
  selectedFieldSet: Set<CrawlSelectableFieldKey>,
  focus: CrawlPageFocus
) {
  const urls: string[] = [];
  const seen = new Set<string>();

  const sortedLinks = [...page.links]
    .map((link) => ({
      url: link.url,
      score: getCandidateLinkScore(link, selectedFieldSet, focus),
    }))
    .filter((item) => item.score > getFocusScoreThreshold(focus))
    .sort((a, b) => b.score - a.score);

  for (const item of sortedLinks) {
    if (seen.has(item.url)) continue;
    if (HTML_PAGE_DENY_EXT.test(item.url)) continue;
    seen.add(item.url);
    urls.push(item.url);
  }

  return urls.slice(0, getFocusNestedCandidateLimit(focus));
}

function pageBoost(url: string) {
  const target = decodeURIComponent(url);
  let score = 0;

  if (COMPANY_KEYWORDS.test(target)) score += 40;
  if (BUSINESS_KEYWORDS.test(target)) score += 30;
  if (CONTACT_KEYWORDS.test(target)) score += 20;
  if (STAFF_KEYWORDS.test(target)) score += 25;
  if (NEWS_BLOG_KEYWORDS.test(target)) score -= 50;
  if (RECRUIT_KEYWORDS.test(target)) score -= 40;

  return score;
}

function processPage(
  page: PageData,
  best: Partial<Record<keyof CrawlExtractedFields, BestValue>>,
  selectedFieldSet: Set<CrawlSelectableFieldKey>,
  sourceContext?: {
    company?: string | null;
    address?: string | null;
  },
  focus?: CrawlPageFocus
) {
  const boost = pageBoost(page.finalUrl);
  const pairs = extractPairs(page.html);

  if (shouldProcessFieldInFocus(selectedFieldSet, "website_url", focus)) {
    addBest(best, "website_url", page.finalUrl, 200);
  }

  if (shouldProcessFieldInFocus(selectedFieldSet, "company", focus)) {
    const textCompanyName = extractCompanyNameFromText(page.structuredText);
    const ogSiteName = cleanCompanyName(extractOgSiteName(page.html));
    const jsonLdName = cleanCompanyName(extractJsonLdOrganizationName(page.html));
    const h1Name = cleanCompanyName(page.h1);
    const titleName = cleanCompanyName(page.title);

    addBest(best, "company", ogSiteName, 120 + boost);
    addBest(best, "company", jsonLdName, 115 + boost);
    addBest(best, "company", h1Name, 110 + boost);
    addBest(best, "company", titleName, 90 + boost);
    addBest(best, "company", textCompanyName, 105 + boost);
  }

  if (shouldProcessFieldInFocus(selectedFieldSet, "form_url", focus)) {
    const formLink = page.links.find((link) => {
      const target = `${link.text} ${link.url}`;
      return CONTACT_KEYWORDS.test(target) && !HTML_PAGE_DENY_EXT.test(link.url);
    });

    if (hasHtmlForm(page.html) && CONTACT_KEYWORDS.test(page.finalUrl)) {
      addBest(best, "form_url", page.finalUrl, 150);
    }
    addBest(best, "form_url", formLink?.url ?? null, 120 + boost);
  }

  if (shouldProcessFieldInFocus(selectedFieldSet, "email", focus)) {
    const mailtoMatch = page.html.match(/mailto:([^"'?\s>]+)/i);

    addBest(
      best,
      "email",
      normalizeEmail(decodeURIComponent(mailtoMatch?.[1] || "")),
      150 + boost
    );
    addBest(best, "email", normalizeEmail(page.text), 80 + boost);
  }

  const telMatch = page.html.match(/tel:([^"'?\s>]+)/i);
  const phonePairValue = pickPairValue(pairs, PHONE_LABELS) ?? "";
  const faxPairValue = pickPairValue(pairs, FAX_LABELS) ?? "";
  const contactPairValue = pickPairValue(pairs, CONTACT_INFO_LABELS) ?? "";

  if (shouldProcessFieldInFocus(selectedFieldSet, "phone", focus)) {
    addBest(best, "phone", normalizePhone(phonePairValue), 220 + boost);
    addBest(best, "phone", normalizePhone(contactPairValue), 210 + boost);
    addBest(
      best,
      "phone",
      normalizePhone(decodeURIComponent(telMatch?.[1] || "")),
      150 + boost
    );
    addBest(best, "phone", normalizePhone(page.text), 40 + boost);
  }

  if (shouldProcessFieldInFocus(selectedFieldSet, "fax", focus)) {
    addBest(best, "fax", normalizeFax(faxPairValue), 220 + boost);
    addBest(best, "fax", normalizeFax(contactPairValue), 210 + boost);
  }

  const addressPairValue = pickPairValue(pairs, ADDRESS_LABELS) ?? "";
  const zipcodePairValue = pickPairValue(pairs, ZIPCODE_LABELS) ?? addressPairValue;
  const detectedAddress = extractAddressFromText(
    [addressPairValue, zipcodePairValue, page.text].filter(Boolean).join(" ")
  );

  if (shouldProcessFieldInFocus(selectedFieldSet, "zipcode", focus)) {
    addBest(best, "zipcode", normalizeZipcode(zipcodePairValue), 220 + boost);
    addBest(best, "zipcode", normalizeZipcode(addressPairValue), 210 + boost);
    addBest(best, "zipcode", normalizeZipcode(page.text), 40 + boost);
  }

  if (shouldProcessFieldInFocus(selectedFieldSet, "address", focus)) {
    addBest(best, "address", normalizeAddress(addressPairValue), 220 + boost);
    addBest(best, "address", detectedAddress, 215 + boost);
  }

  if (shouldProcessFieldInFocus(selectedFieldSet, "established_date", focus)) {
    const establishedPairValue = pickPairValue(pairs, ESTABLISHED_LABELS) ?? "";
    const establishedTextValue =
      extractSingleLineLabeledValue(page.structuredText, ESTABLISHED_LABELS) ?? "";

    addBest(
      best,
      "established_date",
      normalizeEstablished(establishedPairValue),
      170 + boost
    );
    addBest(
      best,
      "established_date",
      normalizeEstablished(establishedTextValue),
      165 + boost
    );
  }

  if (shouldProcessFieldInFocus(selectedFieldSet, "representative_name", focus)) {
    const representativeCandidates = collectRepresentativeCandidates(page);

    for (const candidate of representativeCandidates) {
      addBest(
        best,
        "representative_name",
        candidate.value,
        candidate.score + boost
      );
    }
  }

  if (shouldProcessFieldInFocus(selectedFieldSet, "capital", focus)) {
    const capitalPairValue = pickPairValue(pairs, CAPITAL_LABELS) ?? "";
    const capitalTextValue =
      extractSingleLineLabeledValue(page.structuredText, CAPITAL_LABELS) ?? "";

    addBest(
      best,
      "capital",
      normalizeCapital(capitalPairValue),
      200 + boost
    );
    addBest(
      best,
      "capital",
      normalizeCapital(capitalTextValue),
      195 + boost
    );
  }

  if (shouldProcessFieldInFocus(selectedFieldSet, "employee_count", focus)) {
    const employeeCountCandidate = extractEmployeeCountFromPage(
      page,
      sourceContext?.company ?? null,
      sourceContext?.address ?? null
    );

    addBest(
      best,
      "employee_count",
      employeeCountCandidate?.value ?? null,
      employeeCountCandidate?.sourceScore ?? 0
    );
  }

  if (shouldProcessFieldInFocus(selectedFieldSet, "business_content", focus)) {
    const businessPairValue =
      pickPairValue(pairs, BUSINESS_CONTENT_LABELS) ?? "";
    const businessSectionValue =
      extractLabeledSectionText(page.text, BUSINESS_CONTENT_LABELS, 600) ?? "";
    const businessMetaValue = extractMetaDescription(page.html);

    addBest(
      best,
      "business_content",
      normalizeBusinessContent(businessPairValue),
      220 + boost
    );
    addBest(
      best,
      "business_content",
      normalizeBusinessContent(businessSectionValue),
      210 + boost
    );
    addBest(
      best,
      "business_content",
      normalizeBusinessContent(businessMetaValue),
      90 + boost
    );
  }

  if (shouldProcessPermitInFocus(selectedFieldSet, focus)) {
    const permitCategory = extractPermitNumberCategory(
      [page.structuredText, page.text, page.title, page.h1].join("\n")
    );

    addBest(best, "permit_number", permitCategory, 230 + boost);
  }
}

export async function crawlCompanyWebsite(
  websiteUrl: string,
  selectedFields: CrawlSelectableFieldKey[] = DEFAULT_CRAWL_SELECTABLE_FIELDS,
  sourceContext?: {
    company?: string | null;
    address?: string | null;
  },
  runtimeOptions?: CrawlRuntimeOptions
): Promise<CrawlExtractedFields> {
  const selectedFieldSet = new Set(selectedFields);

  if (selectedFieldSet.size === 0) {
    return {
      company: null,
      website_url: null,
      form_url: null,
      phone: null,
      fax: null,
      email: null,
      zipcode: null,
      address: null,
      established_date: null,
      representative_name: null,
      representative_name_raw: null,
      representative_name_reason: null,
      representative_title: null,
      capital: null,
      employee_count: null,
      business_content: null,
      permit_number: null,
      offices: [],
    };
  }

  const best: Partial<Record<keyof CrawlExtractedFields, BestValue>> = {};
  const shouldExtractOffices = hasSelectedOfficeFields(selectedFieldSet);
  const collectedPages: PageData[] = [];
  const representativeEnoughScore =
    getRepresentativeEnoughScore(selectedFieldSet);
  const pageFetchTimeoutMs = 10000;

  throwIfCrawlShouldStop(runtimeOptions);

  const topPage = await fetchTopPage(
    websiteUrl,
    pageFetchTimeoutMs,
    runtimeOptions
  );

  if (!topPage) {
    return {
      company: null,
      website_url: hasSelectedCrawlField(selectedFieldSet, "website_url")
        ? normalizeSeedUrl(websiteUrl)
        : null,
      form_url: null,
      phone: null,
      fax: null,
      email: null,
      zipcode: null,
      address: null,
      established_date: null,
      representative_name: null,
      representative_name_raw: null,
      representative_name_reason: null,
      representative_title: null,
      capital: null,
      employee_count: null,
      business_content: null,
      permit_number: null,
      offices: [],
    };
  }

  if (shouldExtractOffices) {
    pushOfficePageSnapshotIfNeeded(collectedPages, topPage);
  }

  processPage(topPage, best, selectedFieldSet, sourceContext);

  const focusList = getEnabledCrawlPageFocuses(selectedFieldSet);

  const fetchFocusPages = async (
    focus: CrawlPageFocus,
    urls: string[],
    fetchedUrlSet: Set<string>,
    nestedCandidateUrls: string[],
    ignoreInitialStop = false
  ) => {
    let fetchedOnePage = false;

    for (const candidateUrl of urls) {
      throwIfCrawlShouldStop(runtimeOptions);

      if (
        (!ignoreInitialStop || fetchedOnePage) &&
        canStopFetchingAdditionalPages(best, selectedFieldSet, focus)
      ) {
        break;
      }

      const page = await fetchPage(
        candidateUrl,
        pageFetchTimeoutMs,
        runtimeOptions
      );

      if (!page) continue;
      if (fetchedUrlSet.has(page.finalUrl)) continue;

      fetchedUrlSet.add(page.finalUrl);

      if (shouldExtractOffices && focus === "contact") {
        pushOfficePageSnapshotIfNeeded(collectedPages, page);
      }

      processPage(page, best, selectedFieldSet, sourceContext, focus);
      fetchedOnePage = true;

      throwIfCrawlShouldStop(runtimeOptions);

      if (!canStopFetchingAdditionalPages(best, selectedFieldSet, focus)) {
        const nestedUrls = pickNestedCandidatePageUrls(
          page,
          selectedFieldSet,
          focus
        );

        for (const nestedUrl of nestedUrls) {
          if (nestedCandidateUrls.includes(nestedUrl)) continue;
          nestedCandidateUrls.push(nestedUrl);
        }
      }
    }
  };

  for (const focus of focusList) {
    const fetchedUrlSet = new Set<string>([topPage.finalUrl]);
    const nestedCandidateUrls: string[] = [];
    const rawCandidateUrls = pickCandidatePageUrls(
      topPage,
      selectedFieldSet,
      focus
    );

    if (focus === "representative") {
      const overviewCandidateUrls = rawCandidateUrls.filter((url) =>
        isRepresentativeOverviewPageTarget(url)
      );
      const fallbackCandidateUrls = rawCandidateUrls.filter(
        (url) => !isRepresentativeOverviewPageTarget(url)
      );

      for (const candidateUrl of overviewCandidateUrls) {
        throwIfCrawlShouldStop(runtimeOptions);

        if (canStopFetchingAdditionalPages(best, selectedFieldSet, focus)) {
          break;
        }

        const page = await fetchPage(
          candidateUrl,
          pageFetchTimeoutMs,
          runtimeOptions
        );

        if (!page) continue;
        if (fetchedUrlSet.has(page.finalUrl)) continue;

        if (!shouldProcessRepresentativePage(page)) continue;

        fetchedUrlSet.add(page.finalUrl);

        processPage(page, best, selectedFieldSet, sourceContext, focus);

        throwIfCrawlShouldStop(runtimeOptions);

        if (!canStopFetchingAdditionalPages(best, selectedFieldSet, focus)) {
          const nestedUrls = pickNestedCandidatePageUrls(
            page,
            selectedFieldSet,
            focus
          );

          for (const nestedUrl of nestedUrls) {
            if (nestedCandidateUrls.includes(nestedUrl)) continue;
            nestedCandidateUrls.push(nestedUrl);
          }
        }
      }

      const shouldFetchFallbackRepresentativePages = true;

      if (shouldFetchFallbackRepresentativePages) {
        for (const candidateUrl of fallbackCandidateUrls) {
          throwIfCrawlShouldStop(runtimeOptions);

          if (canStopFetchingAdditionalPages(best, selectedFieldSet, focus)) {
            break;
          }

          const page = await fetchPage(
            candidateUrl,
            pageFetchTimeoutMs,
            runtimeOptions
          );

          if (!page) continue;
          if (fetchedUrlSet.has(page.finalUrl)) continue;

          if (!shouldProcessRepresentativePage(page)) continue;

          fetchedUrlSet.add(page.finalUrl);

          processPage(page, best, selectedFieldSet, sourceContext, focus);

          throwIfCrawlShouldStop(runtimeOptions);

          if (!canStopFetchingAdditionalPages(best, selectedFieldSet, focus)) {
            const nestedUrls = pickNestedCandidatePageUrls(
              page,
              selectedFieldSet,
              focus
            );

            for (const nestedUrl of nestedUrls) {
              if (nestedCandidateUrls.includes(nestedUrl)) continue;
              nestedCandidateUrls.push(nestedUrl);
            }
          }
        }
      }
    } else {
      await fetchFocusPages(
        focus,
        rawCandidateUrls,
        fetchedUrlSet,
        nestedCandidateUrls
      );
    }

    if (!canStopFetchingAdditionalPages(best, selectedFieldSet, focus)) {
      for (const nestedUrl of nestedCandidateUrls.slice(
        0,
        getFocusNestedCandidateLimit(focus)
      )) {
        throwIfCrawlShouldStop(runtimeOptions);

        if (canStopFetchingAdditionalPages(best, selectedFieldSet, focus)) {
          break;
        }

        const nestedPage = await fetchPage(
          nestedUrl,
          pageFetchTimeoutMs,
          runtimeOptions
        );

        if (!nestedPage) continue;
        if (fetchedUrlSet.has(nestedPage.finalUrl)) continue;

        fetchedUrlSet.add(nestedPage.finalUrl);

        if (shouldExtractOffices && focus === "contact") {
          pushOfficePageSnapshotIfNeeded(collectedPages, nestedPage);
        }

        processPage(
          nestedPage,
          best,
          selectedFieldSet,
          sourceContext,
          focus
        );
      }
    }
  }

  const offices = shouldExtractOffices
    ? extractOfficeResults(collectedPages, best.company?.value ?? null, {
        phone: best.phone?.value ?? null,
        fax: best.fax?.value ?? null,
        email: best.email?.value ?? null,
        zipcode: best.zipcode?.value ?? null,
        address: best.address?.value ?? null,
      })
    : [];

  const representativeRawValue = hasSelectedCrawlField(
    selectedFieldSet,
    "representative_name"
  )
    ? best.representative_name?.value ?? null
    : null;

  return {
    company: hasSelectedCrawlField(selectedFieldSet, "company")
      ? best.company?.value ?? null
      : null,
    website_url: hasSelectedCrawlField(selectedFieldSet, "website_url")
      ? best.website_url?.value ?? null
      : null,
    form_url: hasSelectedCrawlField(selectedFieldSet, "form_url")
      ? best.form_url?.value ?? null
      : null,
    phone: hasSelectedCrawlField(selectedFieldSet, "phone")
      ? best.phone?.value ?? null
      : null,
    fax: hasSelectedCrawlField(selectedFieldSet, "fax")
      ? best.fax?.value ?? null
      : null,
    email: hasSelectedCrawlField(selectedFieldSet, "email")
      ? best.email?.value ?? null
      : null,
    zipcode: hasSelectedCrawlField(selectedFieldSet, "zipcode")
      ? best.zipcode?.value ?? null
      : null,
    address: hasSelectedCrawlField(selectedFieldSet, "address")
      ? best.address?.value ?? null
      : null,
    established_date: hasSelectedCrawlField(selectedFieldSet, "established_date")
      ? best.established_date?.value ?? null
      : null,
    representative_name: hasSelectedCrawlField(
      selectedFieldSet,
      "representative_name"
    )
      ? best.representative_name?.value ?? null
      : null,
    representative_name_raw: representativeRawValue,
    representative_name_reason: null,
    representative_title: null,
    capital: hasSelectedCrawlField(selectedFieldSet, "capital")
      ? best.capital?.value ?? null
      : null,
    employee_count: hasSelectedCrawlField(selectedFieldSet, "employee_count")
      ? best.employee_count?.value ?? null
      : null,
    business_content: hasSelectedCrawlField(selectedFieldSet, "business_content")
      ? best.business_content?.value ?? null
      : null,
    permit_number: best.permit_number?.value ?? null,
    offices,
  };
}