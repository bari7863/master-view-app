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
  | "business_content";

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
  /\.(pdf|zip|jpg|jpeg|png|gif|svg|webp|doc|docx|xls|xlsx|ppt|pptx)$/i;
const REPRESENTATIVE_BAD_VALUE_REGEX =
  /(基本情報|人事|新卒入社|会社概要(?:・沿革)?|会社案内|会社情報|企業情報|企業理念|経営理念|経営方針|経営者略歴|構成|声明|調査相談専用|番号|事業内容|製造|資本|者名|店舗詳細|営業所所在地|本社所在地|本社住所|採用\s*情報|採用情報|級建築士|登録番号|福山市|府中市|長野事業所|ショップ|shop|キャンパス|campus|一覧|大切にしていること|補償制度|道路工事|会社沿革|会社紹介動画|工場紹介|地域活動|協賛募集|募集要項|福利厚生|企業を知る|社員紹介|社員インタビュー|interview|voice|story|blog|news|topics|column|株式会社$|有限会社$|合同会社$|合資会社$|合名会社$|営業責任者|責任者\s*兼|生年月日|営\s*業|平成|昭和|令和)/i;
const REPRESENTATIVE_TRAILING_TITLE_REGEX =
  /\s*(?:代表取締役(?:社長|会長)?|取締役社長|取締役|代表社員|代表理事|理事長|社長|会長|CEO|COO|CFO|CTO|常務取締役?|専務取締役?|執行役員(?:専務|常務)?|常務|専務|相談役|名誉相談役|所長|センター長|学院長|校長|学長|施設長|室長).*$/i;

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
  /代表者/,
  /代表取締役/,
  /代表社員/,
  /代表理事/,
  /理事長/,
];

const REPRESENTATIVE_TITLE_LABELS = [
  /^役職$/,
  /^代表者役職$/,
  /^肩書$/,
  /^職位$/,
  /^代表取締役$/,
  /^代表取締役社長$/,
  /^代表取締役会長$/,
  /^取締役社長$/,
  /^代表社員$/,
  /^代表理事$/,
  /^理事長$/,
  /^社長$/,
  /^会長$/,
  /^CEO$/,
  /^COO$/,
  /^CFO$/,
  /^代表$/,
  /役職/,
  /肩書/,
  /職位/,
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
  /^グループ従業員数$/,
  /^社員数$/,
  /^職員数$/,
  /^スタッフ数$/,
  /^人数$/,
  /^総人数$/,
  /^在籍人数$/,
  /^人員構成$/,
  /^人員$/,
  /^メンバー数$/,
  /従業員数/,
  /従業員/,
  /総従業員数/,
  /全従業員数/,
  /連結従業員数/,
  /単体従業員数/,
  /単独従業員数/,
  /グループ従業員数/,
  /社員数/,
  /職員数/,
  /スタッフ数/,
  /人数/,
  /総人数/,
  /在籍人数/,
  /人員/,
];

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

const PREFECTURE_NAMES = [
  "北海道",
  "青森県",
  "岩手県",
  "宮城県",
  "秋田県",
  "山形県",
  "福島県",
  "茨城県",
  "栃木県",
  "群馬県",
  "埼玉県",
  "千葉県",
  "東京都",
  "神奈川県",
  "新潟県",
  "富山県",
  "石川県",
  "福井県",
  "山梨県",
  "長野県",
  "岐阜県",
  "静岡県",
  "愛知県",
  "三重県",
  "滋賀県",
  "京都府",
  "大阪府",
  "兵庫県",
  "奈良県",
  "和歌山県",
  "鳥取県",
  "島根県",
  "岡山県",
  "広島県",
  "山口県",
  "徳島県",
  "香川県",
  "愛媛県",
  "高知県",
  "福岡県",
  "佐賀県",
  "長崎県",
  "熊本県",
  "大分県",
  "宮崎県",
  "鹿児島県",
  "沖縄県",
] as const;

const BUSINESS_SECTION_STOP_WORDS =
  /(〒|住所|所在地|本社所在地|アクセス|Access|TEL|FAX|営業時間|定休日|会社概要|代表者|代表取締役|資本金|従業員数|社員数|スタッフ数|職員数|採用|求人|お知らせ|NEWS|BLOG|COLUMN)/i;

  const MUNICIPALITY_REGEX_SOURCE =
  "(?:[一-龠々ぁ-んァ-ヶー]+市[一-龠々ぁ-んァ-ヶー]*区?|[一-龠々ぁ-んァ-ヶー]+郡[一-龠々ぁ-んァ-ヶー]+町|[一-龠々ぁ-んァ-ヶー]+郡[一-龠々ぁ-んァ-ヶー]+村|[一-龠々ぁ-んァ-ヶー]+区|[一-龠々ぁ-んァ-ヶー]+町|[一-龠々ぁ-んァ-ヶー]+村)";

const ADDRESS_PREFIX_REGEX =
  /^(?:所在地|住所|本社所在地|本店所在地|支店所在地|営業所所在地|工場所在地|事業所所在地|本社住所|本店住所|本社|本店|支店|営業所|工場|事業所|(?:[^\s　]+(?:工場|支店|営業所|事業所)))\s*[:：]?\s*/i;

const ADDRESS_TRAILING_LABEL_REGEX =
  /\s*(?:営業部|総務部|生産部|管理部|品質管理部|技術部|開発部|工務部|経理部|人事部|企画部|購買部|物流部|業務部|連絡先|本社|本店|支店|営業所|工場|事業所)\s*$/i;

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
      .replace(/<\/(p|div|section|article|li|ul|ol|tr|td|th|dd|dt|h1|h2|h3|h4|h5|h6)>/gi, "\n")
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
      firstMatch(block, /"@type"\s*:\s*"Organization"[\s\S]*?"name"\s*:\s*"([^"]+)"/i) ??
      firstMatch(block, /"name"\s*:\s*"([^"]+)"[\s\S]*?"@type"\s*:\s*"Organization"/i);

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

    if (!href || /^javascript:/i.test(href) || href.startsWith("#")) {
      continue;
    }

    if (/^(mailto:|tel:)/i.test(href)) {
      continue;
    }

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
    /株式会社|有限会社|合同会社|合資会社|合名会社|Inc\.?|INC\.?|Co\.\s*,?\s*Ltd\.?|CO\.\s*,?\s*LTD\.?/i.test(item)
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
  maxLength = 120
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

function extractRepresentativeSingleLineValue(text: string) {
  const lines = text
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "");

  const exactLabelOnlyPattern =
    /^(?:代表者名?|代表者|代表取締役(?:社長|会長)?|取締役社長|代表社員|代表理事|理事長|社長|会長|所長|センター長|学院長|校長|学長|施設長|室長|役員(?!一覧|紹介))$/i;

  const exactSameLinePattern =
    /^(?:代表者名?|代表者|代表取締役(?:社長|会長)?|取締役社長|代表社員|代表理事|理事長|社長|会長|所長|センター長|学院長|校長|学長|施設長|室長|役員(?!一覧|紹介))\s*[:：]?\s*(.+)$/i;

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];

    const sameLine = line.match(exactSameLinePattern);
    if (sameLine?.[1]) {
      const candidate = normalizeRepresentativeName(sameLine[1], {
        allowCompactSingleToken: true,
      });
      if (candidate) return candidate;
    }

    if (exactLabelOnlyPattern.test(line)) {
      const nextLine = lines[i + 1] ?? "";
      const nextCandidate = normalizeRepresentativeName(nextLine, {
        allowCompactSingleToken: true,
      });
      if (nextCandidate) return nextCandidate;

      const nextNextLine = lines[i + 2] ?? "";
      const nextNextCandidate = normalizeRepresentativeName(nextNextLine, {
        allowCompactSingleToken: true,
      });
      if (nextNextCandidate) return nextNextCandidate;
    }
  }

  return null;
}

function normalizeDigits(value: string) {
  return value.replace(/[０-９]/g, (char) =>
    String.fromCharCode(char.charCodeAt(0) - 0xfee0)
  );
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

  const area3Prefixes = [
    "011", "015", "017", "018", "019", "022", "023", "024", "025", "026",
    "027", "028", "029", "042", "043", "044", "045", "046", "047", "048",
    "049", "052", "053", "054", "055", "058", "059", "072", "073", "075",
    "076", "077", "078", "079", "082", "083", "084", "086", "087", "088",
    "089", "092", "093", "095", "096", "097", "098", "099",
  ];

  if (normalized.length === 10 && area3Prefixes.some((prefix) => normalized.startsWith(prefix))) {
    return `${normalized.slice(0, 3)}-${normalized.slice(3, 6)}-${normalized.slice(6)}`;
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
    if (/^\d{3}\d{4}$/.test(digits)) continue;
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

type EmployeeCountContextHints = {
  sourceCompany: string | null;
  sourceAddress: string | null;
  officeNames: string[];
  prefecture: string | null;
  city: string | null;
};

type EmployeeCountCandidate = {
  value: string;
  count: number;
  year: number | null;
  month: number | null;
  yearMonth: number | null;
  officeName: string | null;
  prefecture: string | null;
  city: string | null;
  officeMatched: boolean;
  prefectureMatched: boolean;
  cityMatched: boolean;
  consolidated: boolean;
  standalone: boolean;
  sourceScore: number;
};

const OFFICE_NAME_IN_TEXT_REGEX =
  /[^\s　]{1,40}?(?:本社|本店|支社|支店|営業所|工場|事業所|センター)/g;

function parseEmployeeCountNumber(value: string | null) {
  if (!value) return null;
  const matched = normalizeDigits(value).match(/([0-9][0-9,]*)\s*(?:名|人)/);
  if (!matched?.[1]) return null;

  const num = Number(matched[1].replace(/,/g, ""));
  return Number.isFinite(num) ? num : null;
}

function extractPrefectureName(value: string | null) {
  const normalized = normalizeDigits(normalizeSpace(value ?? ""));
  if (!normalized) return null;

  const matched = normalized.match(new RegExp(PREFECTURE_REGEX_SOURCE));
  return matched?.[0] ? normalizeSpace(matched[0]) : null;
}

function extractCityName(value: string | null) {
  const normalized = normalizeDigits(normalizeSpace(value ?? ""));
  if (!normalized) return null;

  const matched = normalized.match(new RegExp(MUNICIPALITY_REGEX_SOURCE));
  return matched?.[0] ? normalizeSpace(matched[0]) : null;
}

function normalizeOfficeName(value: string) {
  return normalizeDigits(normalizeSpace(value));
}

function extractOfficeNamesFromValue(value: string | null) {
  const normalized = normalizeDigits(normalizeSpace(value ?? ""));
  if (!normalized) return [];

  const matches = normalized.match(OFFICE_NAME_IN_TEXT_REGEX) ?? [];
  return uniqueNonEmpty(matches.map((item) => normalizeOfficeName(item)));
}

function buildEmployeeCountContextHints(
  sourceCompany?: string | null,
  sourceAddress?: string | null
): EmployeeCountContextHints {
  return {
    sourceCompany: normalizeSpace(sourceCompany ?? "") || null,
    sourceAddress: normalizeSpace(sourceAddress ?? "") || null,
    officeNames: extractOfficeNamesFromValue(sourceCompany ?? null),
    prefecture: extractPrefectureName(sourceAddress ?? null),
    city: extractCityName(sourceAddress ?? null),
  };
}

function dedupeEmployeeCountCandidates(candidates: EmployeeCountCandidate[]) {
  const map = new Map<string, EmployeeCountCandidate>();

  for (const candidate of candidates) {
    const key = [
      candidate.value,
      candidate.yearMonth ?? "",
      candidate.officeName ?? "",
      candidate.prefecture ?? "",
      candidate.city ?? "",
      candidate.officeMatched ? "1" : "0",
      candidate.prefectureMatched ? "1" : "0",
      candidate.cityMatched ? "1" : "0",
    ].join("|");

    const current = map.get(key);
    if (!current || candidate.sourceScore > current.sourceScore) {
      map.set(key, candidate);
    }
  }

  return Array.from(map.values());
}

function buildEmployeeCountCandidate(
  count: number,
  year: number | null,
  month: number | null,
  officeName: string | null,
  prefecture: string | null,
  city: string | null,
  officeMatched: boolean,
  prefectureMatched: boolean,
  cityMatched: boolean,
  consolidated: boolean,
  standalone: boolean,
  sourceScore: number
): EmployeeCountCandidate {
  return {
    value: `${count.toLocaleString()}名`,
    count,
    year,
    month,
    yearMonth:
      year != null
        ? Number(`${year}${String(month ?? 12).padStart(2, "0")}`)
        : null,
    officeName,
    prefecture,
    city,
    officeMatched,
    prefectureMatched,
    cityMatched,
    consolidated,
    standalone,
    sourceScore,
  };
}

function extractEmployeeCountCandidatesFromSnippet(
  snippet: string,
  baseScore: number,
  hints: EmployeeCountContextHints
) {
  const normalized = normalizeDigits(normalizeSpace(snippet)).replace(/[，]/g, ",");
  if (!normalized) return [];

  const officeNamesInSnippet = extractOfficeNamesFromValue(normalized);
  const officeName = officeNamesInSnippet[0] ?? null;
  const officeMatched =
    officeNamesInSnippet.length > 0 &&
    officeNamesInSnippet.some((name) => hints.officeNames.includes(name));

  const prefecture = extractPrefectureName(normalized);
  const city = extractCityName(normalized);
  const prefectureMatched =
    !!prefecture && !!hints.prefecture && prefecture === hints.prefecture;
  const cityMatched = !!city && !!hints.city && city === hints.city;

  const consolidated = /(?:連結|consolidated)/i.test(normalized);
  const standalone =
    /(?:単体|単独|個別|individual|non-consolidated|nonconsolidated)/i.test(
      normalized
    );

  const candidates: EmployeeCountCandidate[] = [];

  const pushCandidate = (
    count: number | null,
    year: number | null,
    month: number | null,
    sourceAdjust: number
  ) => {
    if (count == null || count <= 0) return;

    let score = baseScore + sourceAdjust;

    if (officeMatched) score += 400;
    if (cityMatched) score += 180;
    if (prefectureMatched) score += 100;

    if (!officeMatched && hints.officeNames.length > 0 && officeName) score -= 260;
    if (!cityMatched && hints.city && city) score -= 100;
    if (!prefectureMatched && hints.prefecture && prefecture) score -= 50;

    if (consolidated) score += 20;
    if (standalone) score += 15;

    candidates.push(
      buildEmployeeCountCandidate(
        count,
        year,
        month,
        officeName,
        prefecture,
        city,
        officeMatched,
        prefectureMatched,
        cityMatched,
        consolidated,
        standalone,
        score
      )
    );
  };

  const yearCountMatches = Array.from(
    normalized.matchAll(
      /([12][0-9]{3})\s*年(?:\s*([0-9]{1,2})\s*月)?[^0-9]{0,16}(?:現在|時点|末時点|末)?[^0-9]{0,16}([0-9][0-9,]*)\s*(?:名|人)/g
    )
  );

  for (const match of yearCountMatches) {
    const year = Number(match[1]);
    const month = match[2] ? Number(match[2]) : 12;
    const count = Number((match[3] || "").replace(/,/g, ""));
    pushCandidate(
      Number.isFinite(count) ? count : null,
      Number.isFinite(year) ? year : null,
      Number.isFinite(month) ? month : null,
      80
    );
  }

  const labelPattern = buildLooseLabelPattern(EMPLOYEE_COUNT_LABELS);

  const labelAfterMatches = Array.from(
    normalized.matchAll(
      new RegExp(
        `(?:${labelPattern})\\s*[:：]?\\s*([0-9][0-9,]*)\\s*(?:名|人)`,
        "gi"
      )
    )
  );

  for (const match of labelAfterMatches) {
    const count = Number((match[1] || "").replace(/,/g, ""));
    pushCandidate(Number.isFinite(count) ? count : null, null, null, 60);
  }

  const labelBeforeMatches = Array.from(
    normalized.matchAll(
      new RegExp(
        `([0-9][0-9,]*)\\s*(?:名|人)[^0-9]{0,16}(?:${labelPattern})`,
        "gi"
      )
    )
  );

  for (const match of labelBeforeMatches) {
    const count = Number((match[1] || "").replace(/,/g, ""));
    pushCandidate(Number.isFinite(count) ? count : null, null, null, 40);
  }

  if (
    candidates.length === 0 &&
    EMPLOYEE_COUNT_LABELS.some((regex) => regex.test(normalized))
  ) {
    const fallbackValue = normalizeEmployeeCount(normalized);
    const fallbackCount = parseEmployeeCountNumber(fallbackValue);
    pushCandidate(fallbackCount, null, null, 20);
  }

  return dedupeEmployeeCountCandidates(candidates);
}

function selectBestEmployeeCountCandidate(
  candidates: EmployeeCountCandidate[],
  hints: EmployeeCountContextHints
) {
  if (candidates.length === 0) return null;

  let filtered = [...candidates];

  if (hints.officeNames.length > 0) {
    const officeMatched = filtered.filter((candidate) => candidate.officeMatched);
    if (officeMatched.length > 0) {
      filtered = officeMatched;
    }
  }

  if (hints.city) {
    const cityMatched = filtered.filter((candidate) => candidate.cityMatched);
    if (cityMatched.length > 0) {
      filtered = cityMatched;
    }
  }

  if (hints.prefecture) {
    const prefectureMatched = filtered.filter(
      (candidate) => candidate.prefectureMatched
    );
    if (prefectureMatched.length > 0) {
      filtered = prefectureMatched;
    }
  }

  filtered.sort((a, b) => {
    return (
      Number(b.officeMatched) - Number(a.officeMatched) ||
      Number(b.cityMatched) - Number(a.cityMatched) ||
      Number(b.prefectureMatched) - Number(a.prefectureMatched) ||
      (b.yearMonth ?? -1) - (a.yearMonth ?? -1) ||
      b.sourceScore - a.sourceScore ||
      Number(b.standalone) - Number(a.standalone) ||
      Number(b.consolidated) - Number(a.consolidated) ||
      b.count - a.count
    );
  });

  return filtered[0] ?? null;
}

function extractEmployeeCountFromPage(
  page: PageData,
  sourceCompany?: string | null,
  sourceAddress?: string | null
) {
  const hints = buildEmployeeCountContextHints(sourceCompany, sourceAddress);
  const boost = pageBoost(page.finalUrl);
  const pairs = extractPairs(page.html);
  const candidates: EmployeeCountCandidate[] = [];
  const processedSnippets = new Set<string>();

  const pushSnippetCandidates = (snippet: string, score: number) => {
    const normalizedSnippet = normalizeSpace(snippet);
    if (!normalizedSnippet) return;

    const key = `${score}__${normalizedSnippet}`;
    if (processedSnippets.has(key)) return;
    processedSnippets.add(key);

    candidates.push(
      ...extractEmployeeCountCandidatesFromSnippet(
        normalizedSnippet,
        score,
        hints
      )
    );
  };

  for (const pair of pairs) {
    if (!EMPLOYEE_COUNT_LABELS.some((regex) => regex.test(pair.label))) continue;

    pushSnippetCandidates(`${pair.label} ${pair.value}`, 280 + boost);
  }

  const lines = page.structuredText
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "");

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];

    const lineHasEmployeeLabel = EMPLOYEE_COUNT_LABELS.some((regex) =>
      regex.test(line)
    );
    const lineHasYearCount =
      /[12][0-9]{3}\s*年/.test(line) &&
      /[0-9][0-9,]*\s*(?:名|人)/.test(line);

    if (!lineHasEmployeeLabel && !lineHasYearCount) {
      continue;
    }

    const start = Math.max(0, i - 2);
    const end = Math.min(lines.length, i + 3);
    const nearLines = lines.slice(start, end);
    const snippet = nearLines.join(" ");

    const hasOfficeScope = nearLines.some(
      (targetLine) => extractOfficeNamesFromValue(targetLine).length > 0
    );

    if (lineHasEmployeeLabel) {
      pushSnippetCandidates(snippet, 250 + boost);
      continue;
    }

    if (lineHasYearCount && (hasOfficeScope || !!hints.city || !!hints.prefecture)) {
      pushSnippetCandidates(snippet, 230 + boost);
    }
  }

  const selected = selectBestEmployeeCountCandidate(candidates, hints);
  return selected;
}

function extractAllEmails(value: string) {
  const matches = value.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) ?? [];
  return uniqueNonEmpty(matches);
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
        block.lines.flatMap((line) => extractAllEmails(line))
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

function normalizeRepresentativeTitle(value: string) {
  const normalized = normalizeSpace(value)
    .replace(/（.*?）/g, " ")
    .replace(/\(.*?\)/g, " ")
    .replace(/代表ご挨拶.*$/i, " ")
    .replace(/ご挨拶.*$/i, " ")
    .replace(/メッセージ.*$/i, " ")
    .trim();

  if (!normalized) return null;

  const matched = normalized.match(
    /(代表取締役社長|代表取締役会長|代表取締役|取締役社長|代表社員|代表理事|理事長|会長|社長|CEO|COO|CFO|代表)(?!から)/i
  );

  return matched ? normalizeSpace(matched[1]) : null;
}

function looksLikeRepresentativeNoise(value: string) {
  const normalized = normalizeSpace(value);
  if (!normalized) return true;

  return (
    REPRESENTATIVE_BAD_VALUE_REGEX.test(normalized) ||
    /^(?:会社情報|会社概要(?:・沿革)?|会社沿革|企業情報|会社案内|ご挨拶|社長挨拶|代表挨拶|役員一覧|一覧|大切にしていること|経営理念|経営方針|補償制度|道路工事|地域活動|協賛募集|募集要項|福利厚生|会社紹介動画|工場紹介|企業を知る|採用情報|社員紹介|社員インタビュー|インタビュー|ブログ|お知らせ|ニュース|株式会社|有限会社|合同会社|合資会社|合名会社)$/i.test(
      normalized
    )
  );
}

const REPRESENTATIVE_TITLE_REGEX =
  /代表取締役会長CEO|代表取締役社長COO|代表取締役副社長|代表取締役専務|代表取締役常務|代表取締役|取締役会長|取締役社長|取締役副社長|取締役専務|取締役常務|取締役|会長|社長|副社長|専務|常務|執行役員|監査役|理事長|院長|所長|支店長|本部長|部長|課長|店長|工場長|センター長|室長|主任|係長|担当役員|担当者|担当|責任者|マネージャー|CEO|COO|CFO|CTO|CMO/gu;

const REPRESENTATIVE_STOPWORDS = [
  "営業",
  "採用",
  "人事",
  "総務",
  "経理",
  "広報",
  "受付",
  "窓口",
  "担当者",
  "責任者",
  "事務局",
  "センター",
  "グループ",
  "取締役",
  "執行役員",
  "監査役",
  "理事長",
  "院長",
  "支店長",
  "本部長",
  "工場長",
  "マネージャー",
  "manager",
  "mgr",
  "ceo",
  "coo",
  "cfo",
  "cto",
] as const;

const REPRESENTATIVE_NON_NAME_EXACT_VALUES = new Set([
  "不明",
  "ふめい",
  "未定",
  "なし",
  "無し",
  "該当なし",
  "担当者不明",
  "代表者不明",
  "各位",
  "御中",
  "一同",
]);

const REPRESENTATIVE_AREA_NAME_TOKENS = new Set<string>([
  ...PREFECTURE_NAMES,
  "北海道",
  "東北",
  "関東",
  "中部",
  "近畿",
  "関西",
  "中国",
  "四国",
  "九州",
  "沖縄",
  "東海",
  "札幌",
  "仙台",
  "東京",
  "横浜",
  "川崎",
  "相模原",
  "新潟",
  "静岡",
  "浜松",
  "名古屋",
  "京都",
  "大阪",
  "堺",
  "神戸",
  "岡山",
  "広島",
  "北九州",
  "福岡",
  "熊本",
]);

const REPRESENTATIVE_STRICT_NON_NAME_AREA_TOKENS = new Set<string>([
  "北海道",
  "東北",
  "関東",
  "中部",
  "近畿",
  "関西",
  "中国",
  "四国",
  "九州",
  "沖縄",
  "東海",
  "名古屋",
  "北名古屋",
  "北九州",
  "伊勢志摩",
  "東近江",
  "西三河",
  "東三河",
]);

const REPRESENTATIVE_STRONG_NAME_TOKEN_REGEX =
  /^(?:[\p{sc=Han}々ヶヵ]{1,5}|[\p{sc=Hiragana}]{2,8}|[\p{sc=Katakana}ー]{2,12}|[A-Za-z]{2,20})$/u;

const REPRESENTATIVE_ORGANIZATION_LIKE_REGEX =
  /(?:紙器|紙工|鋼材|電装|工業|工務|建設|住建|工機|工房|工藝|工芸|製材|製茶|製粉|製菓|製鋼|製作所|製作|機工|機器|器械|設備|電工|電設|電機|電子|通信|運輸|通運|産業|化学|化工|化成|鐵工|鉄工|織機|理化|光学|薬品|薬局|眼科|歯科|医院|病院|幼稚園|保育所|保育園|信用金庫|銀行|郵便局|研究所|研究機関|大学|短期大|学園|学校|高校|小学校|中学校|生協|協会|神宮|神社|茶屋|温泉|商店|家具|無線|木材|測量|登記|缶詰|道路|海運|建材|空調|鉄道|製本|看板|解体|葬祭|整体院|料理|酒房|生花|工作所|製麺|総業|乳業|産機)$/u;

const REPRESENTATIVE_NON_NAME_PREFIX_REGEX =
  /^(?:関係者各位|各位|御中|一同|不明|ふめい|未定|該当なし|お問い合わせ)$/u;

const REPRESENTATIVE_NON_NAME_SUFFIX_REGEX =
  /(?:会社|法人|組合|協会|事務局|センター|会館|病院|医院|クリニック|学校|学園|大学|高校|中学|小学校|幼稚園|保育園|施設|寮|館|ホール|ビル|タワー|本社|支社|支店|営業所|工場|研究所|製作所|製麺所|商店|店舗|ホテル|旅館|神社|寺院|農場|牧場|倉庫|公園|市場|駅|空港|港|団地|マンション|ハイツ|コーポ|号室|事務所|部署|部門|売場|園|店|会)$/u;

const REPRESENTATIVE_MUNICIPALITY_LIKE_REGEX =
  /^(?:[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]{4,}(?:市|区|町|村)|[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]{2,}市立[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]+)$/u;

const REPRESENTATIVE_NON_NAME_CONTENT_REGEX = new RegExp(
  [
    "規約",
    "概要",
    "案内",
    "情報",
    "紹介",
    "募集",
    "理念",
    "方針",
    "住宅",
    "新着",
    "連絡先",
    "仕事内容",
    "保証",
    "解析",
    "購入",
    "教室",
    "開発",
    "販売",
    "支援",
    "金融",
    "運行",
    "事業",
    "店舗",
    "製品",
    "商品",
    "商品名",
    "注文",
    "対象者",
    "取引先",
    "取引銀行",
    "問題",
    "品質",
    "利便性",
    "維持",
    "施工",
    "設計",
    "製作",
    "制作",
    "製缶",
    "板金",
    "鈑金",
    "塗装",
    "修理",
    "整備",
    "工事",
    "加工",
    "精密加工",
    "機械加工",
    "大型機械加工",
    "大型精密機械加工",
    "生産",
    "一貫生産体制",
    "流体",
    "計測",
    "測定",
    "測定機",
    "分析",
    "診断",
    "診断装置",
    "試作",
    "量産",
    "量産対応",
    "包装",
    "梱包",
    "発送",
    "印刷",
    "輪転機",
    "設備",
    "省力化設備",
    "機械",
    "電子機器",
    "電機",
    "電装",
    "電気設備工事",
    "電気工事",
    "電気制御",
    "自動制御",
    "材料",
    "素材",
    "部材",
    "衝撃吸収材",
    "制振遮音技術",
    "治具",
    "工具",
    "重量",
    "燃料電池",
    "防水",
    "断熱",
    "気密",
    "特殊鋼",
    "特殊印刷",
    "熱処理",
    "表面処理",
    "真空技術",
    "不動産",
    "建設",
    "建築",
    "建設工業",
    "工務店",
    "修繕",
    "大規模修繕工事",
    "分譲",
    "分譲地",
    "土地",
    "賃貸",
    "売却",
    "売買",
    "中古車",
    "新車",
    "輸入車",
    "車検",
    "車両",
    "車輌",
    "車専門物流",
    "運輸",
    "運送",
    "輸送",
    "配送",
    "物流",
    "産業用",
    "産業廃棄物",
    "保育",
    "介護",
    "看護",
    "障害福祉",
    "障害者共同生活",
    "福祉用具",
    "訪問介護",
    "訪問看護",
    "訪問入浴",
    "介護事業",
    "在宅診療",
    "専門入院自然療法",
    "診療所",
    "病院",
    "医院",
    "診療",
    "内科",
    "外科",
    "小児科",
    "循環器内",
    "呼吸器内",
    "放射線科",
    "腎臓内科",
    "神経科",
    "精神科",
    "皮膚科",
    "眼科",
    "耳鼻科",
    "鼻咽喉科",
    "整形外科",
    "接骨院",
    "歯科",
    "歯医者",
    "矯正歯科",
    "美容皮膚科",
    "調剤薬局",
    "薬局",
    "医薬品",
    "農業薬品",
    "化粧品",
    "司法書士",
    "行政書士",
    "税理士",
    "会計士",
    "認会計士",
    "理事長",
    "病院長",
    "最高経営責任者",
    "役員指名",
    "役員報酬",
    "取締役",
    "代表取締",
    "監査役",
    "役員",
    "理事",
    "副会長",
    "報酬",
    "報酬規程",
    "報酬支給基準",
    "名簿",
    "顧問名簿",
    "組織図",
    "組織概要",
    "組織機構",
    "職員",
    "全従業員",
    "全職員",
    "顧問",
    "氏名",
    "名前",
    "必須",
    "入力",
    "確認",
    "送信",
    "受信可能",
    "事項",
    "項目",
    "編集",
    "削除",
    "権限",
    "公式",
    "最新",
    "最近",
    "履歴",
    "流行",
    "歴史",
    "沿革",
    "活動",
    "活動内容",
    "取組",
    "体制",
    "変更",
    "業務改変",
    "配色変更",
    "余白設定追加",
    "色変換",
    "策定",
    "実行",
    "戦略",
    "計画",
    "創業",
    "創業以来",
    "設立",
    "年月日",
    "設立年月日",
    "法人設立年月日",
    "体験",
    "参加同意",
    "見学",
    "公演",
    "授業",
    "受験対策",
    "講座",
    "作品",
    "油彩",
    "日本画",
    "特産物",
    "地産地消",
    "洋菓子",
    "生菓子",
    "焼菓子",
    "和菓子",
    "味噌",
    "料理",
    "中華",
    "泡盛",
    "焼酎",
    "着物",
    "呉服",
    "弁当",
    "腕時計",
    "時計",
    "宝石",
    "花火",
    "園芸",
    "野菜収穫",
    "薬草",
    "焼肉",
    "研究会",
    "研究",
    "生涯学習",
    "国立長寿医療研究",
    "世界基準",
    "世界情勢",
    "世界最高水準",
    "持続可能",
    "創意工夫",
    "付加価値",
    "人材派遣",
    "人材育成",
    "進路",
    "就職支援",
    "新卒",
    "中途採用",
    "求人",
    "求人情報",
    "広告",
    "広告代",
    "映像",
    "動画",
    "動画付記事",
    "解説動画",
    "展示情報",
    "展示会",
    "展示車",
    "試乗車",
    "宿泊予約",
    "会員登録",
    "資料請求",
    "商品検索",
    "物件検索",
    "導入事例",
    "施工事例",
    "施工事例集",
    "施工実績",
    "施工実例",
    "制作事例",
    "納入事例",
    "対応事例",
    "参考事例",
    "紹介事例",
    "事例",
    "実績",
    "受賞履歴",
    "作業内容",
    "作業工程",
    "事前",
    "作業所",
    "事業所",
    "事業場",
    "店舗情報",
    "情報公開",
    "情報提供",
    "提案",
    "企画",
    "受注生産",
    "地図検索",
    "周辺情報",
    "利用案内",
    "利用方法",
    "利用時間",
    "利用料金",
    "最低価格",
    "午後最速",
    "無料貸出",
    "送料無料",
    "全画面表示",
    "表示拡大",
    "言語切替",
    "今準備中",
    "年末年始休業",
    "冬季休業",
    "定休日",
    "受付時間",
    "参加同意",
    "保存",
    "更新",
    "同意管理",
    "目的",
    "役割",
    "税務行政",
    "多国語展開",
    "中国料理",
    "中文",
    "简体",
    "繁體",
    "中文簡体",
    "中文简体字",
    "全球",
    "网络",
    "網絡",
    "集团",
    "举报",
    "站点",
    "浏览",
    "关于我们",
    "关于征途国际数码",
    "您的域名已过期",
    "参观公司张澜文献"
  ].join("|"),
  "u"
);

const REPRESENTATIVE_ADDRESS_LIKE_REGEX =
  /(?:[0-9０-９]|丁目|番地|番|号|都道府県|市.+区|県.+市|市.+町|市.+村|区.+町|区.+村)/u;

const REPRESENTATIVE_PREFIX_TITLE_TRIM_REGEX =
  /^(?:代表取締役会長CEO|代表取締役社長COO|代表取締役副社長|代表取締役専務|代表取締役常務|代表取締役|取締役会長|取締役社長|取締役副社長|取締役専務|取締役常務|取締役|代表|会長|社長|副社長|専務|常務|執行役員|監査役|理事長|院長|所長|支店長|本部長|部長|課長|店長|工場長|センター長|室長|主任|係長|担当役員|担当者|担当|責任者|マネージャー)+/u;

const REPRESENTATIVE_SUFFIX_TITLE_TRIM_REGEX =
  /(?:代表取締役会長CEO|代表取締役社長COO|代表取締役副社長|代表取締役専務|代表取締役常務|代表取締役|取締役会長|取締役社長|取締役副社長|取締役専務|取締役常務|取締役|代表|会長|社長|副社長|専務|常務|執行役員|監査役|理事長|院長|所長|支店長|本部長|部長|課長|店長|工場長|センター長|室長|主任|係長|担当役員|担当者|担当|責任者|マネージャー|様|さん|氏)+$/u;

const REPRESENTATIVE_NOISE_TOKEN_REGEX =
  /^(?:男性|女性|男|女|担当|担当者|責任者|窓口|受付|御中|様|さん|氏|代表|社長|会長)$/u;

function normalizeRepresentativeComparisonValue(value: string) {
  return value
    .normalize("NFKC")
    .replace(/\s+/g, " ")
    .trim();
}
  
function trimRepresentativeAffixes(value: string) {
  let text = value.normalize("NFKC").trim();
  let previous = "";

  while (text !== previous) {
    previous = text;
    text = text
      .replace(REPRESENTATIVE_PREFIX_TITLE_TRIM_REGEX, "")
      .replace(REPRESENTATIVE_SUFFIX_TITLE_TRIM_REGEX, "")
      .trim();
  }

  return text;
}

function normalizeRepresentativeSource(value: string) {
  return value
    .normalize("NFKC")
    .replace(/[（(][^）)]*[）)]/g, " ")
    .replace(/[【】\[\]「」『』<>〈〉《》〔〕]/g, " ")
    .replace(/[\/／|｜,，、・｡。!！?？:：;；"'`´]/g, " ")
    .replace(/[^\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}A-Za-z\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeRepresentativeToken(token: string) {
  const normalized = token
    .normalize("NFKC")
    .replace(
      /^[A-Za-z]+(?=[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}])/gu,
      " "
    )
    .replace(/[【】\[\]「」『』<>〈〉《》〔〕]/g, " ")
    .replace(/[\/／|｜,，、・｡。!！?？:：;；"'`´]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return trimRepresentativeAffixes(
    normalized
      .replace(
        /^[^A-Za-z\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]+/gu,
        ""
      )
      .replace(
        /[^A-Za-z\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}]+$/gu,
        ""
      )
      .trim()
  );
}

function containsRepresentativeStopword(value: string) {
  const lower = value.toLowerCase();

  return REPRESENTATIVE_STOPWORDS.some((word) => {
    const target = word.toLowerCase();
    return (
      lower === target ||
      lower.startsWith(target) ||
      lower.endsWith(target)
    );
  });
}

function isRepresentativeAreaToken(value: string) {
  return REPRESENTATIVE_AREA_NAME_TOKENS.has(value.normalize("NFKC"));
}

function isLikelyPersonalNameEndingWithShi(value: string) {
  const text = trimRepresentativeAffixes(value.trim()).normalize("NFKC");

  if (!/^[\p{sc=Han}々ヶヵ]{3,5}市$/u.test(text)) return false;
  if (REPRESENTATIVE_STRICT_NON_NAME_AREA_TOKENS.has(text)) return false;
  if (isRepresentativeAreaToken(text)) return false;
  if (REPRESENTATIVE_ORGANIZATION_LIKE_REGEX.test(text)) return false;
  if (REPRESENTATIVE_NON_NAME_CONTENT_REGEX.test(text)) return false;

  return true;
}

function looksLikeProtectedRepresentativeName(value: string) {
  const text = trimRepresentativeAffixes(value.trim());

  if (!text) return false;
  if (/[0-9０-９]/.test(text)) return false;
  if (/株式会社|有限会社|合同会社|御中|様/.test(text)) return false;
  if (REPRESENTATIVE_ADDRESS_LIKE_REGEX.test(text)) return false;

  const parts = text.split(/\s+/).filter((part) => part !== "");

  if (parts.length === 0 || parts.length > 2) return false;

  return parts.every((part) => {
    const normalized = part.normalize("NFKC");

    if (!REPRESENTATIVE_STRONG_NAME_TOKEN_REGEX.test(part)) return false;
    if (REPRESENTATIVE_STRICT_NON_NAME_AREA_TOKENS.has(normalized)) return false;
    
    if (
      REPRESENTATIVE_MUNICIPALITY_LIKE_REGEX.test(part) &&
      !isLikelyPersonalNameEndingWithShi(part)
    ) {
      return false;
    }

    if (REPRESENTATIVE_ORGANIZATION_LIKE_REGEX.test(part)) return false;
    if (REPRESENTATIVE_NON_NAME_CONTENT_REGEX.test(part)) return false;

    return true;
  });
}

function looksLikeNonNameToken(value: string) {
  const text = trimRepresentativeAffixes(value.trim());
  if (!text) return true;

  if (looksLikeProtectedRepresentativeName(text)) return false;
  if (REPRESENTATIVE_NON_NAME_EXACT_VALUES.has(text)) return true;
  if (REPRESENTATIVE_STRICT_NON_NAME_AREA_TOKENS.has(text.normalize("NFKC"))) {
    return true;
  }
  if (REPRESENTATIVE_NON_NAME_PREFIX_REGEX.test(text)) return true;
  if (REPRESENTATIVE_NON_NAME_SUFFIX_REGEX.test(text)) return true;
  
  if (
    REPRESENTATIVE_MUNICIPALITY_LIKE_REGEX.test(text) &&
    !isLikelyPersonalNameEndingWithShi(text)
  ) {
    return true;
  }

  if (REPRESENTATIVE_ORGANIZATION_LIKE_REGEX.test(text)) return true;
  if (REPRESENTATIVE_NON_NAME_CONTENT_REGEX.test(text)) return true;
  if (REPRESENTATIVE_ADDRESS_LIKE_REGEX.test(text)) return true;
  if (/株式会社|有限会社|合同会社|御中|様/.test(text)) return true;
  if (containsRepresentativeStopword(text)) return true;

  return false;
}

function looksLikeRepresentativeNameToken(value: string) {
  const text = trimRepresentativeAffixes(value.trim());

  if (!text) return false;
  if (/[0-9０-９]/.test(text)) return false;
  if (looksLikeNonNameToken(text)) return false;

  if (/^[\p{sc=Han}々ヶヵ\p{sc=Hiragana}\p{sc=Katakana}ー]{1,10}$/u.test(text)) {
    return true;
  }

  if (/^[A-Za-z]{2,20}$/u.test(text)) {
    return true;
  }

  return false;
}

function looksLikeRepresentativeName(value: string) {
  const text = trimRepresentativeAffixes(value.trim());

  if (!text) return false;
  if (looksLikeProtectedRepresentativeName(text)) return true;
  if (/[0-9０-９]/.test(text)) return false;
  if (/株式会社|有限会社|合同会社|御中|様/.test(text)) return false;
  if (REPRESENTATIVE_ORGANIZATION_LIKE_REGEX.test(text)) return false;
  if (REPRESENTATIVE_NON_NAME_CONTENT_REGEX.test(text)) return false;
  if (REPRESENTATIVE_ADDRESS_LIKE_REGEX.test(text)) return false;

  const parts = text.split(/\s+/).filter((part) => part !== "");

  if (parts.length === 0 || parts.length > 2) return false;
  if (parts.some((part) => looksLikeNonNameToken(part))) return false;

  if (
    parts.length === 1 &&
    REPRESENTATIVE_STRICT_NON_NAME_AREA_TOKENS.has(parts[0].normalize("NFKC"))
  ) {
    return false;
  }

  if (
    parts.length === 2 &&
    parts.every(
      (part) =>
        isRepresentativeAreaToken(part) ||
        REPRESENTATIVE_STRICT_NON_NAME_AREA_TOKENS.has(part.normalize("NFKC"))
    )
  ) {
    return false;
  }

  return parts.every((part) => looksLikeRepresentativeNameToken(part));
}

function getRepresentativeTokenVariants(token: string) {
  const normalized = normalizeRepresentativeToken(token);
  const variants = new Set<string>();

  if (!normalized) return [];

  variants.add(normalized);

  const leadingJapanese =
    normalized.match(/^[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}ー]{1,12}/u)?.[0];
  if (leadingJapanese) {
    variants.add(leadingJapanese);
  }

  const trailingJapanese =
    normalized.match(/[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}ー]{1,12}$/u)?.[0];
  if (trailingJapanese) {
    variants.add(trailingJapanese);
  }

  const japaneseChunks = Array.from(
    normalized.matchAll(/[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}ー]{1,12}/gu)
  ).map((match) => match[0]);

  japaneseChunks.forEach((chunk) => variants.add(chunk));

  const leadingAscii = normalized.match(/^[A-Za-z]{2,20}/u)?.[0];
  if (leadingAscii) {
    variants.add(leadingAscii);
  }

  const trailingAscii = normalized.match(/[A-Za-z]{2,20}$/u)?.[0];
  if (trailingAscii) {
    variants.add(trailingAscii);
  }

  return Array.from(variants).filter(
    (variant) =>
      variant !== "" &&
      !REPRESENTATIVE_NOISE_TOKEN_REGEX.test(variant) &&
      !looksLikeNonNameToken(variant)
  );
}

function extractRepresentativeName(value: string) {
  const normalized = normalizeRepresentativeSource(value);
  if (!normalized) return null;

  const withoutTitles = trimRepresentativeAffixes(
    normalized
      .replace(REPRESENTATIVE_TITLE_REGEX, " ")
      .replace(/\s+/g, " ")
      .trim()
  );

  if (!withoutTitles) return null;

  const tokens = withoutTitles
    .split(" ")
    .map((token) => normalizeRepresentativeToken(token))
    .filter(
      (token) => token !== "" && !REPRESENTATIVE_NOISE_TOKEN_REGEX.test(token)
    );

  for (let i = tokens.length - 2; i >= 0; i--) {
    const firstVariants = getRepresentativeTokenVariants(tokens[i]);
    const secondVariants = getRepresentativeTokenVariants(tokens[i + 1]);

    for (const first of firstVariants) {
      for (const second of secondVariants) {
        const candidate = `${first} ${second}`.replace(/\s+/g, " ").trim();

        if (looksLikeRepresentativeName(candidate)) {
          return candidate;
        }
      }
    }
  }

  for (let i = tokens.length - 1; i >= 0; i--) {
    const variants = getRepresentativeTokenVariants(tokens[i]);

    for (const variant of variants) {
      if (looksLikeRepresentativeName(variant)) {
        return variant;
      }
    }
  }

  const compactMatches = Array.from(
    withoutTitles.matchAll(
      /[\p{sc=Han}\p{sc=Hiragana}\p{sc=Katakana}ーA-Za-z]{1,20}/gu
    )
  ).map((match) => normalizeRepresentativeToken(match[0]));

  for (let i = compactMatches.length - 1; i >= 0; i--) {
    if (looksLikeRepresentativeName(compactMatches[i])) {
      return compactMatches[i];
    }
  }

  return null;
}

function inspectRepresentativeNameValue(value: string | null) {
  const original = typeof value === "string" ? value.trim() : "";

  if (original === "") {
    return {
      cleanedValue: null as string | null,
      shouldUpdate: false,
      shouldDelete: false,
      shouldReview: false,
      reason: "",
    };
  }

  const extractedName = extractRepresentativeName(original);

  if (extractedName) {
    const normalizedOriginal = normalizeRepresentativeComparisonValue(original);
    const normalizedExtracted =
      normalizeRepresentativeComparisonValue(extractedName);

    if (normalizedOriginal !== normalizedExtracted) {
      return {
        cleanedValue: extractedName,
        shouldUpdate: true,
        shouldDelete: false,
        shouldReview: false,
        reason: "氏名を抽出できたため更新候補",
      };
    }

    return {
      cleanedValue: extractedName,
      shouldUpdate: false,
      shouldDelete: false,
      shouldReview: false,
      reason: "",
    };
  }

  const normalizedSource = trimRepresentativeAffixes(
    normalizeRepresentativeSource(original)
  );

  if (!normalizedSource || looksLikeNonNameToken(normalizedSource)) {
    return {
      cleanedValue: null as string | null,
      shouldUpdate: false,
      shouldDelete: true,
      shouldReview: false,
      reason: "氏名ではない可能性が高いため削除候補",
    };
  }

  return {
    cleanedValue: null as string | null,
    shouldUpdate: false,
    shouldDelete: false,
    shouldReview: true,
    reason: "氏名か断定できないため要確認",
  };
}

function isStrictRepresentativeNameValue(
  value: string | null,
  options?: { allowCompactSingleToken?: boolean }
) {
  const allowCompactSingleToken =
    options?.allowCompactSingleToken === true;

  const text = trimRepresentativeAffixes(normalizeSpace(value ?? ""));

  if (!text) return false;
  if (looksLikeNonNameToken(text)) return false;
  if (/^[A-Za-z]+$/u.test(text)) return false;

  const parts = text.split(/\s+/).filter((part) => part !== "");

  if (parts.length === 2) {
    return parts.every(
      (part) =>
        /^[\p{sc=Han}々ヶヵ]{1,5}$/u.test(part) ||
        /^[\p{sc=Katakana}ー]{2,12}$/u.test(part) ||
        /^[\p{sc=Hiragana}]{2,8}$/u.test(part)
    );
  }

  if (parts.length === 1 && allowCompactSingleToken) {
    return /^[\p{sc=Han}々ヶヵ]{3,8}$/u.test(parts[0]);
  }

  return false;
}

function normalizeRepresentativeName(
  value: string,
  options?: { allowCompactSingleToken?: boolean }
) {
  const result = inspectRepresentativeNameValue(value);

  if (result.shouldDelete || result.shouldReview || !result.cleanedValue) {
    return null;
  }

  return isStrictRepresentativeNameValue(result.cleanedValue, options)
    ? result.cleanedValue
    : null;
}

function extractRepresentativeNameFromText(text: string) {
  const singleLineCandidate = extractRepresentativeSingleLineValue(text);
  if (singleLineCandidate) return singleLineCandidate;

  const lines = text
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "")
    .slice(0, 120);

  const skipPattern =
    /(代表ご挨拶|ご挨拶|代表メッセージ|メッセージ|スタッフ紹介|役員一覧|役員紹介|マネジメント|management|staff|member|members|profile|blog|voice|story|success|diary|interview|ひとりごと|卒業生|在校生|コーチ|先生|人事|採用|新卒|店舗|shop|campus)/i;

  const titlePattern =
    /(代表取締役社長|代表取締役会長|代表取締役|取締役社長|代表社員|代表理事|理事長|会長|社長|CEO|COO|CFO|CTO|代表|所長|センター長|学院長|校長|学長|施設長|室長)(?!から)/i;

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    if (skipPattern.test(line)) continue;

    const labeledSameLine = line.match(
      /(?:代表者名?|代表者|理事長|所長|センター長|学院長|校長|学長|施設長|室長)\s*[:：]?\s*(.+)$/i
    );
    if (labeledSameLine?.[1]) {
      const name = normalizeRepresentativeName(labeledSameLine[1], {
        allowCompactSingleToken: true,
      });
      if (name) return name;
    }

    const sameLineAfterTitle = line.match(
      /(代表取締役社長|代表取締役会長|代表取締役|取締役社長|代表社員|代表理事|理事長|会長|社長|CEO|COO|CFO|CTO|代表|所長|センター長|学院長|校長|学長|施設長|室長)(?!から)\s*[:：/／]?\s*(.+)$/i
    );
    if (sameLineAfterTitle?.[2]) {
      const name = normalizeRepresentativeName(sameLineAfterTitle[2], {
        allowCompactSingleToken: true,
      });
      if (name) return name;
    }

    const sameLineBeforeTitle = line.match(
      /^(.+?)\s+(?:代表取締役(?:社長|会長)?|取締役社長|取締役|代表社員|代表理事|理事長|社長|会長|CEO|COO|CFO|CTO|常務取締役?|専務取締役?|執行役員(?:専務|常務)?|常務|専務|相談役|名誉相談役|所長|センター長|学院長|校長|学長|施設長|室長)$/u
    );
    if (sameLineBeforeTitle?.[1]) {
      const name = normalizeRepresentativeName(sameLineBeforeTitle[1], {
        allowCompactSingleToken: true,
      });
      if (name) return name;
    }

    if (
      /^(?:代表者(?:名)?|理事長|所長|センター長|学院長|校長|学長|施設長|室長)$/.test(
        line
      )
    ) {
      const nextLine = lines[i + 1] ?? "";
      const nextName = normalizeRepresentativeName(nextLine, {
        allowCompactSingleToken: true,
      });
      if (nextName) return nextName;
    }

    if (titlePattern.test(line)) {
      const nextLine = lines[i + 1] ?? "";
      if (!skipPattern.test(nextLine)) {
        const nextName = normalizeRepresentativeName(nextLine, {
          allowCompactSingleToken: true,
        });
        if (nextName) return nextName;
      }
    }
  }

  return null;
}

function extractRepresentativeTitleFromText(text: string) {
  const lines = text
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "");

  for (const line of lines) {
    if (/代表ご挨拶|ご挨拶|メッセージ/i.test(line)) continue;

    const title = normalizeRepresentativeTitle(line);
    if (title) return title;

    const sameLine = line.match(
      /代表者\s*[:：]?\s*(代表取締役社長|代表取締役会長|代表取締役|取締役社長|代表社員|代表理事|理事長|会長|社長|CEO|COO|CFO|代表)(?!から)/i
    );
    if (sameLine?.[1]) {
      return normalizeSpace(sameLine[1]);
    }
  }

  return null;
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
    /<tr[^>]*>\s*<(?:th|td)[^>]*>([\s\S]*?)<\/(?:th|td)>\s*<td[^>]*>([\s\S]*?)<\/td>[\s\S]*?<\/tr>/gi,
    /<dt[^>]*>([\s\S]*?)<\/dt>\s*<dd[^>]*>([\s\S]*?)<\/dd>/gi,
    /<(?:li|p|div)[^>]*>\s*([^<:：]{1,40})\s*[:：]\s*([\s\S]*?)<\/(?:li|p|div)>/gi,
    /<(?:div|p|li)[^>]*>\s*<(?:span|strong|b)[^>]*>([\s\S]{1,40}?)<\/(?:span|strong|b)>\s*[:：]?\s*<(?:span|em|strong|b)[^>]*>([\s\S]*?)<\/(?:span|em|strong|b)>\s*<\/(?:div|p|li)>/gi,
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

async function fetchPage(
  url: string,
  timeoutMs = 10000,
  runtimeOptions?: CrawlRuntimeOptions
): Promise<PageData | null> {
  throwIfCrawlShouldStop(runtimeOptions);

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
        Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        Pragma: "no-cache",
      },
      cache: "no-store",
    });

    throwIfCrawlShouldStop(runtimeOptions);

    if (!response.ok) return null;

    const contentType = response.headers.get("content-type") || "";
    if (!contentType.toLowerCase().includes("text/html")) return null;

    const html = await response.text();

    throwIfCrawlShouldStop(runtimeOptions);

    return {
      requestedUrl: url,
      finalUrl: response.url || url,
      html,
      text: stripHtml(html),
      structuredText: stripHtmlKeepLineBreaks(html),
      title: extractTitle(html),
      h1: extractH1(html),
      links: extractLinks(html, response.url || url),
    };
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
    clearTimeout(timeoutId);
    clearInterval(stopCheckId);
  }
}

async function fetchTopPage(
  seedUrl: string,
  timeoutMs = 10000,
  runtimeOptions?: CrawlRuntimeOptions
): Promise<PageData | null> {
  throwIfCrawlShouldStop(runtimeOptions);

  const normalized = normalizeSeedUrl(seedUrl);
  if (!normalized) return null;

  const first = await fetchPage(normalized, timeoutMs, runtimeOptions);
  if (first) return first;

  if (/^https:\/\//i.test(normalized)) {
    return await fetchPage(
      normalized.replace(/^https:\/\//i, "http://"),
      timeoutMs,
      runtimeOptions
    );
  }

  return null;
}

function getCandidateLinkScore(
  link: LinkItem,
  selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  const target = decodeURIComponent(`${link.text} ${link.url}`);
  let score = 0;

  const needsCompanyPages =
    hasSelectedCrawlField(selectedFieldSet, "company") ||
    hasSelectedCrawlField(selectedFieldSet, "established_date") ||
    hasSelectedCrawlField(selectedFieldSet, "representative_name") ||
    hasSelectedCrawlField(selectedFieldSet, "capital") ||
    hasSelectedCrawlField(selectedFieldSet, "employee_count") ||
    hasSelectedCrawlField(selectedFieldSet, "business_content");

  const needsContactPages =
    hasSelectedCrawlField(selectedFieldSet, "form_url") ||
    hasSelectedCrawlField(selectedFieldSet, "phone") ||
    hasSelectedCrawlField(selectedFieldSet, "fax") ||
    hasSelectedCrawlField(selectedFieldSet, "email") ||
    hasSelectedCrawlField(selectedFieldSet, "zipcode") ||
    hasSelectedCrawlField(selectedFieldSet, "address");

  const needsRepresentativePages = hasSelectedCrawlField(
    selectedFieldSet,
    "representative_name"
  );

  const needsEmployeeCountPages = hasSelectedCrawlField(
    selectedFieldSet,
    "employee_count"
  );

  const needsBusinessPages = hasSelectedCrawlField(
    selectedFieldSet,
    "business_content"
  );

  if (COMPANY_KEYWORDS.test(target) && needsCompanyPages) score += 140;
  if (BUSINESS_KEYWORDS.test(target) && (needsBusinessPages || needsCompanyPages))
    score += 110;
  if (CONTACT_KEYWORDS.test(target) && needsContactPages) score += 160;
  if (STAFF_KEYWORDS.test(target) && needsRepresentativePages) score += 180;

  if (
    needsRepresentativePages &&
    /(会社概要|会社情報|企業情報|法人概要|about|company|corporate|outline|profile|gaiyou|overview|information)/i.test(
      target
    )
  ) {
    score += 220;
  }

  if (
    needsRepresentativePages &&
    /(代表挨拶|社長挨拶|理事長挨拶|所長挨拶|トップメッセージ|topmessage|greeting|message|president)/i.test(
      target
    )
  ) {
    score += 260;
  }

  if (
    needsEmployeeCountPages &&
    /(従業員数|社員数|職員数|スタッフ数|人数|人員|会社データ|数字で見る|data|numbers|ir|profile|outline)/i.test(
      target
    )
  ) {
    score += 260;
  }

  if (
    needsEmployeeCountPages &&
    /(会社概要|企業情報|会社案内|corporate|company|about|outline|profile)/i.test(
      target
    )
  ) {
    score += 80;
  }

  if (
    hasSelectedCrawlField(selectedFieldSet, "form_url") &&
    CONTACT_KEYWORDS.test(target)
  ) {
    score += 80;
  }

  if (needsRepresentativePages) {
    if (isRepresentativeOverviewPageTarget(target)) {
      score += 520;
    }

    if (/(役員一覧|officer|executive)/i.test(target)) {
      score += 180;
    }

    if (
      /(greeting|message|挨拶|メッセージ|代表挨拶|社長挨拶|トップメッセージ|topmessage|president)/i.test(
        target
      )
    ) {
      score += 220;
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
  }

  if (NEWS_BLOG_KEYWORDS.test(target)) score -= 120;
  if (RECRUIT_KEYWORDS.test(target)) score -= 80;
  if (/\.pdf(?:$|\?)/i.test(link.url)) score -= 40;

  return score;
}

function pickCandidatePageUrls(
  topPage: PageData,
  selectedFieldSet: Set<CrawlSelectableFieldKey>
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

  const scoredCommonPaths = COMMON_CANDIDATE_PATHS.map((path) => {
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
          selectedFieldSet
        ),
      };
    } catch {
      return null;
    }
  })
    .filter((item): item is { url: string; score: number } => item !== null)
    .sort((a, b) => b.score - a.score);

  for (const item of scoredCommonPaths) {
    pushIfValid(item.url);
  }

  const sortedLinks = [...topPage.links]
    .map((link) => ({
      url: link.url,
      score: getCandidateLinkScore(link, selectedFieldSet),
    }))
    .filter((item) =>
      hasSelectedCrawlField(selectedFieldSet, "representative_name")
        ? item.score > -120
        : item.score > 0
    )
    .sort((a, b) => b.score - a.score);

  for (const item of sortedLinks) {
    pushIfValid(item.url);
  }

  return urls.slice(0, getCandidatePageLimit(selectedFieldSet));
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
  selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  return (
    selectedFieldSet.size === 1 &&
    hasSelectedCrawlField(selectedFieldSet, "representative_name")
  );
}

function getRepresentativeEnoughScore(
  selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  return isRepresentativeOnlyMode(selectedFieldSet) ? 300 : 340;
}

function getCandidatePageLimit(
  selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  const needsEmployeeCount = hasSelectedCrawlField(
    selectedFieldSet,
    "employee_count"
  );

  if (isRepresentativeOnlyMode(selectedFieldSet)) {
    return 18;
  }

  if (selectedFieldSet.size === 1) {
    if (hasSelectedCrawlField(selectedFieldSet, "form_url")) return 5;
    if (hasSelectedOfficeFields(selectedFieldSet)) {
      return needsEmployeeCount ? 12 : 8;
    }
    if (needsEmployeeCount) return 16;
    return 6;
  }

  if (
    selectedFieldSet.size <= 3 &&
    hasSelectedCrawlField(selectedFieldSet, "representative_name")
  ) {
    return needsEmployeeCount ? 18 : 12;
  }

  if (selectedFieldSet.size <= 3 && !hasSelectedOfficeFields(selectedFieldSet)) {
    return needsEmployeeCount ? 18 : 8;
  }

  if (needsEmployeeCount) {
    return 30;
  }

  return 20;
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
  selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  if (hasSelectedOfficeFields(selectedFieldSet)) {
    return false;
  }

  const representativeEnoughScore =
    getRepresentativeEnoughScore(selectedFieldSet);

  return Array.from(selectedFieldSet).every((field) => {
    if (field === "company") return !!best.company?.value;
    if (field === "website_url") return !!best.website_url?.value;
    if (field === "form_url") return !!best.form_url?.value;
    if (field === "established_date") return !!best.established_date?.value;

    if (field === "representative_name") {
      return (
        !!best.representative_name?.value &&
        (best.representative_name?.score ?? 0) >= representativeEnoughScore
      );
    }

    if (field === "capital") return !!best.capital?.value;
    if (field === "employee_count") return hasHighConfidenceEmployeeCount(best);
    if (field === "business_content") return !!best.business_content?.value;

    return true;
  });
}

function pickNestedCandidatePageUrls(
  page: PageData,
  selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  const urls: string[] = [];
  const seen = new Set<string>();

  const sortedLinks = [...page.links]
    .map((link) => ({
      url: link.url,
      score: getCandidateLinkScore(link, selectedFieldSet),
    }))
    .filter((item) =>
      hasSelectedCrawlField(selectedFieldSet, "representative_name")
        ? item.score > -120
        : item.score > 0
    )
    .sort((a, b) => b.score - a.score);

  for (const item of sortedLinks) {
    if (seen.has(item.url)) continue;
    if (HTML_PAGE_DENY_EXT.test(item.url)) continue;
    seen.add(item.url);
    urls.push(item.url);
  }

  return urls.slice(0, 20);
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
  }
) {
  const boost = pageBoost(page.finalUrl);
  const pairs = extractPairs(page.html);

  if (hasSelectedCrawlField(selectedFieldSet, "website_url")) {
    addBest(best, "website_url", page.finalUrl, 200);
  }

  if (hasSelectedCrawlField(selectedFieldSet, "company")) {
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

  if (hasSelectedCrawlField(selectedFieldSet, "form_url")) {
    const formLink = page.links.find((link) => {
      const target = `${link.text} ${link.url}`;
      return CONTACT_KEYWORDS.test(target) && !HTML_PAGE_DENY_EXT.test(link.url);
    });

    if (hasHtmlForm(page.html) && CONTACT_KEYWORDS.test(page.finalUrl)) {
      addBest(best, "form_url", page.finalUrl, 150);
    }
    addBest(best, "form_url", formLink?.url ?? null, 120 + boost);
  }

  if (hasSelectedCrawlField(selectedFieldSet, "email")) {
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

  if (hasSelectedCrawlField(selectedFieldSet, "phone")) {
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

  if (hasSelectedCrawlField(selectedFieldSet, "fax")) {
    addBest(best, "fax", normalizeFax(faxPairValue), 220 + boost);
    addBest(best, "fax", normalizeFax(contactPairValue), 210 + boost);
  }

  const addressPairValue = pickPairValue(pairs, ADDRESS_LABELS) ?? "";
  const zipcodePairValue = pickPairValue(pairs, ZIPCODE_LABELS) ?? addressPairValue;
  const detectedAddress = extractAddressFromText(
    [addressPairValue, zipcodePairValue, page.text].filter(Boolean).join(" ")
  );

  if (hasSelectedCrawlField(selectedFieldSet, "zipcode")) {
    addBest(best, "zipcode", normalizeZipcode(zipcodePairValue), 220 + boost);
    addBest(best, "zipcode", normalizeZipcode(addressPairValue), 210 + boost);
    addBest(best, "zipcode", normalizeZipcode(page.text), 40 + boost);
  }

  if (hasSelectedCrawlField(selectedFieldSet, "address")) {
    addBest(best, "address", normalizeAddress(addressPairValue), 220 + boost);
    addBest(best, "address", detectedAddress, 215 + boost);
  }

  if (hasSelectedCrawlField(selectedFieldSet, "established_date")) {
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

if (hasSelectedCrawlField(selectedFieldSet, "representative_name")) {
  const representativeNamePairValue =
    pickPairValue(pairs, REPRESENTATIVE_NAME_LABELS) ?? "";
  const representativeNameTextValue =
    extractRepresentativeSingleLineValue(page.structuredText) ?? "";

  const representativePairName = normalizeRepresentativeName(
    representativeNamePairValue,
    { allowCompactSingleToken: true }
  );
  const representativeTextName = normalizeRepresentativeName(
    representativeNameTextValue,
    { allowCompactSingleToken: true }
  );
  const representativeNameFromText =
    representativePairName || representativeTextName
      ? null
      : extractRepresentativeNameFromText(page.structuredText);

  const representativeSourceTarget = decodeURIComponent(
    `${page.finalUrl} ${page.title} ${page.h1}`
  );

  const representativeScoreAdjustment =
    (/(会社概要|会社概要・沿革|会社案内|企業情報|法人概要|about|company|corporate|outline|who\.html|gaiyou|data|profile|officer|executive|役員一覧)/i.test(
      representativeSourceTarget
    )
      ? 120
      : 0) +
    (/(office\/greeting|社長挨拶|代表挨拶|理事長挨拶|所長挨拶|ご挨拶|greeting|message)/i.test(
      representativeSourceTarget
    )
      ? 90
      : 0) -
    (/(recruit|career|job|jobs|entry|新卒|中途|採用|shop|shopinfo|campus|店舗|商品|製品|service|blog|news|topics|column|interview|voice|story|success|diary|staff|member|members|社員紹介|社員インタビュー)/i.test(
      representativeSourceTarget
    )
      ? 320
      : 0);

  addBest(
    best,
    "representative_name",
    representativePairName,
    300 + boost + representativeScoreAdjustment
  );
  addBest(
    best,
    "representative_name",
    representativeTextName,
    320 + boost + representativeScoreAdjustment
  );
  addBest(
    best,
    "representative_name",
    representativeNameFromText,
    240 + boost + representativeScoreAdjustment
  );
}

  if (hasSelectedCrawlField(selectedFieldSet, "capital")) {
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

  if (hasSelectedCrawlField(selectedFieldSet, "employee_count")) {
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

  if (hasSelectedCrawlField(selectedFieldSet, "business_content")) {
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
}

function findRejectedRepresentativeValue(pages: PageData[]) {
  for (const page of pages) {
    const rawValue =
      extractSingleLineLabeledValue(
        page.structuredText,
        REPRESENTATIVE_NAME_LABELS
      ) ?? null;

    if (!rawValue) continue;

    const inspected = inspectRepresentativeNameValue(rawValue);

    if (inspected.shouldDelete || inspected.shouldReview) {
      return {
        raw: normalizeSpace(rawValue),
        reason: inspected.reason,
      };
    }
  }

  return {
    raw: null as string | null,
    reason: null as string | null,
  };
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
      offices: [],
    };
  }

  const best: Partial<Record<keyof CrawlExtractedFields, BestValue>> = {};
  const collectedPages: PageData[] = [];

  const representativeOnlyMode = isRepresentativeOnlyMode(selectedFieldSet);
  const representativeEnoughScore =
    getRepresentativeEnoughScore(selectedFieldSet);
  const pageFetchTimeoutMs = representativeOnlyMode ? 8000 : 10000;

  throwIfCrawlShouldStop(runtimeOptions);

  const topPage = await fetchTopPage(
    websiteUrl,
    representativeOnlyMode ? 10000 : pageFetchTimeoutMs,
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
      offices: [],
    };
  }

  collectedPages.push(topPage);
  processPage(topPage, best, selectedFieldSet, sourceContext);

  const fetchedUrlSet = new Set<string>([topPage.finalUrl]);
  const nestedCandidateUrls: string[] = [];
  const rawCandidateUrls = pickCandidatePageUrls(topPage, selectedFieldSet);
  const overviewCandidateUrls = hasSelectedCrawlField(
    selectedFieldSet,
    "representative_name"
  )
    ? rawCandidateUrls.filter((url) => isRepresentativeOverviewPageTarget(url))
    : rawCandidateUrls;
  const fallbackCandidateUrls = hasSelectedCrawlField(
    selectedFieldSet,
    "representative_name"
  )
    ? rawCandidateUrls.filter((url) => !isRepresentativeOverviewPageTarget(url))
    : [];
  const queuedUrlSet = new Set<string>(rawCandidateUrls);

    const fetchCandidatePages = async (urls: string[]) => {
    for (const candidateUrl of urls) {
      throwIfCrawlShouldStop(runtimeOptions);

      if (canStopFetchingAdditionalPages(best, selectedFieldSet)) {
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
      collectedPages.push(page);
      processPage(page, best, selectedFieldSet, sourceContext);

      throwIfCrawlShouldStop(runtimeOptions);

      const shouldCollectNestedRepresentativePages =
        hasSelectedCrawlField(selectedFieldSet, "representative_name") &&
        (!best.representative_name?.value ||
          (best.representative_name?.score ?? 0) < representativeEnoughScore);

      if (shouldCollectNestedRepresentativePages) {
        const nestedUrls = pickNestedCandidatePageUrls(page, selectedFieldSet);
        for (const nestedUrl of nestedUrls) {
          if (queuedUrlSet.has(nestedUrl)) continue;
          queuedUrlSet.add(nestedUrl);
          nestedCandidateUrls.push(nestedUrl);
        }
      }
    }
  };

  if (hasSelectedCrawlField(selectedFieldSet, "representative_name")) {
    if (!canStopFetchingAdditionalPages(best, selectedFieldSet)) {
      await fetchCandidatePages(overviewCandidateUrls);
    }

    const shouldFetchFallbackRepresentativePages =
      !best.representative_name?.value ||
      (best.representative_name?.score ?? 0) < representativeEnoughScore;

    if (shouldFetchFallbackRepresentativePages) {
      await fetchCandidatePages(fallbackCandidateUrls);
    }
  } else {
    await fetchCandidatePages(rawCandidateUrls);
  }

  const shouldFetchNestedRepresentativePages =
    hasSelectedCrawlField(selectedFieldSet, "representative_name") &&
    (!best.representative_name?.value ||
      (best.representative_name?.score ?? 0) < representativeEnoughScore);

  if (shouldFetchNestedRepresentativePages) {
    const nestedLimit = representativeOnlyMode ? 20 : 40;

    for (const nestedUrl of nestedCandidateUrls.slice(0, nestedLimit)) {
      throwIfCrawlShouldStop(runtimeOptions);

      if (canStopFetchingAdditionalPages(best, selectedFieldSet)) {
        break;
      }

      const nestedPage = await fetchPage(
        nestedUrl,
        representativeOnlyMode ? 8000 : pageFetchTimeoutMs,
        runtimeOptions
      );
      if (!nestedPage) continue;
      if (fetchedUrlSet.has(nestedPage.finalUrl)) continue;

      fetchedUrlSet.add(nestedPage.finalUrl);
      collectedPages.push(nestedPage);
      processPage(nestedPage, best, selectedFieldSet, sourceContext);
    }
  }

  const shouldExtractOffices =
    hasSelectedCrawlField(selectedFieldSet, "phone") ||
    hasSelectedCrawlField(selectedFieldSet, "fax") ||
    hasSelectedCrawlField(selectedFieldSet, "email") ||
    hasSelectedCrawlField(selectedFieldSet, "zipcode") ||
    hasSelectedCrawlField(selectedFieldSet, "address");

  const offices = shouldExtractOffices
    ? extractOfficeResults(collectedPages, best.company?.value ?? null, {
        phone: best.phone?.value ?? null,
        fax: best.fax?.value ?? null,
        email: best.email?.value ?? null,
        zipcode: best.zipcode?.value ?? null,
        address: best.address?.value ?? null,
      })
    : [];

  const rejectedRepresentative =
    hasSelectedCrawlField(selectedFieldSet, "representative_name") &&
    !best.representative_name?.value
      ? findRejectedRepresentativeValue(collectedPages)
      : { raw: null as string | null, reason: null as string | null };

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
    representative_name_raw: hasSelectedCrawlField(
      selectedFieldSet,
      "representative_name"
    )
      ? rejectedRepresentative.raw
      : null,
    representative_name_reason: hasSelectedCrawlField(
      selectedFieldSet,
      "representative_name"
    )
      ? rejectedRepresentative.reason
      : null,
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
    offices,
  };
}