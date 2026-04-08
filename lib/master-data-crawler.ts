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
  /(基本情報|ご?挨拶|人事|新卒入社|会社概要(?:・沿革)?|会社案内|会社情報|企業情報|企業理念|経営理念|経営方針|経営者略歴|構成|声明|調査相談専用|番号|事業内容|製造|資本|者名|店舗詳細|営業所所在地|本社所在地|本社住所|月現在|月\s*移動|採用\s*情報|採用情報|名誉相談役|級建築士|登録番号|福山市|府中市|長野事業所|ショップ|shop|キャンパス|campus|一覧|大切にしていること|補償制度|道路工事|会社沿革|会社紹介動画|工場紹介|地域活動|協賛募集|募集要項|福利厚生|企業を知る|社員紹介|社員インタビュー|interview|voice|story|blog|news|topics|column|株式会社$|有限会社$|合同会社$|合資会社$|合名会社$|営業責任者|責任者\s*兼|生年月日|営\s*業|設立|創業|平成|昭和|令和)/i;
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
  /^社員数$/,
  /^スタッフ数$/,
  /^人員構成$/,
  /^人員$/,
  /^職員数$/,
  /^メンバー数$/,
  /^在籍人数$/,
  /従業員数/,
  /従業員/,
  /社員数/,
  /スタッフ数/,
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
      const candidate = normalizeRepresentativeName(sameLine[1]);
      if (candidate) return candidate;
    }

    if (exactLabelOnlyPattern.test(line)) {
      const nextLine = lines[i + 1] ?? "";
      const nextCandidate = normalizeRepresentativeName(nextLine);
      if (nextCandidate) return nextCandidate;

      const nextNextLine = lines[i + 2] ?? "";
      const nextNextCandidate = normalizeRepresentativeName(nextNextLine);
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

function pickLikelyRepresentativeName(value: string) {
  const normalized = normalizeSpace(value);
  if (!normalized) return null;

  const spacedCandidates =
    normalized.match(/[一-龠々]{1,4}\s+[一-龠々]{1,4}/gu) ?? [];

  for (const raw of spacedCandidates) {
    const candidate = normalizeSpace(raw);
    const joinedLength = candidate.replace(/\s/g, "").length;
    if (joinedLength < 3 || joinedLength > 7) continue;
    if (looksLikeRepresentativeNoise(candidate)) continue;
    return candidate;
  }

  const compactCandidates =
    normalized.match(/[一-龠々]{4,8}/gu) ?? [];

  for (const raw of compactCandidates) {
    const candidate = normalizeSpace(raw);
    if (candidate.length < 4 || candidate.length > 8) continue;
    if (looksLikeRepresentativeNoise(candidate)) continue;
    return candidate;
  }

  return null;
}

function normalizeRepresentativeName(value: string) {
  const original = normalizeSpace(value);
  if (!original) return null;

  const source = original
    .replace(/https?:\/\/\S+/gi, " ")
    .replace(/www\.\S+/gi, " ")
    .replace(/[【】\[\]<>＜＞]/g, " ")
    .replace(/\s{2,}/g, " ")
    .trim();

  const inlineCompanyTitleName = source.match(
    /(?:株式会社|有限会社|合同会社|合資会社|合名会社)[^\s　]{0,40}\s+(?:代表取締役(?:社長|会長)?|取締役社長|代表社員|代表理事|理事長|社長|会長|CEO|COO|CFO|CTO|代表)\s*([一-龠々]{1,4}\s*[一-龠々]{1,4}|[一-龠々]{4,8})/u
  );
  if (inlineCompanyTitleName?.[1]) {
    const candidate = pickLikelyRepresentativeName(
      normalizeSpace(inlineCompanyTitleName[1])
    );
    if (candidate) return candidate;
  }

  let normalized = source
    .replace(/（.*?）/g, " ")
    .replace(/\(.*?\)/g, " ")
    .replace(/\bPROFILE\b.*$/i, " ")
    .replace(/代表(?:から)?のご挨拶.*$/i, " ")
    .replace(/代表メッセージ.*$/i, " ")
    .replace(/ご挨拶.*$/i, " ")
    .replace(/メッセージ.*$/i, " ")
    .replace(/スタッフ紹介.*$/i, " ")
    .replace(/プロフィール.*$/i, " ")
    .replace(/役員(?:紹介|一覧).*$/i, " ")
    .replace(/マネジメント.*$/i, " ")
    .replace(/ブログ.*$/i, " ")
    .replace(/ひとりごと.*$/i, " ")
    .replace(/(?:略歴|経歴|担当|就任|出身|profile).*$/i, " ")
    .replace(/[／/｜|]/g, " ")
    .replace(/[、。,．・･]/g, " ")
    .replace(/\s*(?:様|さん|氏|先生)\s*$/u, "")
    .replace(
      /(?:TEL|Tel|tel|FAX|Fax|fax|メール|E-mail|Mail|住所|所在地|会社概要|事業内容|資本金|従業員数).*$/i,
      " "
    )
    .replace(
      /^(?:(?:代表者名?|代表取締役(?:社長|会長)?|取締役社長|取締役|代表社員|代表理事|理事長|社長|会長|CEO|COO|CFO|CTO|代表|執行役員(?:専務|常務)?|専務取締役?|常務取締役?|専務|常務|相談役|名誉相談役|所長|センター長|学院長|校長|学長|施設長|室長|締役社長|締役|兼社長|常務取締|専務取締)\s*[:：]?\s*)+/i,
      ""
    )
    .replace(
      /^(?:株式会社|有限会社|合同会社|合資会社|合名会社)\s*[^\s　]{1,40}\s+/u,
      ""
    )
    .replace(
      /^[^\s　]{1,40}(?:株式会社|有限会社|合同会社|合資会社|合名会社)\s+/u,
      ""
    )
    .replace(/\s{2,}/g, " ")
    .trim();

  if (!normalized) return null;
  if (/[0-9０-９@]/.test(normalized)) return null;
  if (looksLikeRepresentativeNoise(normalized)) return null;
  if (!/[一-龠々]/u.test(normalized)) return null;
  if (/[ぁ-んァ-ヶ]/u.test(normalized)) return null;

  const leadingNameBeforeTitle = normalized.match(
    /^([一-龠々]{1,4}\s*[一-龠々]{1,4}|[一-龠々]{4,8})\s+(?:代表取締役(?:社長|会長)?|取締役社長|取締役|代表社員|代表理事|理事長|社長|会長|CEO|COO|CFO|CTO|常務取締役?|専務取締役?|執行役員(?:専務|常務)?|常務|専務|相談役|名誉相談役|所長|センター長|学院長|校長|学長|施設長|室長)/u
  );
  if (leadingNameBeforeTitle?.[1]) {
    const candidate = pickLikelyRepresentativeName(leadingNameBeforeTitle[1]);
    if (candidate) return candidate;
  }

  normalized = normalized.replace(REPRESENTATIVE_TRAILING_TITLE_REGEX, "").trim();
  if (!normalized) return null;
  if (looksLikeRepresentativeNoise(normalized)) return null;

  if (
    /^(?:代表者名?|代表取締役(?:社長|会長)?|取締役社長|取締役|代表社員|代表理事|理事長|社長|会長|CEO|COO|CFO|CTO|代表|所長|センター長|学院長|校長|学長|施設長|室長)$/i.test(
      normalized
    )
  ) {
    return null;
  }

  return pickLikelyRepresentativeName(normalized);
}

function extractRepresentativeNameFromText(text: string) {
  const singleLineCandidate = extractRepresentativeSingleLineValue(text);
  if (singleLineCandidate) return singleLineCandidate;

  const lines = text
    .split("\n")
    .map((line) => normalizeSpace(line))
    .filter((line) => line !== "");

  const skipPattern =
    /(代表ご挨拶|ご挨拶|代表メッセージ|メッセージ|スタッフ紹介|役員一覧|役員紹介|マネジメント|management|staff|member|members|profile|blog|voice|story|success|diary|interview|ひとりごと|卒業生|在校生|コーチ|先生|人事|採用|新卒|店舗|shop|campus)/i;

  const titlePattern =
    /(代表取締役社長|代表取締役会長|代表取締役|取締役社長|代表社員|代表理事|理事長|会長|社長|CEO|COO|CFO|CTO|代表|所長|センター長|学院長|校長|学長|施設長|室長)(?!から)/i;

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];

    if (skipPattern.test(line)) continue;

    const labeledSameLine = line.match(
      /(?:代表者名?|代表者|役員(?!一覧|紹介)|理事長|所長|センター長|学院長|校長|学長|施設長|室長)\s*[:：]?\s*(.+)$/i
    );
    if (labeledSameLine?.[1]) {
      const name = normalizeRepresentativeName(labeledSameLine[1]);
      if (name) return name;
    }

    const sameLine = line.match(
      /(代表取締役社長|代表取締役会長|代表取締役|取締役社長|代表社員|代表理事|理事長|会長|社長|CEO|COO|CFO|CTO|代表|所長|センター長|学院長|校長|学長|施設長|室長)(?!から)\s*[:：/／]?\s*(.+)$/i
    );
    if (sameLine?.[2]) {
      const name = normalizeRepresentativeName(sameLine[2]);
      if (name) return name;
    }

    if (
      /^(?:代表者(?:名)?|役員(?!一覧|紹介)|理事長|所長|センター長|学院長|校長|学長|施設長|室長)$/.test(
        line
      )
    ) {
      const nextLine = lines[i + 1] ?? "";
      const nextLineName = normalizeRepresentativeName(nextLine);
      if (nextLineName) return nextLineName;

      const nextNextLine = lines[i + 2] ?? "";
      const nextNextLineName = normalizeRepresentativeName(nextNextLine);
      if (nextNextLineName) return nextNextLineName;
    }

    if (titlePattern.test(line)) {
      const nextLine = lines[i + 1] ?? "";
      if (!skipPattern.test(nextLine)) {
        const name = normalizeRepresentativeName(nextLine);
        if (name) return name;
      }
    }
  }

  const normalized = normalizeSpace(text);
  if (!normalized) return null;

  const patterns = [
    /(?:株式会社|有限会社|合同会社|合資会社|合名会社)[^\s　]{0,40}\s+(?:代表取締役(?:社長|会長)?|取締役社長|代表社員|代表理事|理事長|会長|社長|CEO|COO|CFO|CTO|代表|所長|センター長|学院長|校長|学長|施設長|室長)\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /代表者名?\s*[:：]?\s*(?:代表取締役(?:社長|会長)?|取締役社長|代表社員|代表理事|理事長|会長|社長|CEO|COO|CFO|CTO|代表|所長|センター長|学院長|校長|学長|施設長|室長)?\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /役員\s*[:：]?\s*(?:代表取締役(?:社長|会長)?|取締役社長|代表社員|代表理事|理事長|会長|社長|CEO|COO|CFO|CTO|代表|所長|センター長|学院長|校長|学長|施設長|室長)?\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /代表取締役(?:社長|会長)?\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /取締役社長\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /代表社員\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /代表理事\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /理事長\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /所長\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /センター長\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /学院長\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /校長\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /学長\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /施設長\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /室長\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /社長\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /会長\s*[:：/／]?\s*([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})/u,
    /^([一-龠々]{1,4}\s+[一-龠々]{1,4}|[一-龠々]{4,8})\s+(?:代表取締役(?:社長|会長)?|取締役社長|取締役|代表社員|代表理事|理事長|社長|会長|CEO|COO|CFO|CTO|常務取締役?|専務取締役?|執行役員(?:専務|常務)?|常務|専務|相談役|名誉相談役|所長|センター長|学院長|校長|学長|施設長|室長)/u,
  ];

  for (const pattern of patterns) {
    const matched = normalized.match(pattern);
    if (matched?.[1]) {
      const name = normalizeRepresentativeName(matched[1]);
      if (name) return name;
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

  const matches = [...normalized.matchAll(/([0-9,]+)\s*(名|人|店|社|拠点|ヶ所|か所|箇所|営業所)/g)];

  if (matches.length >= 2) {
    const total = matches.reduce((sum, matched) => {
      const num = Number((matched[1] || "0").replace(/,/g, ""));
      return sum + (Number.isFinite(num) ? num : 0);
    }, 0);

    return total > 0 ? `${total}名` : null;
  }

  const matched = normalized.match(/([0-9,]+)\s*(名|人|店|社|拠点|ヶ所|か所|箇所|営業所)/);
  if (!matched) return null;

  const num = Number((matched[1] || "0").replace(/,/g, ""));
  if (!Number.isFinite(num)) return null;

  const unit = matched[2];
  return `${num.toLocaleString()}${unit === "名" || unit === "人" ? "名" : unit}`;
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
  timeoutMs = 10000
): Promise<PageData | null> {
  try {
    const response = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(timeoutMs),
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; MasterDataCrawler/1.0)",
        Accept: "text/html,application/xhtml+xml",
      },
      cache: "no-store",
    });

    if (!response.ok) return null;

    const contentType = response.headers.get("content-type") || "";
    if (!contentType.toLowerCase().includes("text/html")) return null;

    const html = await response.text();

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
  } catch {
    return null;
  }
}

async function fetchTopPage(
  seedUrl: string,
  timeoutMs = 10000
): Promise<PageData | null> {
  const normalized = normalizeSeedUrl(seedUrl);
  if (!normalized) return null;

  const first = await fetchPage(normalized, timeoutMs);
  if (first) return first;

  if (/^https:\/\//i.test(normalized)) {
    return await fetchPage(
      normalized.replace(/^https:\/\//i, "http://"),
      timeoutMs
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
    /(greeting|message|挨拶|メッセージ)/i.test(target) &&
    /(代表|社長|会長|理事長|所長|センター長|学院長|校長|学長|施設長|室長|president|director|chief)/i.test(
      target
    )
  ) {
    score += 90;
  }

  if (
    /(staff|member|members|社員紹介|社員インタビュー|経営者略歴)/i.test(
      target
    )
  ) {
    score -= 320;
  }

  if (
    /(recruit|career|job|jobs|entry|新卒|中途|採用|shop|shopinfo|campus|店舗|商品|製品|service|blog|news|topics|column|interview|voice|story|success|diary|faq|contact)/i.test(
      target
    )
  ) {
    score -= 380;
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

  const sortedLinks = [...topPage.links]
    .map((link) => ({
      url: link.url,
      score: getCandidateLinkScore(link, selectedFieldSet),
    }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score);

  for (const item of sortedLinks) {
    pushIfValid(item.url);
  }

  for (const path of COMMON_CANDIDATE_PATHS) {
    try {
      pushIfValid(new URL(path, base).toString());
    } catch {
      continue;
    }
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
  return isRepresentativeOnlyMode(selectedFieldSet) ? 320 : 360;
}

function getCandidatePageLimit(
  selectedFieldSet: Set<CrawlSelectableFieldKey>
) {
  if (isRepresentativeOnlyMode(selectedFieldSet)) {
    return 8;
  }

  if (selectedFieldSet.size === 1) {
    if (hasSelectedCrawlField(selectedFieldSet, "form_url")) return 5;
    if (hasSelectedOfficeFields(selectedFieldSet)) return 8;
    return 6;
  }

  if (
    selectedFieldSet.size <= 3 &&
    hasSelectedCrawlField(selectedFieldSet, "representative_name")
  ) {
    return 12;
  }

  if (selectedFieldSet.size <= 3 && !hasSelectedOfficeFields(selectedFieldSet)) {
    return 8;
  }

  if (selectedFieldSet.size <= 3) {
    return 10;
  }

  return 20;
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
    if (field === "employee_count") return !!best.employee_count?.value;
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
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score);

  for (const item of sortedLinks) {
    if (seen.has(item.url)) continue;
    if (HTML_PAGE_DENY_EXT.test(item.url)) continue;
    seen.add(item.url);
    urls.push(item.url);
  }

  return urls.slice(0, 8);
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
  selectedFieldSet: Set<CrawlSelectableFieldKey>
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
  const representativeNameFromText =
    extractRepresentativeNameFromText(page.structuredText);

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
    normalizeRepresentativeName(representativeNamePairValue),
    280 + boost + representativeScoreAdjustment
  );
  addBest(
    best,
    "representative_name",
    normalizeRepresentativeName(representativeNameTextValue),
    270 + boost + representativeScoreAdjustment
  );
  addBest(
    best,
    "representative_name",
    representativeNameFromText,
    220 + boost + representativeScoreAdjustment
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
    const employeeCountPairValue =
      pickPairValue(pairs, EMPLOYEE_COUNT_LABELS) ?? "";
    const employeeCountTextValue =
      extractSingleLineLabeledValue(page.structuredText, EMPLOYEE_COUNT_LABELS) ??
      "";

    addBest(
      best,
      "employee_count",
      normalizeEmployeeCount(employeeCountPairValue),
      200 + boost
    );
    addBest(
      best,
      "employee_count",
      normalizeEmployeeCount(employeeCountTextValue),
      195 + boost
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

export async function crawlCompanyWebsite(
  websiteUrl: string,
  selectedFields: CrawlSelectableFieldKey[] = DEFAULT_CRAWL_SELECTABLE_FIELDS
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
  const pageFetchTimeoutMs = representativeOnlyMode ? 3500 : 10000;

  const topPage = await fetchTopPage(
    websiteUrl,
    representativeOnlyMode ? 4000 : pageFetchTimeoutMs
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
      representative_title: null,
      capital: null,
      employee_count: null,
      business_content: null,
      offices: [],
    };
  }

  const topPageSelectedFieldSet =
    hasSelectedCrawlField(selectedFieldSet, "representative_name") &&
    !representativeOnlyMode
      ? new Set<CrawlSelectableFieldKey>(
          Array.from(selectedFieldSet).filter(
            (field): field is CrawlSelectableFieldKey =>
              field !== "representative_name"
          )
        )
      : selectedFieldSet;

  collectedPages.push(topPage);
  processPage(topPage, best, topPageSelectedFieldSet);

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
      if (canStopFetchingAdditionalPages(best, selectedFieldSet)) {
        break;
      }

      const page = await fetchPage(candidateUrl, pageFetchTimeoutMs);
      if (!page) continue;
      if (fetchedUrlSet.has(page.finalUrl)) continue;

      fetchedUrlSet.add(page.finalUrl);
      collectedPages.push(page);
      processPage(page, best, selectedFieldSet);

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
    const nestedLimit = representativeOnlyMode ? 6 : 40;

    for (const nestedUrl of nestedCandidateUrls.slice(0, nestedLimit)) {
      if (canStopFetchingAdditionalPages(best, selectedFieldSet)) {
        break;
      }

      const nestedPage = await fetchPage(
        nestedUrl,
        representativeOnlyMode ? 3000 : pageFetchTimeoutMs
      );
      if (!nestedPage) continue;
      if (fetchedUrlSet.has(nestedPage.finalUrl)) continue;

      fetchedUrlSet.add(nestedPage.finalUrl);
      collectedPages.push(nestedPage);
      processPage(nestedPage, best, selectedFieldSet);
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