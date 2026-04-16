import argparse
import json
import os
import re
import sys
import time
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


def emit(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False), flush=True)

def emit_scrape_progress(
    processed: int,
    total: int,
    success_count: int,
    failed_count: int,
    current_page: int,
    current_company_index: int,
    current_field: str,
    current_url: str,
    current_company: str = "",
) -> None:
    emit(
        {
            "type": "progress",
            "phase": "scrape_details",
            "processed": processed,
            "total": total,
            "success_count": success_count,
            "failed_count": failed_count,
            "current_page": current_page,
            "current_company_index": current_company_index,
            "current_field": current_field,
            "current_url": current_url,
            "current_company": current_company,
        }
    )


def normalize_grad_year(value: str) -> str:
    num = int(value)
    if num < 1 or num > 99:
      raise ValueError("grad year must be between 1 and 99")
    return str(num).zfill(2)


def build_search_url(grad_year: str) -> str:
    return f"https://job.mynavi.jp/{grad_year}/pc/search/query.html?OP:1/"


def build_session():
    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/147.0.0.0 Safari/537.36"
            )
        }
    )
    return session


def clean_text(value):
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    text = re.sub(r"\r", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_illegal_chars(value):
    if isinstance(value, str):
        return "".join(ch for ch in value if ch in "\n\r\t" or ord(ch) >= 32)
    return value


def get_soup_by_requests(session, url, timeout=20):
    res = session.get(url, timeout=timeout)
    res.raise_for_status()
    return BeautifulSoup(res.content, "html.parser")


def get_soup_by_playwright(page, url, wait_sec=2.0):
    page.goto(url, timeout=90000, wait_until="domcontentloaded")
    time.sleep(wait_sec)
    return BeautifulSoup(page.content(), "html.parser")


def normalize_label(text):
    text = clean_text(text)
    text = text.replace("　", " ")
    text = re.sub(r"\s+", "", text)
    return text


def parse_all_tr_th_td(soup):
    rows = {}
    for tr in soup.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue

        key_raw = clean_text(th.get_text(" ", strip=True))
        val_raw = clean_text(td.get_text("\n", strip=True))

        key_norm = normalize_label(key_raw)
        if key_norm and key_norm not in rows:
            rows[key_norm] = {
                "label": key_raw,
                "value": val_raw,
                "th": th,
                "td": td,
            }
    return rows


def get_value_from_rows(rows, exact_labels=None, partial_labels=None):
    exact_labels = exact_labels or []
    partial_labels = partial_labels or []

    for label in exact_labels:
        key = normalize_label(label)
        if key in rows and rows[key]["value"]:
            return rows[key]["value"]

    for _, item in rows.items():
        label_norm = normalize_label(item["label"])
        for partial in partial_labels:
            p = normalize_label(partial)
            if p and p in label_norm and item["value"]:
                return item["value"]

    return ""


def extract_email_from_text(text):
    if not text:
        return ""
    m = re.search(r"[\w\.\-\+]+@[\w\.\-]+\.\w+", text)
    return m.group(0) if m else ""


def is_probable_company_url(url):
    if not url:
        return False

    lower = url.lower()

    ng_domains = [
        "job.mynavi.jp",
        "maps.google.",
        "goo.gl",
        "instagram.com",
        "x.com",
        "twitter.com",
        "youtube.com",
        "youtu.be",
        "facebook.com",
        "line.me",
    ]
    if any(d in lower for d in ng_domains):
        return False

    return lower.startswith("http://") or lower.startswith("https://")


def extract_company_site_url_from_outline(soup):
    rows = parse_all_tr_th_td(soup)

    url_labels_exact = [
        "URL",
        "ホームページ",
        "企業ホームページ",
        "会社ホームページ",
        "HP",
        "企業サイト",
        "コーポレートサイト",
    ]

    for label in url_labels_exact:
        key = normalize_label(label)
        if key in rows:
            td = rows[key]["td"]

            a_tags = td.find_all("a", href=True)
            for a in a_tags:
                href = clean_text(a.get("href"))
                if is_probable_company_url(href):
                    return href

            text = clean_text(td.get_text(" ", strip=True))
            m = re.search(r"https?://[^\s]+", text)
            if m and is_probable_company_url(m.group(0)):
                return m.group(0)

    for _, item in rows.items():
        label_norm = normalize_label(item["label"])
        if any(normalize_label(x) in label_norm for x in url_labels_exact):
            td = item["td"]
            a_tags = td.find_all("a", href=True)
            for a in a_tags:
                href = clean_text(a.get("href"))
                if is_probable_company_url(href):
                    return href

            text = clean_text(td.get_text(" ", strip=True))
            m = re.search(r"https?://[^\s]+", text)
            if m and is_probable_company_url(m.group(0)):
                return m.group(0)

    for a in soup.find_all("a", href=True):
        href = clean_text(a.get("href"))
        if is_probable_company_url(href):
            return href

    return ""


def extract_industries(soup):
    items = [clean_text(el.get_text(" ", strip=True)) for el in soup.select("div.category ul li span")]
    items = [x for x in items if x]
    return " / ".join(items)


def extract_company_name(soup):
    h1 = soup.select_one("div.heading1 h1")
    return clean_text(h1.get_text(" ", strip=True)) if h1 else ""


def extract_outline_info(session, outline_url):
    soup = get_soup_by_requests(session, outline_url)
    rows = parse_all_tr_th_td(soup)

    data = {
        "会社名": extract_company_name(soup),
        "本社郵便番号": get_value_from_rows(
            rows,
            exact_labels=["本社郵便番号", "郵便番号"],
            partial_labels=[],
        ),
        "本社所在地": get_value_from_rows(
            rows,
            exact_labels=["本社所在地", "所在地"],
            partial_labels=[],
        ),
        "本社電話番号": get_value_from_rows(
            rows,
            exact_labels=["本社電話番号", "電話番号", "代表電話番号"],
            partial_labels=[],
        ),
        "URL": extract_company_site_url_from_outline(soup),
        "E-MAIL": "",
        "設立": get_value_from_rows(
            rows,
            exact_labels=["設立", "創業"],
            partial_labels=[],
        ),
        "資本金": get_value_from_rows(
            rows,
            exact_labels=["資本金"],
            partial_labels=[],
        ),
        "従業員": get_value_from_rows(
            rows,
            exact_labels=["従業員", "社員数"],
            partial_labels=[],
        ),
        "売上高": get_value_from_rows(
            rows,
            exact_labels=["売上高", "年商"],
            partial_labels=[],
        ),
        "代表者": get_value_from_rows(
            rows,
            exact_labels=["代表者", "代表取締役", "社長"],
            partial_labels=[],
        ),
        "業種": extract_industries(soup),
        "採用実績（学校）": "",
        "採用実績（人数）": "",
        "問合せ先": "",
        "交通機関": "",
        "元URL": outline_url,
    }

    return data


def extract_recruit_results_with_playwright(page, outline_url):
    soup = get_soup_by_playwright(page, outline_url)

    school = ""
    school_tag = soup.find("tr", id="school")
    if school_tag and school_tag.find("td"):
        school = clean_text(
            school_tag.find("td").get_text(" ", strip=True)
        ).replace("もっと見る", "").strip()

    ninzuu = ""
    ninzuu_tag = soup.find("tr", id="ninzuu")
    if ninzuu_tag and ninzuu_tag.find("td"):
        raw = ninzuu_tag.find("td").get_text(separator="\n", strip=True)
        lines = [clean_text(line) for line in raw.splitlines() if clean_text(line)]
        ninzuu = "\n".join(lines)

    return school, ninzuu


def extract_contact_info_exact(soup):
    table = soup.find("table", class_="dataTable last dataTable02 ver02")
    if not table:
        return {}

    contact_data = {}
    rows = table.find_all("tr")

    for row in rows:
        th = row.find("th")
        td = row.find("td")
        if th and td:
            label = clean_text(th.get_text(strip=True))
            value = clean_text(td.get_text(separator="\n", strip=True))
            contact_data[label] = value

    return contact_data


def extract_contact_info_flexible(soup):
    contact_data = {}

    target_labels = {
        "問合せ先", "交通機関", "E-MAIL", "メールアドレス", "MAIL", "Mail", "Email", "EMAIL"
    }

    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        temp = {}

        for row in rows:
            th = row.find("th")
            td = row.find("td")
            if th and td:
                label = clean_text(th.get_text(strip=True))
                value = clean_text(td.get_text(separator="\n", strip=True))
                if label:
                    temp[label] = value

        if any(k in temp for k in target_labels):
            contact_data.update(temp)

    return contact_data


def merge_contact_dict(base_dict, new_dict):
    merged = dict(base_dict)
    for k, v in new_dict.items():
        if v and (k not in merged or not merged[k]):
            merged[k] = v
    return merged


def get_contact_info_from_outline_url(session, outline_url):
    base = "https://job.mynavi.jp"
    employment_url = outline_url.replace("outline.html", "employment.html")

    contact_info = {}

    try:
        soup_emp = get_soup_by_requests(session, employment_url)

        info1 = extract_contact_info_exact(soup_emp)
        if not info1:
            info1 = extract_contact_info_flexible(soup_emp)
        contact_info = merge_contact_dict(contact_info, info1)

        detail_links = soup_emp.find_all("a", href=re.compile(r"displayEmployment/index/"))
        for a in detail_links:
            href = a.get("href")
            if not href:
                continue

            detail_url = urljoin(base, href)
            try:
                soup_detail = get_soup_by_requests(session, detail_url)

                info2 = extract_contact_info_exact(soup_detail)
                if not info2:
                    info2 = extract_contact_info_flexible(soup_detail)

                contact_info = merge_contact_dict(contact_info, info2)

                if (
                    contact_info.get("問合せ先")
                    and (
                        contact_info.get("E-MAIL")
                        or contact_info.get("メールアドレス")
                        or contact_info.get("MAIL")
                        or contact_info.get("Mail")
                        or contact_info.get("Email")
                        or contact_info.get("EMAIL")
                    )
                    and contact_info.get("交通機関")
                ):
                    break

            except Exception:
                continue

    except Exception as e:
        return {"エラー": str(e)}

    return contact_info


def normalize_contact_fields(contact_info):
    email = ""
    for key in ["E-MAIL", "メールアドレス", "MAIL", "Mail", "Email", "EMAIL"]:
        if contact_info.get(key):
            email = contact_info[key]
            break

    if not email:
        email = extract_email_from_text(contact_info.get("問合せ先", ""))

    return {
        "問合せ先": contact_info.get("問合せ先", ""),
        "交通機関": contact_info.get("交通機関", ""),
        "E-MAIL": email,
    }


def scrape_one_company(
    session,
    page,
    outline_url,
    source_page_num: int,
    company_index: int,
    total_companies: int,
    success_count: int,
    failed_count: int,
):
    emit_scrape_progress(
        processed=max(company_index - 1, 0),
        total=total_companies,
        success_count=success_count,
        failed_count=failed_count,
        current_page=source_page_num,
        current_company_index=company_index,
        current_field="outline",
        current_url=outline_url,
        current_company="",
    )

    data = extract_outline_info(session, outline_url)
    current_company = data.get("会社名", "")

    emit_scrape_progress(
        processed=max(company_index - 1, 0),
        total=total_companies,
        success_count=success_count,
        failed_count=failed_count,
        current_page=source_page_num,
        current_company_index=company_index,
        current_field="recruit_results",
        current_url=outline_url,
        current_company=current_company,
    )

    school, ninzuu = extract_recruit_results_with_playwright(page, outline_url)
    data["採用実績（学校）"] = school
    data["採用実績（人数）"] = ninzuu

    emit_scrape_progress(
        processed=max(company_index - 1, 0),
        total=total_companies,
        success_count=success_count,
        failed_count=failed_count,
        current_page=source_page_num,
        current_company_index=company_index,
        current_field="contact",
        current_url=outline_url,
        current_company=current_company,
    )

    contact_info = get_contact_info_from_outline_url(session, outline_url)
    normalized_contact = normalize_contact_fields(contact_info)

    data["問合せ先"] = normalized_contact["問合せ先"]
    data["交通機関"] = normalized_contact["交通機関"]
    data["E-MAIL"] = normalized_contact["E-MAIL"]

    if not data["URL"]:
        for v in contact_info.values():
            if not isinstance(v, str):
                continue
            m = re.search(r"https?://[^\s]+", v)
            if m and is_probable_company_url(m.group(0)):
                data["URL"] = m.group(0)
                break

    emit_scrape_progress(
        processed=max(company_index - 1, 0),
        total=total_companies,
        success_count=success_count,
        failed_count=failed_count,
        current_page=source_page_num,
        current_company_index=company_index,
        current_field="sanitize",
        current_url=outline_url,
        current_company=current_company,
    )

    data = {k: clean_illegal_chars(v) for k, v in data.items()}

    return data


def get_company_urls_from_page(page, grad_year: str):
    urls = []
    anchors = page.query_selector_all("a.js-add-examination-list-text")
    for a in anchors:
        href = a.get_attribute("href")
        if href and href.startswith(f"/{grad_year}/pc/search/corp") and href.endswith("/outline.html"):
            urls.append("https://job.mynavi.jp" + href)
    return urls

def extract_total_companies_from_html(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text(" ", strip=True))

    patterns = [
        r"企業検索結果\s*([0-9,]+)\s*社",
        r"全\s*([0-9,]+)\s*社\s*\d+\s*-\s*\d+\s*社表示",
    ]

    for pattern in patterns:
        m = re.search(pattern, page_text)
        if m:
            return int(m.group(1).replace(",", ""))

    return 0


def wait_for_search_result_page_ready(page, timeout_ms: int = 20000):
    page.wait_for_selector("a.js-add-examination-list-text", timeout=timeout_ms)

    end_time = time.time() + (timeout_ms / 1000)
    while time.time() < end_time:
        total_pages = extract_total_pages_from_html(page.content())
        if total_pages > 1:
            return
        time.sleep(0.5)


def extract_total_pages_from_html(html: str) -> int:
    total_companies = extract_total_companies_from_html(html)
    if total_companies > 0:
        return max(1, (total_companies + 99) // 100)

    slash_numbers = [
        int(value)
        for value in re.findall(r"\(\s*\d+\s*/\s*(\d+)\s*\)", html)
        if value.isdigit()
    ]
    slash_numbers = [value for value in slash_numbers if 1 <= value <= 9999]
    if slash_numbers:
        return max(slash_numbers)

    soup = BeautifulSoup(html, "html.parser")

    slash_text_numbers = []
    page_text = soup.get_text(" ", strip=True)
    for value in re.findall(r"\(\s*\d+\s*/\s*(\d+)\s*\)", page_text):
        if value.isdigit():
            num = int(value)
            if 1 <= num <= 9999:
                slash_text_numbers.append(num)

    if slash_text_numbers:
        return max(slash_text_numbers)

    text_numbers = []
    for tag in soup.find_all(["a", "button", "span"]):
        text = clean_text(tag.get_text(" ", strip=True))
        if text.isdigit():
            num = int(text)
            if 1 <= num <= 9999:
                text_numbers.append(num)

    if text_numbers:
        return max(text_numbers)

    return 1

def detect_total_pages_by_expanding(page, grad_year: str, emit_progress_enabled: bool = False) -> int:
    detected_total_pages = max(1, extract_total_pages_from_html(page.content()))
    visited = set()

    if emit_progress_enabled:
        emit(
            {
                "type": "progress",
                "phase": "count_pages",
                "current_page": 1,
                "current_field": "count_parse",
                "detected_total_pages": detected_total_pages,
            }
        )

    while detected_total_pages not in visited:
        visited.add(detected_total_pages)

        if detected_total_pages <= 1:
            break

        try:
            move_to_page(page, grad_year, detected_total_pages)
        except Exception:
            break

        next_total_pages = max(
            detected_total_pages,
            extract_total_pages_from_html(page.content())
        )

        if emit_progress_enabled:
            emit(
                {
                    "type": "progress",
                    "phase": "count_pages",
                    "current_page": detected_total_pages,
                    "current_field": "count_parse",
                    "detected_total_pages": next_total_pages,
                }
            )

        if next_total_pages <= detected_total_pages:
            break

        detected_total_pages = next_total_pages

    return detected_total_pages


def count_total_pages(grad_year: str, headless: bool = True) -> int:
    url = build_search_url(grad_year)

    emit(
        {
            "type": "progress",
            "phase": "count_pages",
            "current_page": 1,
            "current_field": "count_open",
            "detected_total_pages": 0,
        }
    )

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.goto(url, timeout=90000, wait_until="domcontentloaded")
        wait_for_search_result_page_ready(page)

        total_pages = detect_total_pages_by_expanding(
            page=page,
            grad_year=grad_year,
            emit_progress_enabled=True,
        )

        browser.close()

    emit(
        {
            "type": "result",
            "phase": "count_pages",
            "total_pages": total_pages,
        }
    )

    return total_pages


def move_to_page(page, grad_year: str, page_num: int):
    with page.expect_navigation(wait_until="domcontentloaded", timeout=90000):
        page.evaluate(f"""
            setSpecifiedPage('displaySearchCorpListByGenCondDispForm', '{page_num}', '100');
            setActionMode(
              'displaySearchCorpListByGenCondDispForm',
              '/{grad_year}/pc/corpinfo/searchCorpListByGenCond/doSpecifiedPage'
            );
            document.forms['displaySearchCorpListByGenCondDispForm'].submit();
        """)
    wait_for_search_result_page_ready(page)


def collect_company_urls(grad_year: str, page_count, headless: bool = True):
    search_url = build_search_url(grad_year)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()

        page.goto(search_url, timeout=90000, wait_until="domcontentloaded")
        wait_for_search_result_page_ready(page)

        total_pages = detect_total_pages_by_expanding(
            page=page,
            grad_year=grad_year,
            emit_progress_enabled=False,
        )

        page.goto(search_url, timeout=90000, wait_until="domcontentloaded")
        wait_for_search_result_page_ready(page)

        selected_page_count = total_pages if page_count == "all" else min(int(page_count), total_pages)

        emit(
            {
                "type": "meta",
                "total_pages": total_pages,
                "selected_page_count": selected_page_count,
            }
        )

        url_to_page = {}

        urls = get_company_urls_from_page(page, grad_year)
        for url in urls:
            url_to_page.setdefault(url, 1)

        emit(
            {
                "type": "progress",
                "phase": "collect_urls",
                "current_page": 1,
                "current_field": "collect_urls",
                "total_pages": total_pages,
                "collected_urls": len(url_to_page),
            }
        )

        for page_num in range(2, selected_page_count + 1):
            for attempt in range(3):
                try:
                    if attempt > 0:
                        page.goto(search_url, timeout=90000, wait_until="domcontentloaded")
                        wait_for_search_result_page_ready(page)

                    move_to_page(page, grad_year, page_num)
                    urls = get_company_urls_from_page(page, grad_year)
                    for url in urls:
                        url_to_page.setdefault(url, page_num)
                    break
                except Exception:
                    if attempt == 2:
                        pass

            emit(
                {
                    "type": "progress",
                    "phase": "collect_urls",
                    "current_page": page_num,
                    "current_field": "collect_urls",
                    "total_pages": total_pages,
                    "collected_urls": len(url_to_page),
                }
            )

        browser.close()

    url_page_pairs = list(url_to_page.items())
    return total_pages, selected_page_count, url_page_pairs

CSV_COLUMNS = [
    "会社名",
    "本社郵便番号",
    "本社所在地",
    "本社電話番号",
    "URL",
    "E-MAIL",
    "設立",
    "資本金",
    "従業員",
    "売上高",
    "代表者",
    "業種",
    "採用実績（学校）",
    "採用実績（人数）",
    "問合せ先",
    "交通機関",
    "元URL",
]


def write_results_csv(results, output_csv):
    df = pd.DataFrame(results)
    if not df.empty:
        for col in CSV_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        df = df[CSV_COLUMNS]
    else:
        df = pd.DataFrame(columns=CSV_COLUMNS)

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")


def save_scrape_state(
    state_file: str,
    grad_year: str,
    total_pages: int,
    selected_page_count: int,
    url_page_pairs,
    processed_count: int,
    success_count: int,
    failed_count: int,
    results,
):
    payload = {
        "grad_year": grad_year,
        "total_pages": total_pages,
        "selected_page_count": selected_page_count,
        "url_page_pairs": [[url, page_num] for url, page_num in url_page_pairs],
        "processed_count": processed_count,
        "success_count": success_count,
        "failed_count": failed_count,
        "results": results,
    }

    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)


def load_scrape_state(state_file: str):
    with open(state_file, "r", encoding="utf-8") as f:
        return json.load(f)

def run_scrape(
    grad_year: str,
    page_count,
    output_csv: str,
    state_file: str,
    headless: bool = True
):
    total_pages, selected_page_count, url_page_pairs = collect_company_urls(
        grad_year=grad_year,
        page_count=page_count,
        headless=headless,
    )

    save_scrape_state(
        state_file=state_file,
        grad_year=grad_year,
        total_pages=total_pages,
        selected_page_count=selected_page_count,
        url_page_pairs=url_page_pairs,
        processed_count=0,
        success_count=0,
        failed_count=0,
        results=[],
    )

    run_scrape_resume(
        state_file=state_file,
        output_csv=output_csv,
        headless=headless,
    )

def run_scrape_resume(state_file: str, output_csv: str, headless: bool = True):
    state = load_scrape_state(state_file)

    grad_year = state["grad_year"]
    total_pages = int(state["total_pages"])
    selected_page_count = int(state["selected_page_count"])
    url_page_pairs = [(item[0], int(item[1])) for item in state["url_page_pairs"]]
    processed_count = int(state.get("processed_count", 0))
    success_count = int(state.get("success_count", 0))
    failed_count = int(state.get("failed_count", 0))
    results = state.get("results", [])

    session = build_session()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()

        total_urls = len(url_page_pairs)

        for index, (url, source_page_num) in enumerate(
            url_page_pairs[processed_count:], start=processed_count + 1
        ):
            page = context.new_page()
            current_company = ""

            try:
                row = scrape_one_company(
                    session=session,
                    page=page,
                    outline_url=url,
                    source_page_num=source_page_num,
                    company_index=index,
                    total_companies=total_urls,
                    success_count=success_count,
                    failed_count=failed_count,
                )
                current_company = row.get("会社名", "")
                results.append(row)
                success_count += 1
            except (PlaywrightTimeoutError, requests.RequestException, Exception):
                failed_count += 1
            finally:
                page.close()

                save_scrape_state(
                    state_file=state_file,
                    grad_year=grad_year,
                    total_pages=total_pages,
                    selected_page_count=selected_page_count,
                    url_page_pairs=url_page_pairs,
                    processed_count=index,
                    success_count=success_count,
                    failed_count=failed_count,
                    results=results,
                )
                write_results_csv(results, output_csv)

                emit(
                    {
                        "type": "progress",
                        "phase": "scrape_details",
                        "processed": index,
                        "total": total_urls,
                        "success_count": success_count,
                        "failed_count": failed_count,
                        "current_url": url,
                        "current_page": source_page_num,
                        "current_company_index": index,
                        "current_field": "completed",
                        "current_company": current_company,
                    }
                )
                time.sleep(1.0)

        browser.close()

    write_results_csv(results, output_csv)

    emit(
        {
            "type": "result",
            "phase": "scrape",
            "total_pages": total_pages,
            "selected_page_count": selected_page_count,
            "total_urls": len(url_page_pairs),
            "success_count": success_count,
            "failed_count": failed_count,
            "csv_path": output_csv,
            "csv_file_name": os.path.basename(output_csv),
        }
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["count_pages", "scrape", "scrape_resume"], required=True)
    parser.add_argument("--grad-year", required=True)
    parser.add_argument("--page-count", default="all")
    parser.add_argument("--output-csv", default="")
    parser.add_argument("--state-file", default="")
    parser.add_argument("--headless", default="true")
    args = parser.parse_args()

    try:
        grad_year = normalize_grad_year(args.grad_year)
        headless = str(args.headless).lower() != "false"

        if args.mode == "count_pages":
            count_total_pages(grad_year=grad_year, headless=headless)
            return

        if not args.output_csv:
            raise ValueError("output csv is required in scrape mode")

        if args.mode == "scrape_resume":
            if not args.state_file:
                raise ValueError("state file is required in scrape_resume mode")

            run_scrape_resume(
                state_file=args.state_file,
                output_csv=args.output_csv,
                headless=headless,
            )
            return

        if not args.state_file:
            raise ValueError("state file is required in scrape mode")

        page_count = args.page_count
        if page_count != "all":
            page_count = int(page_count)
            if page_count < 1 or page_count > 999:
                raise ValueError("page count must be between 1 and 999")

        run_scrape(
            grad_year=grad_year,
            page_count=page_count,
            output_csv=args.output_csv,
            state_file=args.state_file,
            headless=headless,
        )
    except Exception as e:
        emit({"type": "error", "message": str(e)})
        sys.exit(1)


if __name__ == "__main__":
    main()