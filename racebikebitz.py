"""
Scrapes every product from racebikebitzusa.com (a single-brand Samco Sport
storefront). Discovery uses the manufacturer listing page (/Samco-Sport),
which paginates through the full catalog. Each unique product URL is then
visited to extract product detail; for products with colour/option variants,
the site's `getotp` AJAX endpoint is called per option value to pull the
variant-specific price, SKU (model) and stock status (these differ from the
base product's displayed price/SKU/availability once an option is chosen).

Output: a Shopify product-CSV-compatible file (one row per variant, extra
rows for additional images), written incrementally and resumable by
Product URL. Failures are printed to the terminal, not logged to a file.
"""

import csv
import json
import os
import random
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    _config = json.load(f)

BASE = _config["BASE"]
BRAND_LISTING = _config["BRAND_LISTING"]
OUTPUT_CSV = _config["OUTPUT_CSV"]
HEADERS = _config["HEADERS"]
CATEGORIES = _config.get("CATEGORIES", [])
GETOTP_URL = urljoin(BASE, "index.php")

MAX_WORKERS = 6

# slug -> category name, built once by discover_category_map() before scraping.
CATEGORY_MAP = {}

CSV_FIELDS = [
    "Handle", "Title", "Body (HTML)", "Vendor", "Type", "Tags", "Published",
    "Option1 Name", "Option1 Value", "Option2 Name", "Option2 Value",
    "Option3 Name", "Option3 Value",
    "Variant SKU", "Variant Grams", "Variant Inventory Tracker",
    "Variant Inventory Qty", "Variant Inventory Policy",
    "Variant Fulfillment Service", "Variant Price", "Variant Compare At Price",
    "Variant Requires Shipping", "Variant Taxable", "Variant Barcode",
    "Image Src", "Image Position", "Image Alt Text", "Gift Card",
    "SEO Title", "SEO Description",
    "Google Shopping / Google Product Category", "Google Shopping / Gender",
    "Google Shopping / Age Group", "Google Shopping / MPN",
    "Google Shopping / AdWords Grouping", "Google Shopping / AdWords Labels",
    "Google Shopping / Condition", "Google Shopping / Custom Product",
    "Google Shopping / Custom Label 0", "Google Shopping / Custom Label 1",
    "Google Shopping / Custom Label 2", "Google Shopping / Custom Label 3",
    "Google Shopping / Custom Label 4",
    "Variant Image", "Variant Weight Unit", "Variant Tax Code",
    "Cost per item", "Status",
    # Custom extras (ignored by Shopify's importer, kept for the fields it
    # has no native per-row equivalent for).
    "MPN", "Currency", "Availability", "Product URL", "Category",
]

csv_lock = threading.Lock()
_thread_local = threading.local()


def new_session(retries=6):
    s = requests.Session()
    s.headers.update(HEADERS)
    # Warm up: category/product pages 302-redirect to "/" without session cookies.
    delay = 1.0
    last_exc = None
    for _ in range(retries):
        try:
            s.get(BASE, timeout=30)
            return s
        except requests.RequestException as e:
            last_exc = e
            time.sleep(delay)
            delay = min(delay * 2, 15)
    raise last_exc


def get_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = new_session()
    return _thread_local.session


def fetch(url, session, retries=6, params=None, json_mode=False, min_length=500):
    delay = 1.0
    for _ in range(retries):
        try:
            r = session.get(url, params=params, headers={"Referer": BASE}, timeout=30)
            if r.status_code == 200 and len(r.content) > (20 if json_mode else min_length):
                return r.json() if json_mode else r.text
            if r.status_code in (403, 429, 500, 502, 503, 504):
                time.sleep(delay)
                delay = min(delay * 2, 20)
                continue
            return None
        except (requests.RequestException, ValueError):
            time.sleep(delay)
            delay = min(delay * 2, 20)
    return None


LISTING_RE = re.compile(r"Showing\s+[\d,]+\s+to\s+[\d,]+\s+of\s+[\d,]+\s+\((\d+)\s+Pages?\)")


def fetch_listing_page(url, session_holder, label, max_bad_retries=6):
    """Fetch a category/brand listing page, validating it actually contains
    the pagination summary text. A genuine listing page always renders this
    (even a single-product category), so a 200 response without it means
    the site silently served something else -- observed in practice after
    a burst of ~150+ requests in one session, where the server returns the
    homepage instead of the listing without any error status. Refresh the
    session and retry with a cooldown rather than trusting a false-empty
    result. Returns (html, total_pages) or (None, None)."""
    for attempt in range(max_bad_retries):
        html = fetch(url, session_holder[0])
        if html:
            m = LISTING_RE.search(html)
            if m:
                return html, int(m.group(1))
        if attempt < max_bad_retries - 1:
            print(f"  [{label}] bad response for {url}, refreshing session and retrying...")
            time.sleep(5 + attempt * 5)
            try:
                session_holder[0] = new_session()
            except requests.RequestException:
                pass
    return None, None


def discover_product_urls():
    session_holder = [new_session()]
    urls = []
    seen = set()
    page = 1
    total_pages = None
    while True:
        html, pages_found = fetch_listing_page(f"{BRAND_LISTING}?page={page}", session_holder, "discover")
        if not html:
            print(f"  [discover] page {page} failed validation after retries, stopping.")
            break
        soup = BeautifulSoup(html, "lxml")
        links = soup.select("div.product-layout div.caption div.name a[href]")
        if not links:
            break
        new_count = 0
        for a in links:
            href = a.get("href")
            if href:
                href = href.split("?", 1)[0]
            if href and href not in seen:
                seen.add(href)
                urls.append(href)
                new_count += 1

        if total_pages is None and pages_found:
            total_pages = pages_found
            print(f"  [discover] catalog reports {total_pages} pages total.")

        print(f"  [discover] page {page}: +{new_count} new (total {len(urls)})")

        if total_pages and page >= total_pages:
            break
        if new_count == 0:
            break
        page += 1
        time.sleep(0.3 + random.random() * 0.3)

    return urls


def discover_category_map():
    """Crawl every category/sub-category listing page (with pagination) and
    map each product's URL slug to its category name, so products discovered
    via the brand catalog can still be tagged with a real category."""
    session_holder = [new_session()]
    mapping = {}
    for cat in CATEGORIES:
        name, cat_url = cat["name"], cat["url"]
        page = 1
        total_pages = None
        found_count = 0
        new_count = 0
        while True:
            html, pages_found = fetch_listing_page(f"{cat_url}?page={page}", session_holder, "categories")
            if not html:
                print(f"  [categories] {name}: page {page} failed validation after retries, stopping category early.")
                break
            soup = BeautifulSoup(html, "lxml")
            links = soup.select("div.product-layout div.caption div.name a[href]")
            if not links:
                break
            for a in links:
                href = a.get("href")
                if not href:
                    continue
                found_count += 1
                slug = slugify(href)
                if slug not in mapping:
                    mapping[slug] = name
                    new_count += 1

            if total_pages is None and pages_found:
                total_pages = pages_found
            if total_pages and page >= total_pages:
                break
            page += 1
            time.sleep(0.3 + random.random() * 0.3)
        already_tagged = found_count - new_count
        note = f" ({already_tagged} already tagged by an earlier category)" if already_tagged else ""
        print(f"  [categories] {name}: {found_count} products found, {new_count} newly tagged{note}")
        time.sleep(2.0)
    return mapping


def text_or_none(el):
    return el.get_text(strip=True) if el else None


PRICE_RE = re.compile(r"([£$€])?\s*([\d,]+\.\d{2})\s*([A-Z]{3})?")
CURRENCY_SYMBOLS = {"£": "GBP", "$": "USD", "€": "EUR"}


def parse_price(price_text):
    """'$238.95USD' -> ('238.95', 'USD'); '£156.94' -> ('156.94', 'GBP')."""
    if not price_text:
        return "", ""
    m = PRICE_RE.search(price_text.replace(",", ""))
    if not m:
        return "", ""
    symbol, amount, code = m.group(1), m.group(2), m.group(3)
    return amount, (code or CURRENCY_SYMBOLS.get(symbol) or "USD")


def parse_product_page(html):
    """Extract base product info + option-0 (value_id, label) pairs from a
    product detail page. Variant-specific price/sku/stock are fetched
    separately via fetch_variant()."""
    soup = BeautifulSoup(html, "lxml")

    title = text_or_none(soup.select_one("h1.product-title"))

    crumbs = [
        a.get_text(strip=True)
        for a in soup.select("ul.breadcrumb li a")
        if a.get_text(strip=True)
    ]
    breadcrumb = " > ".join(crumbs)
    # Category = breadcrumb trail minus the leading Home/brand and the
    # trailing product-title crumb.
    category = " > ".join(crumbs[1:-1]) if len(crumbs) > 2 else ""

    price_new = text_or_none(soup.select_one(".price-section .price-new"))

    brand = None
    for li in soup.select("ul.product-section li"):
        if li.get_text(strip=True).startswith("Brand:"):
            a = li.select_one("a")
            brand = text_or_none(a)
            break

    sku = text_or_none(soup.select_one("#otp-model"))
    availability = text_or_none(soup.select_one("#otp-stock"))

    desc_el = soup.select_one("#tab-description")
    description = desc_el.get_text(" ", strip=True) if desc_el else None

    image_urls = []
    for a in soup.select("#gallery li a[href]"):
        href = a.get("href")
        if href and href not in image_urls:
            image_urls.append(href)
    if not image_urls:
        img = soup.select_one("#gallery_zoom")
        if img and img.get("src"):
            image_urls.append(img.get("src"))

    product_id_el = soup.select_one('input[name="product_id"]')
    product_id = product_id_el.get("value") if product_id_el else None

    option_name = None
    option_values = []  # list of (value_id, value_text)
    wrap0 = soup.select_one("[class*='otp-option-0'][class*='-wrap']")
    if wrap0:
        option_name = text_or_none(wrap0.select_one("label"))
        select0 = wrap0.select_one("select")
        if select0:
            for opt in select0.select("option"):
                val = opt.get("value")
                txt = opt.get_text(strip=True)
                if val and txt and "Please Select" not in txt:
                    option_values.append((val, txt))

    # Some products (a small minority -- e.g. hoses that differ by model
    # year before a colour can even be chosen) have a second, dependent
    # option level: picking an option_values[i] populates a child dropdown
    # via a separate AJAX call, and getotp requires BOTH ids together to
    # return a real variant (parent-only returns an empty response).
    option2_name = None
    option2_type_id = None
    wrap1 = soup.select_one("[class*='otp-option-1'][class*='-wrap']")
    if wrap1:
        option2_name = text_or_none(wrap1.select_one("label"))
        select1 = wrap1.select_one("select")
        if select1:
            option2_type_id = select1.get("option")

    return {
        "title": title,
        "breadcrumb": breadcrumb,
        "category": category,
        "brand": brand,
        "base_sku": sku,
        "base_price": price_new,
        "base_availability": availability,
        "description": description,
        "image_urls": image_urls,
        "product_id": product_id,
        "option_name": option_name,
        "option_values": option_values,
        "option2_name": option2_name,
        "option2_type_id": option2_type_id,
    }


def fetch_variant(session, product_id, value_id, child_value_id=0):
    data = fetch(
        GETOTP_URL,
        session,
        params={
            "route": "product/product/getotp",
            "product_id": product_id,
            "parent_option_value_id": value_id,
            "child_option_value_id": child_value_id,
            "grandchild_option_value_id": 0,
        },
        json_mode=True,
    )
    if not data:
        return None
    return {
        "sku": data.get("model") or "",
        "price": data.get("price") or "",
        "availability": data.get("stock") or "",
        "quantity": data.get("quantity") or "0",
        "out_of_stock": data.get("out_of_stock"),
    }


def fetch_option_children(session, product_id, child_option_id, parent_value_id):
    """For a two-level option product: given the chosen parent (e.g. year)
    value, fetch the dependent child (e.g. colour) values available for
    that combination. Returns a list of (value_id, value_text)."""
    html = fetch(
        GETOTP_URL,
        session,
        params={
            "route": "product/product/getotpchildvalues",
            "product_id": product_id,
            "child_option_id": child_option_id,
            "parent_option_value_id": parent_value_id,
            "mode": "select",
        },
        min_length=20,
    )
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    children = []
    for opt in soup.select("option"):
        val = opt.get("value")
        txt = opt.get_text(strip=True)
        if val and txt and "Please Select" not in txt:
            children.append((val, txt))
    return children


def fallback_variant(info):
    return {
        "option_value": "", "option2_value": "",
        "sku": info["base_sku"], "price": info["base_price"],
        "availability": info["base_availability"], "quantity": "0",
    }


def collect_variants(session, url, info):
    """Fetch real per-variant price/SKU/availability for every option
    combination a product offers. Falls back to a single row built from the
    base product page if it has no options, or if every AJAX lookup fails."""
    if not (info["product_id"] and info["option_values"]):
        return [fallback_variant(info)]

    product_id = info["product_id"]
    variants = []

    if info.get("option2_type_id"):
        for value_id, value_text in info["option_values"]:
            time.sleep(0.15 + random.random() * 0.2)
            children = fetch_option_children(session, product_id, info["option2_type_id"], value_id)
            if not children:
                print(f"  [child-values-failed] {url} option_value={value_id}")
                continue
            for child_id, child_text in children:
                time.sleep(0.15 + random.random() * 0.2)
                v = fetch_variant(session, product_id, value_id, child_id)
                if v is None:
                    print(f"  [variant-failed] {url} option_value={value_id}/{child_id}")
                    continue
                v["option_value"] = value_text
                v["option2_value"] = child_text
                variants.append(v)
    else:
        for value_id, value_text in info["option_values"]:
            time.sleep(0.15 + random.random() * 0.2)
            v = fetch_variant(session, product_id, value_id)
            if v is None:
                print(f"  [variant-failed] {url} option_value={value_id}")
                continue
            v["option_value"] = value_text
            v["option2_value"] = ""
            variants.append(v)

    if not variants:
        variants = [fallback_variant(info)]
    return variants


def slugify(url):
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    slug = re.sub(r"[^a-zA-Z0-9\-]+", "-", slug).strip("-").lower()
    return slug or "product"


def build_rows(url, info, variants):
    """variants: list of dicts with keys option_value, sku, price,
    availability, quantity. Returns a list of CSV row dicts (Shopify
    multi-row-per-handle format: first row carries product-level fields,
    subsequent variant rows repeat only variant-level fields; extra image
    rows carry only Handle + Image Src + Image Position)."""
    handle = slugify(url)
    images = info["image_urls"]
    rows = []

    category_name = CATEGORY_MAP.get(handle, "")
    product_type = category_name or info["category"]
    category_col = category_name or info["breadcrumb"]

    price_num, currency = parse_price(info["base_price"])

    for i, variant in enumerate(variants):
        v_price_num, v_currency = parse_price(variant.get("price") or info["base_price"])
        row = {k: "" for k in CSV_FIELDS}

        if i == 0:
            row.update({
                "Handle": handle,
                "Title": info["title"] or "",
                "Body (HTML)": info["description"] or "",
                "Vendor": info["brand"] or "",
                "Type": product_type,
                "Published": "TRUE",
                "Gift Card": "FALSE",
                "Status": "active",
                "Google Shopping / Condition": "new",
                "SEO Title": info["title"] or "",
                "Category": category_col,
                "Product URL": url,
            })

        row["Option1 Name"] = info["option_name"] or ""
        row["Option1 Value"] = variant.get("option_value") or ""
        row["Option2 Name"] = info.get("option2_name") or ""
        row["Option2 Value"] = variant.get("option2_value") or ""
        sku = variant.get("sku") or info["base_sku"] or ""
        row["Variant SKU"] = sku
        row["Variant Grams"] = "0"
        row["Variant Inventory Tracker"] = "shopify"
        row["Variant Inventory Qty"] = variant.get("quantity") or "0"
        row["Variant Inventory Policy"] = "continue"
        row["Variant Fulfillment Service"] = "manual"
        row["Variant Price"] = v_price_num or price_num
        row["Variant Requires Shipping"] = "TRUE"
        row["Variant Taxable"] = "TRUE"
        row["MPN"] = sku
        row["Google Shopping / MPN"] = sku
        row["Currency"] = v_currency or currency or "USD"
        row["Availability"] = variant.get("availability") or info["base_availability"] or ""

        if i < len(images):
            row["Image Src"] = images[i]
            row["Image Position"] = str(i + 1)
            row["Image Alt Text"] = info["title"] or ""

        variant_sku_upper = sku.upper()
        for img in images:
            filename = img.rsplit("/", 1)[-1].upper()
            if variant_sku_upper and filename.startswith(variant_sku_upper):
                row["Variant Image"] = img
                break

        rows.append(row)

    for j in range(len(variants), len(images)):
        row = {k: "" for k in CSV_FIELDS}
        row["Handle"] = handle
        row["Image Src"] = images[j]
        row["Image Position"] = str(j + 1)
        row["Image Alt Text"] = info["title"] or ""
        rows.append(row)

    return rows


def load_existing_urls():
    try:
        with open(OUTPUT_CSV, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            return {row["Product URL"] for row in reader if row.get("Product URL")}
    except FileNotFoundError:
        return set()


def ensure_csv_header():
    try:
        with open(OUTPUT_CSV, "r", encoding="utf-8-sig") as f:
            has_content = bool(f.readline())
    except FileNotFoundError:
        has_content = False
    if not has_content:
        with open(OUTPUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_FIELDS).writeheader()


def write_rows(rows):
    with csv_lock:
        with open(OUTPUT_CSV, "a", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writerows(rows)


def scrape_one(url):
    try:
        session = get_session()
    except requests.RequestException as e:
        print(f"  [session-failed] {url}: {e}")
        return False

    html = fetch(url, session)
    if not html:
        print(f"  [failed] {url}")
        return False
    try:
        info = parse_product_page(html)
    except Exception as e:
        print(f"  [parse-error] {url}: {e}")
        return False

    try:
        variants = collect_variants(session, url, info)
    except Exception as e:
        print(f"  [variant-error] {url}: {e}")
        return False

    time.sleep(0.2 + random.random() * 0.3)
    try:
        rows = build_rows(url, info, variants)
        write_rows(rows)
    except Exception as e:
        print(f"  [write-error] {url}: {e}")
        return False
    return True


def main():
    global CATEGORY_MAP
    print("Mapping products to categories from the menu category pages...")
    CATEGORY_MAP = discover_category_map()
    print(f"Category map built: {len(CATEGORY_MAP)} products tagged.")

    print("Discovering product URLs from the Samco-Sport brand catalog...")
    all_urls = [u for u in discover_product_urls() if u != BRAND_LISTING]
    print(f"Discovered {len(all_urls)} unique product URLs.")

    ensure_csv_header()
    done = load_existing_urls()
    todo = [u for u in all_urls if u not in done]
    print(f"Already scraped: {len(done)}. Remaining: {len(todo)}.")

    if not todo:
        print("Nothing left to scrape.")
        return

    completed = 0
    ok_count = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(scrape_one, u): u for u in todo}
        for fut in as_completed(futures):
            completed += 1
            if fut.result():
                ok_count += 1
            if completed % 25 == 0 or completed == len(todo):
                print(f"  progress: {completed}/{len(todo)} ({ok_count} ok)")

    print(f"Done. Scraped {ok_count}/{len(todo)} new products this run.")
    print(f"Output: {OUTPUT_CSV}")


if __name__ == "__main__":
    sys.exit(main())
