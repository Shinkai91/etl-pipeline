from __future__ import annotations

import logging
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Dict

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter, Retry

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

BASE_URL = "https://fashion-studio.dicoding.dev"
TIMEOUT = 15
TOTAL_PAGES = 50


@dataclass
class ProductRaw:
    Title: str
    Price: str
    Rating: str
    Colors: str
    Size: str
    Gender: str
    Timestamp: str


def _requests_session() -> requests.Session:
    """Session dengan retry agar lebih tahan error jaringan."""
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (compatible; ETL-Pipeline/1.0; +https://example.local)"
        }
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s


def _fetch_html(url: str, session: requests.Session) -> str:
    try:
        resp = session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Gagal mengambil URL {url}: {e}")
        raise


def _find_cards(soup: BeautifulSoup) -> List[Tag]:
    """
    Mengembalikan list elemen card produk.
    Selector sengaja fleksibel karena struktur situs bisa berbeda.
    """
    candidates = [
        ".product-card",
        ".card.product",
        "article.product",
        "div.product",
        "li.product",
        "[data-testid='product-card']",
    ]
    for sel in candidates:
        cards = soup.select(sel)
        if cards:
            return cards
    cards = []
    for cont in soup.find_all(["div", "article", "li"]):
        title = cont.select_one(".product-title") or cont.find(
            ["h2", "h3", "h4"], string=True
        )
        price = cont.select_one(".product-price") or cont.find(
            ["span", "p"], string=re.compile(r"\$\s*\d")
        )
        if title and price:
            cards.append(cont)
    return cards


def _text(el: Tag | None) -> str:
    return el.get_text(strip=True) if el else ""


def _extract_with_patterns(container: Tag) -> Dict[str, str]:
    """
    Ekstraksi field dengan beberapa pola umum agar robust.
    """
    title = (
        _text(container.select_one(".product-title"))
        or _text(container.find(["h2", "h3", "h4"]))
    )

    price = (
        _text(container.select_one(".product-price"))
        or _text(container.find(string=re.compile(r"\$\s*\d")))
    )

    rating = (
        _text(container.select_one(".product-rating"))
        or _text(container.find(string=re.compile(r"\d+(\.\d+)?\s*/\s*5")))
        or _text(container.find(string=re.compile(r"^\d+(\.\d+)?$")))
    )

    colors = (
        _text(container.select_one(".product-colors"))
        or _text(container.find(string=re.compile(r"Colors?", re.I)))
    )

    size = (
        _text(container.select_one(".product-size"))
        or _text(container.find(string=re.compile(r"Size", re.I)))
    )

    gender = (
        _text(container.select_one(".product-gender"))
        or _text(container.find(string=re.compile(r"Gender", re.I)))
    )

    return {
        "Title": title,
        "Price": price,
        "Rating": rating,
        "Colors": colors,
        "Size": size,
        "Gender": gender,
    }


def scrape_page(page: int, session: requests.Session | None = None) -> List[ProductRaw]:
    """
    Scrap satu halaman. Mengembalikan list ProductRaw.
    """
    if not session:
        session = _requests_session()

    if page == 1:
        url = f"{BASE_URL}/"
    else:
        url = f"{BASE_URL}/page{page}"

    logging.info(f"Mengambil data dari URL: {url}")
    html = _fetch_html(url, session)
    soup = BeautifulSoup(html, "html.parser")

    cards = _find_cards(soup)
    results: List[ProductRaw] = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for c in cards:
        try:
            fields = _extract_with_patterns(c)
            if not fields.get("Title") or not fields.get("Price"):
                continue
            prod = ProductRaw(
                Title=fields["Title"],
                Price=fields["Price"],
                Rating=fields.get("Rating", ""),
                Colors=fields.get("Colors", ""),
                Size=fields.get("Size", ""),
                Gender=fields.get("Gender", ""),
                Timestamp=ts,
            )
            results.append(prod)
        except Exception as e:
            logging.warning(f"Gagal parsing 1 produk di page {page}: {e}")
            continue

    logging.info(f"Page {page}: {len(results)} produk terambil")
    return results


def extract_all(pages: int = TOTAL_PAGES) -> List[Dict[str, str]]:
    """
    Scrap semua halaman 1..pages dan kembalikan list of dict.
    """
    session = _requests_session()
    all_items: List[Dict[str, str]] = []
    for p in range(1, pages + 1):
        try:
            items = scrape_page(p, session)
            all_items.extend([asdict(i) for i in items])
        except Exception as e:
            logging.error(f"Lewati page {p} karena error: {e}")
            continue
    logging.info(f"Total produk terambil: {len(all_items)}")
    return all_items


__all__ = ["extract_all", "scrape_page", "ProductRaw"]