from __future__ import annotations

import logging
import re
from typing import List, Dict

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

RUPIAH_RATE = 16000
INVALID_TITLE = "Unknown Product"
INVALID_RATING_TEXT = "Invalid Rating"


def _to_float_from_text(text: str) -> float | None:
    """
    Ambil angka float pertama dari string (misal "4.8 / 5" -> 4.8).
    """
    if not isinstance(text, str):
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def _to_int_from_text(text: str) -> int | None:
    """
    Ambil angka integer pertama dari string (misal "3 Colors" -> 3).
    """
    if not isinstance(text, str):
        return None
    m = re.search(r"(\d+)", text)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def _clean_prefix(text: str, prefix: str) -> str:
    """
    Hapus prefix tertentu (case-insensitive), contoh: "Size: M" -> "M".
    """
    if not isinstance(text, str):
        return ""
    return re.sub(rf"(?i)^{re.escape(prefix)}\s*", "", text).strip()


def transform(rows: List[Dict[str, str]]) -> pd.DataFrame:
    """
    Membersihkan dan mengonversi data sesuai rubric.
    - Price (USD string) -> Rupiah (int) * 16.000
    - Rating -> float
    - Colors -> int
    - Size -> string tanpa "Size: "
    - Gender -> string tanpa "Gender: "
    - Hapus null, duplikat, invalid
    """
    if not rows:
        logging.warning("Tidak ada data untuk ditransformasi.")
        return pd.DataFrame(
            columns=["Title", "Price", "Rating", "Colors", "Size", "Gender", "Timestamp"]
        )

    df = pd.DataFrame(rows)

    required_cols = ["Title", "Price", "Rating", "Colors", "Size", "Gender", "Timestamp"]
    for col in required_cols:
        if col not in df.columns:
            raise KeyError(f"Kolom wajib '{col}' tidak ditemukan pada data hasil extract.")

    df.dropna(subset=["Title", "Price"], inplace=True)

    df = df[df["Title"].astype(str).str.strip().ne(INVALID_TITLE)]
    df = df[df["Rating"].astype(str).str.strip().ne(INVALID_RATING_TEXT)]

    def _usd_to_idr(text: str) -> int | None:
        val = _to_float_from_text(text)
        if val is None:
            return None
        return int(round(val * RUPIAH_RATE))

    df["Price"] = df["Price"].apply(_usd_to_idr)

    df["Rating"] = df["Rating"].apply(_to_float_from_text)

    df["Colors"] = df["Colors"].apply(_to_int_from_text)

    df["Size"] = df["Size"].astype(str).apply(lambda x: _clean_prefix(x, "Size:"))
    df["Gender"] = df["Gender"].astype(str).apply(lambda x: _clean_prefix(x, "Gender:"))

    df.dropna(subset=["Title", "Price", "Rating", "Colors", "Size", "Gender", "Timestamp"], inplace=True)

    df = df.astype(
        {
            "Title": "string",
            "Price": "int64",
            "Rating": "float64",
            "Colors": "int64",
            "Size": "string",
            "Gender": "string",
            "Timestamp": "string",
        }
    )

    df.drop_duplicates(
        subset=["Title", "Price", "Rating", "Colors", "Size", "Gender"], inplace=True
    )

    logging.info(f"Transform: hasil {len(df)} baris setelah pembersihan.")
    return df.reset_index(drop=True)


__all__ = ["transform"]