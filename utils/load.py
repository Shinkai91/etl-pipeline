from __future__ import annotations

import logging
import os
from typing import Optional

import pandas as pd

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


def load_to_csv(df: pd.DataFrame, filename: str = "products.csv", directory: Optional[str] = None) -> str:
    """
    Simpan DataFrame ke CSV. Mengembalikan path file.
    """
    if df is None or df.empty:
        raise ValueError("DataFrame kosong. Pastikan tahap extract & transform menghasilkan data.")

    path = os.path.join(directory, filename) if directory else filename

    try:
        os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
        df.to_csv(path, index=False, encoding="utf-8")
        logging.info(f"[CSV] Data berhasil disimpan ke {path}")
        return path
    except PermissionError as e:
        logging.error(f"Izin ditolak saat menyimpan ke '{path}': {e}")
        raise
    except OSError as e:
        logging.error(f"Gagal menulis file '{path}': {e}")
        raise


def load_to_gsheets(df: pd.DataFrame, spreadsheet_id: str) -> None:
    """
    Simpan DataFrame ke Google Sheets yang sudah ada berdasarkan ID-nya.
    """
    if df is None or df.empty:
        raise ValueError("DataFrame kosong, tidak bisa disimpan ke Google Sheets.")

    scope = ["https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("google-sheets-api.json", scope)
    client = gspread.authorize(creds)

    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        sheet = spreadsheet.sheet1
        
        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        
        logging.info(f"[Google Sheets] Data berhasil disimpan di spreadsheet: {spreadsheet.title}")

    except gspread.SpreadsheetNotFound:
        logging.error(f"Spreadsheet dengan ID '{spreadsheet_id}' tidak ditemukan.")
        raise
    except Exception as e:
        logging.error(f"Terjadi error saat mengakses Google Sheets: {e}")
        raise


def load_to_postgres(df: pd.DataFrame, db_config: dict, table_name: str = "etl_data") -> None:
    """
    Simpan DataFrame ke PostgreSQL.
    db_config contoh:
    {
        "host": "localhost",
        "database": "etl_db",
        "user": "postgres",
        "password": "yourpassword",
        "port": 5432
    }
    """
    if df is None or df.empty:
        raise ValueError("DataFrame kosong. Tidak bisa disimpan ke PostgreSQL.")

    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            Title TEXT,
            Price BIGINT,
            Rating FLOAT,
            Colors INT,
            Size TEXT,
            Gender TEXT,
            Timestamp TEXT
        )
        """
        cur.execute(create_table_query)

        for _, row in df.iterrows():
            cur.execute(
                f"INSERT INTO {table_name} (Title, Price, Rating, Colors, Size, Gender, Timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (row["Title"], int(row["Price"]), float(row["Rating"]), int(row["Colors"]),
                row["Size"], row["Gender"], row["Timestamp"])
            )

        conn.commit()
        cur.close()
        logging.info(f"[PostgreSQL] Data berhasil disimpan di tabel {table_name}")

    except Exception as e:
        logging.error(f"Gagal menyimpan data ke PostgreSQL: {e}")
        raise
    finally:
        if conn:
            conn.close()


__all__ = ["load_to_csv", "load_to_gsheets", "load_to_postgres"]