# Proyek ETL Simple: Fashion Product Scraper

Pipeline ETL (Extract, Transform, Load) sederhana dengan Python untuk scraping data produk fashion dari [fashion-studio.dicoding.dev](https://fashion-studio.dicoding.dev).

## Fitur Utama

- **Extract**: Scraping data produk fashion dari website.
- **Transform**: Membersihkan, memvalidasi, dan mengubah format data (misal konversi harga USD ke IDR).
- **Load**: Memuat data ke file CSV, Google Sheets, dan database PostgreSQL.
- **Testing**: Unit test untuk memastikan kualitas dan keandalan kode.

---

## Library yang Digunakan

### Library Utama

- `pandas`: Manipulasi dan transformasi data
- `requests`, `beautifulsoup4`: Scraping data web
- `psycopg2-binary`: Interaksi dengan PostgreSQL
- `gspread`, `oauth2client`: Otentikasi dan load ke Google Sheets

### Library untuk Testing

- `pytest`: Framework unit test
- `pytest-cov`: Mengukur cakupan tes
- `pytest-mock`, `requests-mock`: Mocking dependensi eksternal (API/database)

---

## Cara Menjalankan Proyek

### 1. Persiapan Awal

- Pastikan Python 3.8+ dan PostgreSQL sudah terinstal.
- Clone repositori ke mesin lokal.
- Buat dan aktifkan virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # Linux/macOS
venv\Scripts\activate     # Windows
```

### 2. Instalasi

Instal semua library yang dibutuhkan:

```bash
pip install -r requirements.txt
```

### 3. Konfigurasi

#### Google Sheets

- Buat file kredensial `google-sheets-api.json` dari Google Cloud Console, letakkan di root proyek.
- Buat Google Sheet kosong, bagikan ke email `client_email` dari file `.json` sebagai Editor.
- Salin ID Spreadsheet dari URL dan masukkan ke variabel `SPREADSHEET_ID` di `main.py`.

#### PostgreSQL

- Buat database baru (misal: `etl_db`).
- Sesuaikan konfigurasi database (terutama password) di `main.py`.

### 4. Menjalankan Pipeline ETL

Jalankan proses ETL dari root direktori:

```bash
python main.py
```

Data akan dimuat ke `products.csv`, Google Sheets, dan tabel PostgreSQL.

### 5. Menjalankan Unit Test

Validasi kode dan lihat test coverage:

```bash
pytest --cov=utils
```

Hasil pengujian dan laporan cakupan akan ditampilkan di terminal.

---

## Struktur Direktori

```
.
├── main.py
├── requirements.txt
├── google-sheets-api.json
├── products.csv
├── utils/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── conftest.py
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_load.py
└── README.md
```