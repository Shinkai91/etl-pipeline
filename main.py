import os
from dotenv import load_dotenv
from utils.extract import extract_all
from utils.transform import transform
from utils.load import load_to_csv, load_to_gsheets, load_to_postgres

load_dotenv()

if __name__ == "__main__":
    raw_data = extract_all()
    clean_df = transform(raw_data)
    
    load_to_csv(clean_df)
    
    SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
    
    if not SPREADSHEET_ID:
        raise ValueError("Error: SPREADSHEET_ID tidak ditemukan. Pastikan file .env sudah benar.")
        
    load_to_gsheets(clean_df, spreadsheet_id=SPREADSHEET_ID)

    db_config = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_DATABASE"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": int(os.getenv("DB_PORT", 5432))
    }
    load_to_postgres(clean_df, db_config)

    print("Proses ETL selesai dengan sukses!")