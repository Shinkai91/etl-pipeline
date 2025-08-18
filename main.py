from utils.extract import extract_all
from utils.transform import transform
from utils.load import load_to_csv, load_to_gsheets, load_to_postgres

if __name__ == "__main__":
    raw_data = extract_all()
    clean_df = transform(raw_data)
    
    load_to_csv(clean_df)
    
    SPREADSHEET_ID = "1PPEBq2-iIWAHJbsCh5G9JFpZohoMgq7ENq0F-PnFcv4" 
    load_to_gsheets(clean_df, spreadsheet_id=SPREADSHEET_ID)

    db_config = {
        "host": "localhost",
        "database": "etl_db",
        "user": "postgres",
        "password": "admin",
        "port": 5432
    }
    load_to_postgres(clean_df, db_config)