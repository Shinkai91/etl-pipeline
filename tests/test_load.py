import os
import pandas as pd
import pytest
import gspread
from utils.load import load_to_csv, load_to_gsheets, load_to_postgres

@pytest.fixture
def sample_df():
    return pd.DataFrame([{"Title": "Jacket", "Price": 160000, "Rating": 4.8, "Colors": 3, "Size": "M", "Gender": "Men", "Timestamp": "2025-08-14 12:00:00"}])

def test_load_to_csv_creates_file(sample_df, tmp_path):
    path = load_to_csv(sample_df, "test.csv", tmp_path)
    assert os.path.exists(path)

def test_load_to_csv_empty_df():
    with pytest.raises(ValueError): load_to_csv(pd.DataFrame())

def test_load_to_csv_permission_error(sample_df, mocker):
    mocker.patch("builtins.open", side_effect=PermissionError)
    with pytest.raises(PermissionError): load_to_csv(sample_df, "no.csv")

def test_load_to_gsheets_success(sample_df, mocker):
    mock_client = mocker.Mock()
    mocker.patch("gspread.authorize", return_value=mock_client)
    mocker.patch("oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_name")
    load_to_gsheets(sample_df, spreadsheet_id="test_id")
    mock_client.open_by_key.assert_called_once_with("test_id")
    mock_client.open_by_key.return_value.sheet1.update.assert_called_once()

def test_load_to_gsheets_empty_df():
    with pytest.raises(ValueError): load_to_gsheets(pd.DataFrame(), "any")

def test_load_to_gsheets_spreadsheet_not_found(sample_df, mocker):
    mock_client = mocker.Mock()
    mock_client.open_by_key.side_effect = gspread.SpreadsheetNotFound
    mocker.patch("gspread.authorize", return_value=mock_client)
    mocker.patch("oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_name")
    with pytest.raises(gspread.SpreadsheetNotFound):
        load_to_gsheets(sample_df, "not_found_id")

def test_load_to_postgres_success(sample_df, mocker):
    mock_connect = mocker.patch("psycopg2.connect")
    db_config = {"host": "localhost"}
    load_to_postgres(sample_df, db_config)
    mock_connect.assert_called_once_with(**db_config)
    mock_connect.return_value.commit.assert_called_once()

def test_load_to_postgres_empty_df():
    with pytest.raises(ValueError): load_to_postgres(pd.DataFrame(), {})

def test_load_to_postgres_connection_error(sample_df, mocker):
    mocker.patch("psycopg2.connect", side_effect=Exception("Connection Failed"))
    with pytest.raises(Exception, match="Connection Failed"):
        load_to_postgres(sample_df, {})