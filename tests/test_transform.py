import pandas as pd
import pytest
from utils.transform import transform

@pytest.fixture
def sample_raw_data():
    return [
        {"Title": "Cool Jacket", "Price": "$10.00", "Rating": "4.8 / 5", "Colors": "3 Colors", "Size": "Size: M", "Gender": "Gender: Men", "Timestamp": "2025-08-14T12:00:00.123456"},
        {"Title": "Shiny Shoes", "Price": "$12.50", "Rating": "4.2", "Colors": "1 Color", "Size": "S", "Gender": "Women", "Timestamp": "2025-08-14T12:00:01.123456"},
        {"Title": "Unknown Product", "Price": "$15.00", "Rating": "5/5", "Colors": "2 Colors", "Size": "L", "Gender": "Unisex", "Timestamp": "2025-08-14T12:00:02.123456"},
        {"Title": "Bad Hat", "Price": "$5.00", "Rating": "Invalid Rating", "Colors": "5", "Size": "M", "Gender": "Men", "Timestamp": "2025-08-14T12:00:03.123456"},
        {"Title": "Null Price Shoe", "Price": None, "Rating": "3.0/5", "Colors": "1", "Size": "M", "Gender": "Men", "Timestamp": "2025-08-14T12:00:04.123456"}
    ]

def test_transform_cleaning(sample_raw_data):
    df = transform(sample_raw_data)
    assert len(df) == 2
    assert not df.isnull().values.any()

def test_transform_types(sample_raw_data):
    df = transform(sample_raw_data)
    assert pd.api.types.is_float_dtype(df["Price"])
    assert pd.api.types.is_float_dtype(df["Rating"])
    assert pd.api.types.is_integer_dtype(df["Colors"])
    assert pd.api.types.is_datetime64_any_dtype(df["Timestamp"])

def test_transform_values(sample_raw_data):
    df = transform(sample_raw_data)
    first_row = df.iloc[0]
    assert first_row["Price"] == 160000.0
    assert first_row["Rating"] == 4.8
    assert first_row["Colors"] == 3
    assert first_row["Size"] == "M"
    assert first_row["Gender"] == "Men"
    assert first_row["Timestamp"] == pd.Timestamp("2025-08-14T12:00:00.123456")

def test_transform_empty_input():
    df = transform([])
    assert df.empty

def test_transform_missing_required_column():
    data_missing_price = [{"Title": "Cool Jacket", "Rating": "4.8 / 5"}]
    with pytest.raises(KeyError, match="Kolom wajib 'Price' tidak ditemukan"):
        transform(data_missing_price)

def test_transform_edge_cases_and_duplicates():
    """
    Tes untuk menangani format data yang tidak valid, input non-string, dan data duplikat.
    Ini akan meningkatkan cakupan dengan menguji cabang-cabang error di fungsi helper.
    """
    edge_case_data = [
        {"Title": "Good Shirt", "Price": "$25.00", "Rating": "4.5 / 5", "Colors": "2 Colors", "Size": "Size: L", "Gender": "Men", "Timestamp": "2025-08-19T10:00:00"},
        {"Title": "Good Shirt", "Price": "$25.00", "Rating": "4.5 / 5", "Colors": "2 Colors", "Size": "Size: L", "Gender": "Men", "Timestamp": "2025-08-19T10:00:00"},
        {"Title": "Invalid Price Shirt", "Price": "Gratis", "Rating": "4.0/5", "Colors": "1 Color", "Size": "M", "Gender": "Unisex", "Timestamp": "2025-08-19T10:01:00"},
        {"Title": "Non-string Size", "Price": "$35.00", "Rating": "4.2/5", "Colors": "3 Colors", "Size": None, "Gender": "Women", "Timestamp": "2025-08-19T10:03:00"},
    ]
    
    df = transform(edge_case_data)
    
    assert len(df) == 2
    
    assert df["Title"].nunique() == len(df)
    assert "Good Shirt" in df["Title"].values
    assert "Non-string Size" in df["Title"].values