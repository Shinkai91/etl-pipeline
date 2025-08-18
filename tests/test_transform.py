import pandas as pd
import pytest
from utils.transform import transform

@pytest.fixture
def sample_raw_data():
    return [
        {"Title": "Cool Jacket", "Price": "$10.00", "Rating": "4.8 / 5", "Colors": "3 Colors", "Size": "Size: M", "Gender": "Gender: Men", "Timestamp": "2025-08-14 12:00:00"},
        {"Title": "Shiny Shoes", "Price": "$12.50", "Rating": "4.2", "Colors": "1 Color", "Size": "S", "Gender": "Women", "Timestamp": "2025-08-14 12:00:01"},
        {"Title": "Unknown Product", "Price": "$15.00", "Rating": "5/5", "Colors": "2 Colors", "Size": "L", "Gender": "Unisex", "Timestamp": "2025-08-14 12:00:02"},
        {"Title": "Bad Hat", "Price": "$5.00", "Rating": "Invalid Rating", "Colors": "5", "Size": "M", "Gender": "Men", "Timestamp": "2025-08-14 12:00:03"},
        {"Title": "Null Price Shoe", "Price": None, "Rating": "3.0/5", "Colors": "1", "Size": "M", "Gender": "Men", "Timestamp": "2025-08-14 12:00:04"}
    ]

def test_transform_cleaning(sample_raw_data):
    df = transform(sample_raw_data)
    assert len(df) == 2
    assert not df.isnull().values.any()

def test_transform_types(sample_raw_data):
    df = transform(sample_raw_data)
    assert pd.api.types.is_integer_dtype(df["Price"])
    assert pd.api.types.is_float_dtype(df["Rating"])
    assert pd.api.types.is_integer_dtype(df["Colors"])

def test_transform_values(sample_raw_data):
    df = transform(sample_raw_data)
    first_row = df.iloc[0]
    assert first_row["Price"] == 160000
    assert first_row["Rating"] == 4.8
    assert first_row["Colors"] == 3
    assert first_row["Size"] == "M"
    assert first_row["Gender"] == "Men"

def test_transform_empty_input():
    df = transform([])
    assert df.empty

def test_transform_missing_required_column():
    data_missing_price = [{"Title": "Cool Jacket", "Rating": "4.8 / 5"}]
    with pytest.raises(KeyError, match="Kolom wajib 'Price' tidak ditemukan"):
        transform(data_missing_price)