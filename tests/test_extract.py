import pytest
import requests
from utils.extract import extract_all, _fetch_html

def test_extract_all_structure(requests_mock):
    mock_html = """
    <html><body>
        <div class="product-card">
            <h3 class="product-title">Test Product 1</h3>
            <span class="product-price">$99.99</span>
        </div>
        <div class="product-card">
            <h3 class="product-title">Test Product 2</h3>
            <span class="product-price">$50.00</span>
        </div>
    </body></html>
    """
    requests_mock.get("https://fashion-studio.dicoding.dev/?page=1", text=mock_html)
    data = extract_all(pages=1)
    
    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0]["Title"] == "Test Product 1"

def test_extract_handles_network_error(requests_mock):
    url = "https://fashion-studio.dicoding.dev/?page=1"
    requests_mock.get(url, status_code=500)
    with pytest.raises(requests.exceptions.HTTPError):
        _fetch_html(url, requests.Session())

def test_scrape_page_handles_malformed_card(requests_mock):
    mock_html = """
    <html><body>
        <div class="product-card"><h3 class="product-title">Good Product</h3><span class="product-price">$99.99</span></div>
        <div class="product-card"><span class="product-price">$50.00</span></div>
    </body></html>
    """
    requests_mock.get("https://fashion-studio.dicoding.dev/?page=1", text=mock_html)
    data = extract_all(pages=1)
    assert len(data) == 1
    assert data[0]["Title"] == "Good Product"

def test_extract_all_skips_failed_page(requests_mock):
    mock_html_page_1 = '<html><body><div class="product-card"><h3 class="product-title">P1</h3><span class="product-price">$10</span></div></body></html>'
    requests_mock.get("https://fashion-studio.dicoding.dev/?page=1", text=mock_html_page_1)
    requests_mock.get("https://fashion-studio.dicoding.dev/?page=2", status_code=500)
    data = extract_all(pages=2)
    assert len(data) == 1