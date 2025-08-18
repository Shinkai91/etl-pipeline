import pytest
import requests
from utils.extract import extract_all, _fetch_html, scrape_page

def test_extract_all_structure(requests_mock):
    """Tes struktur dasar output dari extract_all."""
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
    requests_mock.get("https://fashion-studio.dicoding.dev/", text=mock_html)
    data = extract_all(pages=1)
    
    assert isinstance(data, list)
    assert len(data) == 2
    assert data[0]["Title"] == "Test Product 1"

def test_extract_handles_network_error(requests_mock):
    """Tes bahwa _fetch_html memunculkan error HTTP dengan benar."""
    url = "https://fashion-studio.dicoding.dev/"
    requests_mock.get(url, status_code=500)
    with pytest.raises(requests.exceptions.HTTPError):
        _fetch_html(url, requests.Session())

def test_scrape_page_handles_malformed_card(requests_mock):
    """Tes bahwa produk tanpa judul atau harga akan dilewati."""
    mock_html = """
    <html><body>
        <div class="product-card"><h3 class="product-title">Good Product</h3><span class="product-price">$99.99</span></div>
        <div class="product-card"><span class="product-price">$50.00</span></div>
        <div class="product-card"><h3 class="product-title">Product No Price</h3></div>
    </body></html>
    """
    requests_mock.get("https://fashion-studio.dicoding.dev/", text=mock_html)
    
    data = scrape_page(page=1)
    
    assert len(data) == 1
    assert data[0].Title == "Good Product"

def test_extract_all_50_pages_successfully(requests_mock):
    """Memastikan fungsi bisa mengambil data dari 50 halaman dengan benar."""
    TOTAL_TEST_PAGES = 50
    
    for page_num in range(1, TOTAL_TEST_PAGES + 1):
        mock_html = f'<html><body><div class="product-card"><h3 class="product-title">Produk Hal {page_num}</h3><span class="product-price">${page_num}</span></div></body></html>'
        
        if page_num == 1:
            url = "https://fashion-studio.dicoding.dev/"
        else:
            url = f"https://fashion-studio.dicoding.dev/page{page_num}"
            
        requests_mock.get(url, text=mock_html)
        
    data = extract_all(pages=TOTAL_TEST_PAGES)
    
    assert len(data) == TOTAL_TEST_PAGES
    assert data[0]["Title"] == "Produk Hal 1"
    assert data[49]["Title"] == "Produk Hal 50"

def test_extract_all_skips_multiple_failed_pages(requests_mock):
    """Memastikan fungsi bisa melewati beberapa halaman yang gagal dari total 50 halaman."""
    TOTAL_TEST_PAGES = 50
    failed_pages = {10, 25, 40}
    
    for page_num in range(1, TOTAL_TEST_PAGES + 1):
        if page_num == 1:
            url = "https://fashion-studio.dicoding.dev/"
        else:
            url = f"https://fashion-studio.dicoding.dev/page{page_num}"
            
        if page_num in failed_pages:
            requests_mock.get(url, status_code=503)
        else:
            mock_html = f'<html><body><div class="product-card"><h3 class="product-title">Produk Hal {page_num}</h3><span class="product-price">${page_num}</span></div></body></html>'
            requests_mock.get(url, text=mock_html)
            
    data = extract_all(pages=TOTAL_TEST_PAGES)
    
    assert len(data) == TOTAL_TEST_PAGES - len(failed_pages)
    
    titles = {item['Title'] for item in data}
    assert "Produk Hal 10" not in titles
    assert "Produk Hal 25" not in titles
    assert "Produk Hal 40" not in titles

def test_find_cards_with_fallback_selector(requests_mock):
    """Tes logika fallback selector di _find_cards jika .product-card tidak ada."""
    mock_html = """
    <html><body>
        <article class="product">
            <h2 class="product-title">Fallback Product</h2>
            <span class="product-price">$25</span>
        </article>
    </body></html>
    """
    requests_mock.get("https://fashion-studio.dicoding.dev/", text=mock_html)
    data = scrape_page(page=1)
    
    assert len(data) == 1
    assert data[0].Title == "Fallback Product"

def test_scrape_page_with_no_products(requests_mock):
    """Tes saat halaman tidak mengandung produk sama sekali."""
    mock_html = "<html><body><h1>Tidak ada produk ditemukan</h1></body></html>"
    requests_mock.get("https://fashion-studio.dicoding.dev/", text=mock_html)
    
    data = scrape_page(page=1)
    
    assert isinstance(data, list)
    assert len(data) == 0

def test_scrape_page_creates_own_session(requests_mock):
    """Tes bahwa scrape_page membuat session sendiri jika tidak disediakan."""
    mock_html = '<html><body><div class="product-card"><h3>P1</h3><span>$10</span></div></body></html>'
    requests_mock.get("https://fashion-studio.dicoding.dev/", text=mock_html)
    
    data = scrape_page(page=1, session=None)
    
    assert len(data) == 1
    assert data[0].Title == "P1"