# Python script to scrape Rossmann animal category page
# Retrieves product name, regular price, and promotional price.
# Uses requests and BeautifulSoup to parse the HTML content.
# Saves unique products in a pandas DataFrame and prints it.

import requests
from bs4 import BeautifulSoup
import pandas as pd

URL = "https://www.rossmann.pl/kategoria/zwierzeta,19118?Discounts=mega&Discounts=promotion&Order=nameAsc&Page=1"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) "
        "Gecko/20100101 Firefox/117.0"
    )
}

def scrape_page(url: str) -> pd.DataFrame:
    """Fetch `url` and parse product information into a DataFrame."""
    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    products = []
    seen_names = set()

    # Attempt to select product containers; adjust selectors as needed
    for item in soup.select("div.product-tile"):
        name_elem = item.select_one("a.product-tile__name")
        regular_elem = item.select_one("span.product-tile__regular-price")
        promo_elem = item.select_one("span.product-tile__promo-price")
        if not name_elem or not promo_elem:
            continue

        name = name_elem.get_text(strip=True)
        if name in seen_names:
            continue
        seen_names.add(name)
        regular_price = regular_elem.get_text(strip=True) if regular_elem else ""
        promo_price = promo_elem.get_text(strip=True)
        products.append({"BTS": name, "Cena": regular_price, "Promo": promo_price})

    return pd.DataFrame(products, columns=["BTS", "Cena", "Promo"])

def main() -> None:
    try:
        table = scrape_page(URL)
    except Exception as exc:
        print(f"Failed to fetch page: {exc}")
        return

    if table.empty:
        print("No products scraped.")
    else:
        print(table)

if __name__ == "__main__":
    main()
