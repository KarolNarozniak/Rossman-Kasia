import requests
from bs4 import BeautifulSoup
import csv
import time
import datetime
import pandas as pd

# --- Configuration ---
BASE_URL = "https://www.rossmann.pl/kategoria/zwierzeta,19118"
PARAMS = {
    "Discounts": ["mega", "promotion"],
    "Order": "nameAsc",
}
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/137.0.0.0 Safari/537.36"
    )
}

def fetch_page(page_number):
    """Fetch a single page’s HTML, raising on errors."""
    params = PARAMS.copy()
    params["Page"] = page_number
    r = requests.get(BASE_URL, params=params, headers=HEADERS)
    r.raise_for_status()
    return r.text

def parse_products(html):
    """Yield product dicts, skipping tiles missing the cart button."""
    soup = BeautifulSoup(html, "html.parser")
    for tile in soup.find_all("div", {"data-testid": "products-list-product"}):
        btn = tile.find("button", {"data-testid": "add-to-basket-btn"})
        if not btn:
            continue

        # Pull brand from the data-brand attribute (clean, consistent)
        brand        = btn.get("data-brand", "").strip()
        regular      = btn.get("data-regular-price", "").strip()
        promo        = btn.get("data-promo-price", "").strip()

        # Other fields
        caption = tile.find("div", {"data-testid": "products-list-product-caption"})
        caption = caption.get_text(strip=True) if caption else ""

        unit = tile.find("div", {"data-testid": "products-list-product-unit"})
        unit = unit.get_text(strip=True) if unit else ""

        price_el = tile.find("span", {"data-testid": "products-list-product-price"})
        price    = price_el.get_text(strip=True) if price_el else ""

        per_el   = tile.find("span", {"data-testid": "products-list-product-price-per-unit"})
        per_u    = per_el.get_text(strip=True) if per_el else ""

        link_el = tile.select_one("a[href*='/Produkt/']")
        link    = "https://www.rossmann.pl" + link_el["href"] if link_el else ""

        img_el  = tile.find("img", src=True)
        img     = img_el["src"] if img_el else ""

        badge_el = tile.select_one(".styles-module_badge--ptqlS span")
        badge    = badge_el.get_text(strip=True) if badge_el else ""

        avail_elems = tile.find_all("div", class_="AvailabilitySection-module_text--zxru7")
        availability = "; ".join(e.get_text(strip=True) for e in avail_elems)

        yield {
            "sku": btn.get("data-sku", "").strip(),
            "brand": brand,
            "caption": caption,
            "unit": unit,
            "badge": badge,
            "link": link,
            "image_url": img,
            "price": price,
            "per_unit": per_u,
            "regular_price": regular,
            "promo_price": promo,
            "availability": availability,
        }

def scrape_raw(pages=10, delay=2.0):
    """Scrape and save raw data; return the raw filename."""
    ts     = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    raw_fn = f"rossmann_products_{ts}.csv"

    with open(raw_fn, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "sku","brand","caption","unit","badge",
            "link","image_url","price","per_unit",
            "regular_price","promo_price","availability"
        ])
        writer.writeheader()

        for p in range(1, pages + 1):
            html = fetch_page(p)
            for prod in parse_products(html):
                writer.writerow(prod)
            time.sleep(delay)

    return raw_fn

def trim_data(raw_fn):
    """Trim duplicates & extra columns; return the trimmed filename."""
    ts      = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    trim_fn = f"rossmann_trimmed_{ts}.csv"

    df = pd.read_csv(raw_fn, dtype={"sku": str})

    # 1) Drop exact SKU duplicates
    df = df.drop_duplicates(subset=["sku"])

    # 2) Drop by same brand + regular + promo (keeps first)
    df = df.drop_duplicates(subset=["brand", "regular_price", "promo_price"])

    # 3) Build the three columns, ensuring exactly one space between brand & caption
    df["BTS"]   = df["brand"] + " " + df["caption"] + ", " + df["unit"]
    df["Cena"]  = df["regular_price"].astype(float).map(
        lambda x: f"{x:.2f}".replace(".", ",") + " zł"
    )
    df["Promo"] = df["promo_price"].astype(float).map(
        lambda x: f"{x:.2f}".replace(".", ",") + " zł"
    )

    # 4) Keep only BTS, Cena, Promo
    out = df[["BTS", "Cena", "Promo"]]
    out.to_csv(trim_fn, index=False)
    return trim_fn

if __name__ == "__main__":
    raw_file     = scrape_raw(pages=10, delay=2.0)
    trimmed_file = trim_data(raw_file)
    print(f"Saved trimmed file: {trimmed_file}")
