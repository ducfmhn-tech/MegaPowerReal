import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def parse_table_rows(soup, game_type="mega"):
    """
    Parse bảng HTML Mega/Power thành list of dict.
    game_type: "mega" hoặc "power"
    """
    rows_data = []
    if game_type == "mega":
        table = soup.find("table", {"id": "mega_table"})  # Thay id cho đúng website
    else:
        table = soup.find("table", {"id": "power_table"})  # Thay id cho đúng website

    if not table:
        return rows_data

    for tr in table.find_all("tr")[1:]:  # bỏ header
        cols = tr.find_all("td")
        if len(cols) >= 2:
            row = {
                "date": cols[0].get_text(strip=True),
                "numbers": cols[1].get_text(strip=True).split()
            }
            rows_data.append(row)
    return rows_data

def fetch_game(url_base, limit=100, game_type="mega"):
    """
    Fetch nhiều trang để đủ 'limit' dòng
    """
    all_rows = []
    page = 1
    while len(all_rows) < limit:
        url = f"{url_base}?page={page}"
        print(f"🔹 Fetching {game_type} page {page} ...")
        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                print(f"⚠ Không thể fetch page {page}, status={r.status_code}")
                break
            soup = BeautifulSoup(r.text, "html.parser")
            rows = parse_table_rows(soup, game_type)
            if not rows:
                break
            all_rows.extend(rows)
            page += 1
            time.sleep(1)  # tránh spam request
        except Exception as e:
            print(f"❌ Lỗi fetch page {page}: {e}")
            break
    return all_rows[:limit]

def fetch_all_data(limit=100):
    """
    Trả về (mega_data, power_data)
    """
    mega_urls = [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
        "https://www.lotto-8.com/Vietnam/listltoVM45.asp"
    ]
    power_urls = [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html",
        "https://www.lotto-8.com/Vietnam/listltoVM55.asp"
    ]

    mega_data = []
    for url in mega_urls:
        if len(mega_data) >= limit:
            break
        rows = fetch_game(url, limit=limit - len(mega_data), game_type="mega")
        mega_data.extend(rows)
        print(f"✔ Fetched {len(rows)} rows from {url}")

    power_data = []
    for url in power_urls:
        if len(power_data) >= limit:
            break
        rows = fetch_game(url, limit=limit - len(power_data), game_type="power")
        power_data.extend(rows)
        print(f"✔ Fetched {len(rows)} rows from {url}")

    return mega_data[:limit], power_data[:limit]
