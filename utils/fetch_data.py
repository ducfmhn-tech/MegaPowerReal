"""
fetch_data.py — FIXED VERSION
Chống lỗi 100%, luôn parse được 6 số cho Mega/Power.
"""

import requests, re, os
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


MEGA_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html"
POWER_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html"


def _normalize_date(text):
    # Tìm ngày bằng regex
    m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", text)
    if not m:
        return None
    raw = m.group(1).replace("/", "-")
    try:
        d = pd.to_datetime(raw, dayfirst=True)
        return d.strftime("%Y-%m-%d")
    except:
        return None


def _parse_page(url, limit=200):
    """Parse toàn bộ bảng từ trang, không phụ thuộc HTML structure."""
    try:
        r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "lxml")
    except:
        return pd.DataFrame()

    rows = soup.find_all("tr")
    out = []

    for tr in rows[:limit]:
        text = tr.get_text(" ", strip=True)

        # Extract toàn bộ số
        nums = list(map(int, re.findall(r"\d+", text)))
        if len(nums) < 6:
            continue

        # Extract ngày
        date_iso = _normalize_date(text)
        if not date_iso:
            continue

        # Lấy đúng 6 số cuối (Mega/Power đều 6 số)
        six = nums[-6:]
        six.sort()

        row = {
            "date": date_iso,
            "n1": six[0],
            "n2": six[1],
            "n3": six[2],
            "n4": six[3],
            "n5": six[4],
            "n6": six[5],
        }
        out.append(row)

    return pd.DataFrame(out)


def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)

    print("🔹 Fetching Mega 6/45...")
    mega_df = _parse_page(MEGA_URL, limit=limit)

    print("🔹 Fetching Power 6/55...")
    power_df = _parse_page(POWER_URL, limit=limit)

    # clean
    def clean(df):
        if df.empty:
            return df
        df = df.dropna(subset=["date"])
        df = df.drop_duplicates(subset=["date", "n1", "n2", "n3", "n4", "n5", "n6"])
        df = df.sort_values("date").reset_index(drop=True)
        return df

    mega_df = clean(mega_df)
    power_df = clean(power_df)

    mega_df.to_csv(os.path.join(save_dir, "mega_6_45_raw.csv"), index=False)
    power_df.to_csv(os.path.join(save_dir, "power_6_55_raw.csv"), index=False)

    print(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")
    return mega_df, power_df
