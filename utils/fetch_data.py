"""
fetch_data.py
- Fetch Mega 6/45 & Power 6/55 results from ketquadientoan.com
- Clean & normalize data
- Save to CSV (data/mega_6_45_raw.csv, data/power_6_55_raw.csv)
"""

import os
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

MEGA_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html"
POWER_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html"


# -----------------------------
# Utility: Parse date safely
# -----------------------------
def _normalize_date(text):
    text = text.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    try:
        return pd.to_datetime(text, dayfirst=True).strftime("%Y-%m-%d")
    except Exception:
        return None


# -----------------------------
# Parse HTML table for one game
# -----------------------------
def _parse_ketquadientoan(html, limit=200):
    soup = BeautifulSoup(html, "lxml")
    out = []

    # Lấy tất cả các hàng có ngày + dãy số (2 cột)
    for tr in soup.select("table tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        date_text = tds[0].get_text(strip=True)
        nums_text = tds[1].get_text(" ", strip=True)
        nums = re.findall(r"\b\d{1,2}\b", nums_text)
        if len(nums) < 6:
            continue

        nums = list(map(int, nums[:6]))
        nums.sort()
        date_iso = _normalize_date(date_text)
        if not date_iso:
            continue

        row = {"date": date_iso}
        for i, n in enumerate(nums, 1):
            row[f"n{i}"] = n
        out.append(row)

    df = pd.DataFrame(out)
    df = df.drop_duplicates(subset=["date"]).reset_index(drop=True)
    return df.head(limit)


# -----------------------------
# Fetch from URL
# -----------------------------
def _fetch_url(url, limit=200):
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        r.encoding = r.apparent_encoding
        return _parse_ketquadientoan(r.text, limit=limit)
    except Exception as e:
        print(f"❌ fetch error {url}: {e}")
        return pd.DataFrame()


# -----------------------------
# Fetch all (Mega + Power)
# -----------------------------
def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)

    print("🔹 Fetching Mega 6/45...")
    mega_df = _fetch_url(MEGA_URL, limit=limit)
    print("🔹 Fetching Power 6/55...")
    power_df = _fetch_url(POWER_URL, limit=limit)

    # Clean up
    mega_df = mega_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    power_df = power_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # Save to CSV
    mega_path = os.path.join(save_dir, "mega_6_45_raw.csv")
    power_path = os.path.join(save_dir, "power_6_55_raw.csv")
    mega_df.to_csv(mega_path, index=False)
    power_df.to_csv(power_path, index=False)

    print(f"✅ Saved: {len(mega_df)} Mega rows, {len(power_df)} Power rows")
    return mega_df, power_df


# -----------------------------
# Manual test
# -----------------------------
if __name__ == "__main__":
    m, p = fetch_all_data(limit=20)
    print(m.tail(3))
    print(p.tail(3))
