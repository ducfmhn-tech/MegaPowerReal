"""
fetch_data.py
- fetch 6/45 & 6/55 results from ketquadientoan.com (table parser)
- save CSV to data/
"""
import requests, re, os
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

MEGA_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html"
POWER_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html"

def _normalize_date(text):
    # try parsing dd/mm/YYYY or other formats
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d")
        except:
            continue
    # fallback via pandas
    try:
        return pd.to_datetime(text, dayfirst=True).strftime("%Y-%m-%d")
    except:
        return None

def _parse_table_html(html, limit=200):
    soup = BeautifulSoup(html, "lxml")
    # Try several selectors to be robust
    rows = soup.select("table tbody tr")
    if not rows:
        rows = soup.select("table tr")
    out = []
    for tr in rows[:limit]:
        cols = [td.get_text(strip=True) for td in tr.find_all(["td","th"])]
        if not cols:
            continue
        # find first occurrence of a date-like token and 6 numbers
        nums = re.findall(r"\d+", " ".join(cols))
        if len(nums) < 6:
            continue
        # heuristics: date token usually first
        date_token = cols[0]
        date_iso = _normalize_date(date_token) or None
        # take last 6 numbers as the draw numbers (common pattern)
        last6 = list(map(int, nums[-6:]))
        last6.sort()
        row = {"date": date_iso, "n1": last6[0], "n2": last6[1], "n3": last6[2],
               "n4": last6[3], "n5": last6[4], "n6": last6[5]}
        out.append(row)
    return pd.DataFrame(out)

def _fetch_url(url, limit=200):
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent":"Mozilla/5.0"})
        r.encoding = r.apparent_encoding
        return _parse_table_html(r.text, limit=limit)
    except Exception as e:
        print("fetch error", url, e)
        return pd.DataFrame()

def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)
    mega_df = _fetch_url(MEGA_URL, limit=limit)
    power_df = _fetch_url(POWER_URL, limit=limit)
    # dedupe and drop invalid date rows
    mega_df = mega_df.dropna(subset=["date"]).drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"]).reset_index(drop=True)
    power_df = power_df.dropna(subset=["date"]).drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"]).reset_index(drop=True)
    mega_df = mega_df.sort_values("date", ascending=True).reset_index(drop=True)
    power_df = power_df.sort_values("date", ascending=True).reset_index(drop=True)
    mega_path = os.path.join(save_dir, "mega_6_45_raw.csv")
    power_path = os.path.join(save_dir, "power_6_55_raw.csv")
    mega_df.to_csv(mega_path, index=False)
    power_df.to_csv(power_path, index=False)
    print(f"Fetched mega:{len(mega_df)} rows, power:{len(power_df)} rows")
    return mega_df, power_df
