"""
fetch_data.py — FINAL STABLE VERSION
- Mega 6/45 parser (multi-format, robust)
- Power 6/55 parser (div-based format from ketquadientoan.com)
- Retry fetch
- Normalize date + sort n1–n6
- fetch_all_data() public function
"""

import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime
from utils.logger import log

MEGA_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html"
POWER_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html"

HEADERS = {"User-Agent": "Mozilla/5.0"}


# ------------------------------------------------------
# Normalize date from multiple formats
# ------------------------------------------------------
def normalize_date(text):
    if not isinstance(text, str):
        return None
    text = text.strip()

    # remove weekday if exists (e.g. "Thứ 4, 22/09/2024")
    text = re.sub(r"^\D{1,10},\s*", "", text)

    # common formats
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except:
            pass

    # fallback
    try:
        return pd.to_datetime(text, dayfirst=True).strftime("%Y-%m-%d")
    except:
        return None


# ------------------------------------------------------
# Fetch HTML with retry mechanism
# ------------------------------------------------------
def fetch_html(url, retry=3, sleep_time=2):
    for i in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.encoding = "utf-8"
            r.raise_for_status()

            return r.text
        except Exception as e:
            log(f"[Retry {i+1}/{retry}] fetch error {url}: {e}")
            time.sleep(sleep_time)

    return None


# ------------------------------------------------------
# MEGA 6/45 PARSER — multi-format (uses read_html)
# ------------------------------------------------------
def parse_mega(html, limit=100):
    try:
        dfs = pd.read_html(StringIO(html))
    except:
        log("⚠ Mega parser: read_html failed")
        return pd.DataFrame()

    final = pd.DataFrame()

    for df in dfs:
        df.columns = [str(c).strip().lower() for c in df.columns]
        cols = df.columns.tolist()

        # Find date column
        date_col = None
        for c in cols:
            if df[c].astype(str).str.contains(r"\d{1,2}/\d{1,2}/\d{4}", na=False).any():
                date_col = c
                break
        if not date_col:
            continue

        # Find merged numbers column
        num_col = None
        for c in cols:
            # fixed regex (?:...) to avoid pandas group warning
            if df[c].astype(str).str.contains(r"(?:\d+[, ]+){5}\d+", na=False).any():
                num_col = c
                break

        if not num_col:
            continue

        # Extract numbers
        nums = df[num_col].astype(str).apply(lambda x: re.findall(r"\d+", x))
        nums = nums[nums.apply(lambda x: len(x) >= 6)]
        nums = nums.apply(lambda x: list(map(int, x[:6])))

        temp = pd.DataFrame(nums.tolist(), columns=[f"n{i}" for i in range(1, 7)])
        temp.insert(0, "date", df.loc[nums.index, date_col].astype(str))

        final = pd.concat([final, temp], ignore_index=True)

    if final.empty:
        log("⚠ Mega parser returned empty DataFrame")
        return final

    # Normalize date
    final["date"] = final["date"].apply(normalize_date)
    final = final.dropna(subset=["date"])

    # Sort n1–n6
    for idx in final.index:
        arr = sorted([final.loc[idx, f"n{i}"] for i in range(1, 7)])
        for i in range(6):
            final.loc[idx, f"n{i+1}"] = arr[i]

    final = final.drop_duplicates()
    final = final.sort_values("date", ascending=False).reset_index(drop=True)

    return final.head(limit)


# ------------------------------------------------------
# POWER 6/55 PARSER — div-based layout (NOT table-based)
# ------------------------------------------------------
def parse_power(html, limit=100):
    soup = BeautifulSoup(html, "lxml")

    items = soup.select(".list-result .item")
    if not items:
        log("⚠ Power parser: no .item rows found in HTML")
        return pd.DataFrame()

    rows = []

    for it in items:
        # Date
        dt = it.select_one(".col-date")
        if not dt:
            continue
        date = normalize_date(dt.get_text(strip=True))
        if not date:
            continue

        # Numbers
        nums = [
            int(x.get_text(strip=True))
            for x in it.select(".col-number .num")
            if x.get_text(strip=True).isdigit()
        ]

        if len(nums) < 6:
            continue

        nums = sorted(nums[:6])

        rows.append({
            "date": date,
            "n1": nums[0],
            "n2": nums[1],
            "n3": nums[2],
            "n4": nums[3],
            "n5": nums[4],
            "n6": nums[5],
        })

    df = pd.DataFrame(rows)

    if df.empty:
        log("⚠ Power parser returned empty DataFrame")
        return df

    df = df.drop_duplicates()
    df = df.sort_values("date", ascending=False).reset_index(drop=True)

    return df.head(limit)


# ------------------------------------------------------
# PUBLIC: Fetch both datasets
# ------------------------------------------------------
def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)

    log("🔹 Fetching Mega 6/45...")
    mega_html = fetch_html(MEGA_URL)
    mega_df = parse_mega(mega_html, limit) if mega_html else pd.DataFrame()

    log("🔹 Fetching Power 6/55...")
    power_html = fetch_html(POWER_URL)
    power_df = parse_power(power_html, limit) if power_html else pd.DataFrame()

    mega_df.to_csv(os.path.join(save_dir, "mega_6_45_raw.csv"), index=False)
    power_df.to_csv(os.path.join(save_dir, "power_6_55_raw.csv"), index=False)

    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")

    return mega_df, power_df


__all__ = ["fetch_all_data", "parse_mega", "parse_power"]
