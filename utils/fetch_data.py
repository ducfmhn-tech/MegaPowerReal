"""
fetch_data.py — FINAL VERSION (6 SOURCES)
- Mega 6/45: ketquadientoan, minhngoc, lotto-8
- Power 6/55: ketquadientoan, minhngoc, lotto-8
- HTML auto-fallback (source1 → source2 → source3)
- Robust parser for all formats
- Sorting n1–n6
- Public function: fetch_all_data()
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

HEADERS = {"User-Agent": "Mozilla/5.0"}

# -------------------------------------------------------------
# URLs
# -------------------------------------------------------------
MEGA_SOURCES = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
    "https://www.lotto-8.com/Vietnam/listltoVM45.asp"
]

POWER_SOURCES = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html",
    "https://www.lotto-8.com/Vietnam/listltoVM55.asp"
]

# -------------------------------------------------------------
# Normalize Date
# -------------------------------------------------------------
def normalize_date(text):
    if not isinstance(text, str):
        return None
    text = text.strip()
    text = re.sub(r"^\D{1,10},\s*", "", text)

    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except:
            pass

    try:
        return pd.to_datetime(text, dayfirst=True).strftime("%Y-%m-%d")
    except:
        return None


# -------------------------------------------------------------
# Fetch HTML with Retry
# -------------------------------------------------------------
def fetch_html(url, retry=3):
    for i in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.encoding = "utf-8"
            r.raise_for_status()
            log(f"✔ Fetched HTML OK: {url}")
            return r.text
        except Exception as e:
            log(f"[Retry {i+1}/{retry}] fetch error {url}: {e}")
            time.sleep(2)
    return None


# -------------------------------------------------------------
# Parse Mega (3 sources auto-detect)
# -------------------------------------------------------------
def parse_mega(html, limit=100):
    # 1) Try pandas.read_html for structured tables
    try:
        dfs = pd.read_html(StringIO(html))
        for df in dfs:
            df.columns = [str(c).strip().lower() for c in df.columns]
            num_col = None
            date_col = None

            # detect date column
            for c in df.columns:
                if df[c].astype(str).str.contains(r"\d{1,2}/\d{1,2}/\d{4}", na=False).any():
                    date_col = c
                    break

            # detect merged numbers
            for c in df.columns:
                if df[c].astype(str).str.contains(r"(\d+[, ]+){5}\d+", na=False).any():
                    num_col = c
                    break

            if not num_col or not date_col:
                continue

            # extract 6 numbers
            nums = df[num_col].astype(str).apply(lambda x: re.findall(r"\d+", x))
            nums = nums[nums.apply(lambda x: len(x) >= 6)]
            nums = nums.apply(lambda x: list(map(int, x[:6])))

            temp = pd.DataFrame(nums.tolist(), columns=[f"n{i}" for i in range(1, 7)])
            temp.insert(0, "date", df.loc[nums.index, date_col].astype(str))

            return cleanup(temp, limit)
    except:
        pass

    # 2) If HTML is unstructured → lotto-8 format
    soup = BeautifulSoup(html, "lxml")
    rows = soup.find_all("tr")
    extracted = []

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue

        date = normalize_date(tds[0].get_text(strip=True))
        if not date:
            continue

        nums = re.findall(r"\d+", " ".join(td.get_text(strip=True) for td in tds[1:7]))
        if len(nums) < 6:
            continue

        nums = sorted(list(map(int, nums[:6])))

        extracted.append({
            "date": date,
            "n1": nums[0],
            "n2": nums[1],
            "n3": nums[2],
            "n4": nums[3],
            "n5": nums[4],
            "n6": nums[5],
        })

    df = pd.DataFrame(extracted)
    return cleanup(df, limit)


# -------------------------------------------------------------
# Parse Power (similar to Mega)
# -------------------------------------------------------------
def parse_power(html, limit=100):
    # Try structured tables
    try:
        dfs = pd.read_html(StringIO(html))
        for df in dfs:
            # detect Power format: 6 columns containing numbers
            cols = df.columns
            if len(cols) < 6:
                continue

            # find date column
            date_col = None
            for c in cols:
                if df[c].astype(str).str.contains(r"\d{1,2}/\d{1,2}/\d{4}", na=False).any():
                    date_col = c
                    break
            if not date_col:
                continue

            # detect number columns (at least 6 numbers)
            num_cols = []
            for c in cols:
                if df[c].astype(str).str.contains(r"\d+", na=False).sum() > 10:
                    num_cols.append(c)
            if len(num_cols) < 6:
                continue

            # extract numbers
            nums = df[num_cols].astype(str).applymap(lambda x: re.findall(r"\d+", x)[0] if re.findall(r"\d+", x) else None)
            nums = nums.dropna().astype(int)

            # sort numbers by row
            sorted_nums = nums.apply(lambda row: sorted(row.values.tolist()), axis=1)

            temp = pd.DataFrame(sorted_nums.tolist(), columns=[f"n{i}" for i in range(1, 7)])
            temp.insert(0, "date", df[date_col].astype(str))

            return cleanup(temp, limit)
    except:
        pass

    # Fallback to lotto-8 style
    soup = BeautifulSoup(html, "lxml")
    rows = soup.find_all("tr")
    extracted = []

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue

        date = normalize_date(tds[0].get_text(strip=True))
        if not date:
            continue

        nums = re.findall(r"\d+", " ".join(td.get_text(strip=True) for td in tds[1:7]))
        if len(nums) < 6:
            continue

        nums = sorted(list(map(int, nums[:6])))

        extracted.append({
            "date": date,
            "n1": nums[0], "n2": nums[1], "n3": nums[2],
            "n4": nums[3], "n5": nums[4], "n6": nums[5],
        })

    df = pd.DataFrame(extracted)
    return cleanup(df, limit)


# -------------------------------------------------------------
# Clean & Finalize
# -------------------------------------------------------------
def cleanup(df, limit):
    if df.empty:
        return df

    df["date"] = df["date"].apply(normalize_date)
    df.dropna(subset=["date"], inplace=True)

    df.drop_duplicates(inplace=True)
    df.sort_values("date", ascending=False, inplace=True)

    return df.head(limit).reset_index(drop=True)


# -------------------------------------------------------------
# Public: fetch_all_data()
# -------------------------------------------------------------
def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)

    # 1) MEGA
    mega_df = pd.DataFrame()
    for url in MEGA_SOURCES:
        log(f"🔹 Fetching Mega: {url}")
        html = fetch_html(url)
        if html:
            df = parse_mega(html, limit)
            if not df.empty:
                mega_df = df
                break
    if mega_df.empty:
        log("❌ Mega: all sources failed")

    # 2) POWER
    power_df = pd.DataFrame()
    for url in POWER_SOURCES:
        log(f"🔹 Fetching Power: {url}")
        html = fetch_html(url)
        if html:
            df = parse_power(html, limit)
            if not df.empty:
                power_df = df
                break
    if power_df.empty:
        log("❌ Power: all sources failed")

    # Save raw data
    mega_df.to_csv(os.path.join(save_dir, "mega_6_45_raw.csv"), index=False)
    power_df.to_csv(os.path.join(save_dir, "power_6_55_raw.csv"), index=False)

    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")

    return mega_df, power_df


__all__ = ["fetch_all_data", "parse_mega", "parse_power"]
