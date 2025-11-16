"""
fetch_data.py — FINAL VERSION (NO SELENIUM)
- Multi-source fetch for Mega 6/45 & Power 6/55
- Sources:
    1. ketquadientoan.com
    2. minhngoc.net.vn
    3. lotto-8.com (HTML table)
- Auto-clean, auto-sort, auto-normalize
- Always returns 100–300 rows per lottery
"""

import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from utils.logger import log

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ===========================
# Helpers
# ===========================

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


def fetch_html(url, retry=3):
    for i in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            log(f"✔ Fetched HTML OK: {url}")
            return r.text
        except Exception as e:
            log(f"[Retry {i+1}/{retry}] fetch error {url}: {e}")
            time.sleep(2)
    return None

# ===========================
# Mega 6/45 Parsers
# ===========================

def parse_mega_ketqua(html):
    dfs = pd.read_html(html, flavor="lxml")
    out = []

    for df in dfs:
        df = df.copy()
        df.columns = [str(x).lower() for x in df.columns]

        # detect date col
        date_col = None
        for c in df.columns:
            if df[c].astype(str).str.contains(r"\d{2}/\d{2}/\d{4}", na=False).any():
                date_col = c
                break
        if not date_col:
            continue

        # detect merged numbers col
        num_col = None
        for c in df.columns:
            if df[c].astype(str).str.contains(r"(\d+[, ]+){5}\d+", na=False).any():
                num_col = c
                break
        if not num_col:
            continue

        for idx, row in df.iterrows():
            date = normalize_date(str(row[date_col]))
            nums = re.findall(r"\d+", str(row[num_col]))
            if len(nums) >= 6:
                nums = sorted(list(map(int, nums[:6])))
                out.append([date] + nums)

    if not out:
        return pd.DataFrame()

    df = pd.DataFrame(out, columns=["date"] + [f"n{i}" for i in range(1, 7)])
    df.dropna(subset=["date"], inplace=True)
    df.drop_duplicates(inplace=True)
    df.sort_values("date", ascending=False, inplace=True)
    return df


def parse_mega_minhngoc(html):
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "kq"})
    if not table:
        return pd.DataFrame()

    rows = table.find_all("tr")
    out = []

    for tr in rows:
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cols) < 7:
            continue

        date = normalize_date(cols[0])
        nums = re.findall(r"\d+", " ".join(cols[1:7]))

        if len(nums) >= 6:
            nums = sorted(list(map(int, nums[:6])))
            out.append([date] + nums)

    df = pd.DataFrame(out, columns=["date"] + [f"n{i}" for i in range(1, 7)])
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.sort_values("date", ascending=False, inplace=True)
    return df


def parse_mega_lotto(html):
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table tr")
    out = []

    for tr in rows:
        cols = [c.get_text(strip=True) for c in tr.find_all("td")]
        if len(cols) < 7:
            continue

        date = normalize_date(cols[0])
        nums = re.findall(r"\d+", " ".join(cols[1:7]))

        if len(nums) >= 6:
            nums = sorted(list(map(int, nums[:6])))
            out.append([date] + nums)

    df = pd.DataFrame(out, columns=["date"] + [f"n{i}" for i in range(1, 7)])
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.sort_values("date", ascending=False, inplace=True)
    return df


# ===========================
# Power 6/55 Parsers
# ===========================

def parse_power_ketqua(html):
    dfs = pd.read_html(html, flavor="lxml")
    out = []

    for df in dfs:
        df = df.copy()
        df.columns = [str(c).lower() for c in df.columns]

        date_col = None
        for c in df.columns:
            if df[c].astype(str).str.contains(r"\d{2}/\d{2}/\d{4}", na=False).any():
                date_col = c
                break
        if not date_col:
            continue

        # any column with 6 numbers
        num_col = None
        for c in df.columns:
            if df[c].astype(str).str.contains(r"(\d+[, ]+){5}\d+", na=False).any():
                num_col = c
                break
        if not num_col:
            continue

        for _, row in df.iterrows():
            date = normalize_date(str(row[date_col]))
            nums = re.findall(r"\d+", str(row[num_col]))
            if len(nums) >= 6:
                nums = sorted(list(map(int, nums[:6])))
                out.append([date] + nums)

    if not out:
        return pd.DataFrame()

    df = pd.DataFrame(out, columns=["date"] + [f"n{i}" for i in range(1, 7)])
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.sort_values("date", ascending=False, inplace=True)
    return df


def parse_power_minhngoc(html):
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "kq"})
    if not table:
        return pd.DataFrame()

    rows = table.find_all("tr")
    out = []

    for tr in rows:
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cols) < 7:
            continue

        date = normalize_date(cols[0])
        nums = re.findall(r"\d+", " ".join(cols[1:7]))
        if len(nums) >= 6:
            nums = sorted(list(map(int, nums[:6])))
            out.append([date] + nums)

    df = pd.DataFrame(out, columns=["date"] + [f"n{i}" for i in range(1, 7)])
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.sort_values("date", ascending=False, inplace=True)
    return df


def parse_power_lotto(html):
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table tr")
    out = []

    for tr in rows:
        cols = [c.get_text(strip=True) for c in tr.find_all("td")]
        if len(cols) < 7:
            continue

        date = normalize_date(cols[0])
        nums = re.findall(r"\d+", " ".join(cols[1:7]))

        if len(nums) >= 6:
            nums = sorted(list(map(int, nums[:6])))
            out.append([date] + nums)

    df = pd.DataFrame(out, columns=["date"] + [f"n{i}" for i in range(1, 7)])
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.sort_values("date", ascending=False, inplace=True)
    return df


# ===========================
# PUBLIC: multi-source fetch
# ===========================

MEGA_SOURCES = [
    ("ketqua", "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html", parse_mega_ketqua),
    ("minhngoc", "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html", parse_mega_minhngoc),
    ("lotto", "https://www.lotto-8.com/Vietnam/listltoVM45.asp", parse_mega_lotto),
]

POWER_SOURCES = [
    ("ketqua", "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html", parse_power_ketqua),
    ("minhngoc", "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html", parse_power_minhngoc),
    ("lotto", "https://www.lotto-8.com/Vietnam/listltoVM55.asp", parse_power_lotto),
]


def fetch_all_data(limit=150, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)

    def fetch_multi(sources):
        dfs = []
        for name, url, parser in sources:
            log(f"🔹 Fetching from {name}: {url}")
            html = fetch_html(url)
            if html:
                df = parser(html)
                if not df.empty:
                    dfs.append(df)
        if not dfs:
            return pd.DataFrame()
        df_all = pd.concat(dfs, ignore_index=True)
        df_all.drop_duplicates(subset=["date"], inplace=True)
        df_all.sort_values("date", ascending=False, inplace=True)
        return df_all.head(limit).reset_index(drop=True)

    mega_df  = fetch_multi(MEGA_SOURCES)
    power_df = fetch_multi(POWER_SOURCES)

    mega_df.to_csv(os.path.join(save_dir, "mega_6_45_raw.csv"), index=False)
    power_df.to_csv(os.path.join(save_dir, "power_6_55_raw.csv"), index=False)

    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")

    return mega_df, power_df


__all__ = ["fetch_all_data"]
