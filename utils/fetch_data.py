import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from utils.logger import log

HEADERS = {"User-Agent": "Mozilla/5.0"}

# Các nguồn dữ liệu
MEGA_SOURCES = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
    "https://www.lotto-8.com/Vietnam/listltoVM45.asp",
]

POWER_SOURCES = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html",
    "https://www.lotto-8.com/Vietnam/listltoVM55.asp",
]


# ------------------------------------------------------------
# Helper: Normalize date
# ------------------------------------------------------------
def normalize_date(s):
    if not isinstance(s, str):
        return None

    s = s.strip()
    s = s.replace("Thứ ", "").replace("CN,", "").replace("  ", " ")
    s = re.sub(r"^[A-Za-zÀ-ỹ ,]+,", "", s).strip()

    fmts = ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]
    for f in fmts:
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except:
            pass

    try:
        return pd.to_datetime(s, dayfirst=True).strftime("%Y-%m-%d")
    except:
        return None


# ------------------------------------------------------------
# HTML fetch with retry
# ------------------------------------------------------------
def fetch_html(url, retry=3):
    for i in range(retry):
        try:
            res = requests.get(url, headers=HEADERS, timeout=20)
            res.encoding = "utf-8"
            res.raise_for_status()
            log(f"✔ Fetched HTML OK: {url}")
            return res.text
        except Exception as e:
            log(f"⚠ Retry {i+1}/{retry} failed for {url}: {e}")
            time.sleep(1.5)
    return None


# ------------------------------------------------------------
# Detect numbers safely (Mega/Power)
# ------------------------------------------------------------
def extract_numbers_from_text(txt):
    nums = re.findall(r"\d+", txt)
    nums = list(map(int, nums))
    if len(nums) < 6:
        return None
    nums = sorted(nums[:6])
    return nums


# ------------------------------------------------------------
# Parse table where columns may vary
# ------------------------------------------------------------
def parse_generic_table(html, limit=120, need_max=45):
    dfs = []
    try:
        dfs = pd.read_html(html)
    except:
        return pd.DataFrame()

    results = []

    for df in dfs:
        for _, row in df.iterrows():
            row_str = " ".join(map(str, row.values))

            nums = extract_numbers_from_text(row_str)
            if not nums:
                continue
            if max(nums) > need_max:   # Loại sai Power vào Mega & ngược lại
                continue

            date = None
            for c in df.columns:
                d = normalize_date(str(row[c]))
                if d:
                    date = d
                    break
            if not date:
                continue

            results.append([date] + nums)

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results, columns=["date"] + [f"n{i}" for i in range(1, 7)])
    df.drop_duplicates(inplace=True)
    df.sort_values("date", ascending=False, inplace=True)
    return df.head(limit).reset_index(drop=True)


# ------------------------------------------------------------
# Parse lotto-8 (Mega/Power)
# ------------------------------------------------------------
def parse_lotto8(html, need_max=45, limit=120):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")
    results = []

    for tr in rows:
        cols = tr.find_all("td")
        if len(cols) < 7:
            continue

        date = normalize_date(cols[0].get_text(strip=True))
        if not date:
            continue

        nums = extract_numbers_from_text(" ".join(c.get_text() for c in cols[1:7]))
        if not nums:
            continue
        if max(nums) > need_max:
            continue

        results.append([date] + nums)

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results, columns=["date"] + [f"n{i}" for i in range(1, 7)])
    df.drop_duplicates(inplace=True)
    df.sort_values("date", ascending=False, inplace=True)
    return df.head(limit).reset_index(drop=True)


# ------------------------------------------------------------
# Fetch Mega from all sources
# ------------------------------------------------------------
def fetch_mega(limit=120):
    collected = []

    for url in MEGA_SOURCES:
        log(f"🔹 Fetching Mega: {url}")
        html = fetch_html(url)
        if not html:
            continue

        if "lotto-8" in url:
            df = parse_lotto8(html, need_max=45, limit=limit)
        else:
            df = parse_generic_table(html, limit=limit, need_max=45)

        if not df.empty:
            collected.append(df)

    if not collected:
        return pd.DataFrame()

    final = pd.concat(collected, ignore_index=True)
    final.drop_duplicates(inplace=True)
    final.sort_values("date", ascending=False, inplace=True)
    return final.head(limit).reset_index(drop=True)


# ------------------------------------------------------------
# Fetch Power from all sources
# ------------------------------------------------------------
def fetch_power(limit=120):
    collected = []

    for url in POWER_SOURCES:
        log(f"🔹 Fetching Power: {url}")
        html = fetch_html(url)
        if not html:
            continue

        if "lotto-8" in url:
            df = parse_lotto8(html, need_max=55, limit=limit)
        else:
            df = parse_generic_table(html, limit=limit, need_max=55)

        if not df.empty:
            collected.append(df)

    if not collected:
        return pd.DataFrame()
    final = pd.concat(collected, ignore_index=True)
    final.drop_duplicates(inplace=True)
    final.sort_values("date", ascending=False, inplace=True)
    return final.head(limit).reset_index(drop=True)


# ------------------------------------------------------------
# Public: fetch_all_data
# ------------------------------------------------------------
def fetch_all_data(limit=120, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)

    mega_df = fetch_mega(limit)
    power_df = fetch_power(limit)

    mega_df.to_csv(os.path.join(save_dir, "mega_6_45_raw.csv"), index=False)
    power_df.to_csv(os.path.join(save_dir, "power_6_55_raw.csv"), index=False)

    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")
    return mega_df, power_df


__all__ = ["fetch_all_data"]
