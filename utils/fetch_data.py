"""
fetch_data.py — FINAL VERSION (Stable for GitHub Actions)
- Robust parser for Mega 6/45 & Power 6/55
- Auto-detect table or div-based structure
- Retry + fallback sources
- Normalize date, sort n1–n6
- Save raw HTML for debugging
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

# ------------------------------------------------------
# CONFIG
# ------------------------------------------------------
N_PERIODS = 120

PRIMARY_MEGA_URL  = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html"
PRIMARY_POWER_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html"

# Backup sources (ổn định hơn cho GitHub Actions)
BACKUP_MEGA_URLS = [
    "https://www.minhngoc.net.vn/ket-qua-xo-so/mien-bac/vietlott-mega-6-45.html",
    "https://www.lotto-8.com/result/vietlott-mega645"
]
BACKUP_POWER_URLS = [
    "https://www.minhngoc.net.vn/ket-qua-xo-so/mien-bac/vietlott-power-655.html",
    "https://www.lotto-8.com/result/vietlott-power655"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120 Safari/537.36"
}

SAVE_DEBUG_HTML = True       # Lưu HTML thô để debug nếu lỗi


# ------------------------------------------------------
# DATE NORMALIZATION
# ------------------------------------------------------
def normalize_date(text):
    if not isinstance(text, str):
        return None
    text = text.strip()

    # remove weekday
    text = re.sub(r"^[^\d]+,\s*", "", text)

    # try known formats
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
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
# FETCH HTML WITH RETRY
# ------------------------------------------------------
def fetch_html(url, name="unknown", retry=3, timeout=20):
    for i in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.encoding = "utf-8"
            if r.status_code == 200:
                return r.text
            log(f"[{name}] Status: {r.status_code}")
        except Exception as e:
            log(f"[Retry {i+1}/{retry}] fetch error {url}: {e}")
            time.sleep(2)
    return None


# ------------------------------------------------------
# NUMBER EXTRACTOR (robust)
# ------------------------------------------------------
def extract_6_numbers(text, max_val):
    nums = re.findall(r"\b\d{1,2}\b", text)
    nums = [int(n) for n in nums if 1 <= int(n) <= max_val]
    return sorted(nums[-6:]) if len(nums) >= 6 else None


# ------------------------------------------------------
# PARSE MEGA 6/45 (robust multi-structure)
# ------------------------------------------------------
def parse_mega(html, limit):
    if not html:
        return pd.DataFrame()

    soup = BeautifulSoup(html, "lxml")

    # 1) Try table-based parsing
    tables = soup.find_all("table")
    if tables:
        all_rows = []
        for tbl in tables:
            for tr in tbl.find_all("tr"):
                cols = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                if len(cols) < 2:
                    continue

                date = normalize_date(cols[0])
                if not date:
                    continue

                nums = extract_6_numbers(" ".join(cols[1:]), 45)
                if not nums:
                    continue

                all_rows.append({
                    "date": date,
                    "n1": nums[0], "n2": nums[1], "n3": nums[2],
                    "n4": nums[3], "n5": nums[4], "n6": nums[5],
                })

        if all_rows:
            df = pd.DataFrame(all_rows)
            df.drop_duplicates(inplace=True)
            df.sort_values("date", ascending=False, inplace=True)
            return df.head(limit).reset_index(drop=True)

    # 2) Try read_html fallback
    try:
        dfs = pd.read_html(StringIO(html))
        for df in dfs:
            rows = []
            for _, row in df.iterrows():
                row_str = " ".join(str(x) for x in row)
                nums = extract_6_numbers(row_str, 45)
                if not nums:
                    continue
                date_candidate = re.search(r"\d{1,2}[/-]\d{1,2}[/-]\d{4}", row_str)
                if not date_candidate:
                    continue

                rows.append({
                    "date": normalize_date(date_candidate.group()),
                    "n1": nums[0], "n2": nums[1], "n3": nums[2],
                    "n4": nums[3], "n5": nums[4], "n6": nums[5],
                })

            if rows:
                df2 = pd.DataFrame(rows)
                df2.drop_duplicates(inplace=True)
                df2.sort_values("date", ascending=False, inplace=True)
                return df2.head(limit).reset_index(drop=True)
    except:
        pass

    return pd.DataFrame()


# ------------------------------------------------------
# PARSE POWER 6/55 (detect table OR div-based)
# ------------------------------------------------------
def parse_power(html, limit):
    if not html:
        return pd.DataFrame()

    soup = BeautifulSoup(html, "lxml")

    # (A) NEW LAYOUT: div-based list
    items = soup.select(".list-result .item")
    if items:
        rows = []
        for it in items:
            date_txt = it.select_one(".col-date")
            if not date_txt:
                continue
            date = normalize_date(date_txt.get_text(strip=True))

            nums_txt = " ".join(
                n.get_text(strip=True) for n in it.select(".col-number .number")
            )
            nums = extract_6_numbers(nums_txt, 55)
            if not nums:
                continue

            rows.append({
                "date": date,
                "n1": nums[0], "n2": nums[1], "n3": nums[2],
                "n4": nums[3], "n5": nums[4], "n6": nums[5],
            })

        if rows:
            df = pd.DataFrame(rows)
            df.drop_duplicates(inplace=True)
            df.sort_values("date", ascending=False, inplace=True)
            return df.head(limit).reset_index(drop=True)

        log("⚠ Power parser: found .item but no numbers extracted")

    # (B) TABLE fallback
    tables = soup.find_all("table")
    if tables:
        rows = []
        for tbl in tables:
            for tr in tbl.find_all("tr"):
                txt = " ".join(td.get_text(" ", strip=True) for td in tr.find_all(["td","th"]))
                nums = extract_6_numbers(txt, 55)
                if not nums:
                    continue
                date = re.search(r"\d{1,2}[/-]\d{1,2}[/-]\d{4}", txt)
                if not date:
                    continue

                rows.append({
                    "date": normalize_date(date.group()),
                    "n1": nums[0], "n2": nums[1], "n3": nums[2],
                    "n4": nums[3], "n5": nums[4], "n6": nums[5],
                })

        if rows:
            df = pd.DataFrame(rows)
            df.drop_duplicates(inplace=True)
            df.sort_values("date", ascending=False, inplace=True)
            return df.head(limit).reset_index(drop=True)

    log("⚠ Power parser: no valid structure found")
    return pd.DataFrame()


# ------------------------------------------------------
# VALIDATE OUTPUT DATAFRAME
# ------------------------------------------------------
def validate(df, max_num):
    if df.empty:
        return False
    if "date" not in df.columns:
        return False
    for i in range(1,7):
        col = f"n{i}"
        if col not in df.columns:
            return False
        if not df[col].between(1, max_num).all():
            return False
    return True


# ------------------------------------------------------
# PUBLIC: FETCH ALL DATA WITH FALLBACK
# ------------------------------------------------------
def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)

    # ---------- MEGA ----------
    log("🔹 Fetching Mega 6/45...")
    html = fetch_html(PRIMARY_MEGA_URL, "MEGA")
    if SAVE_DEBUG_HTML and html:
        open(os.path.join(save_dir,"debug_mega.html"),"w",encoding="utf-8").write(html)

    mega_df = parse_mega(html, limit)
    if not validate(mega_df, 45):
        log("⚠ Mega main source invalid → trying backups...")
        for url in BACKUP_MEGA_URLS:
            h = fetch_html(url, "MEGA-BACKUP")
            if not h:
                continue
            mega_df = parse_mega(h, limit)
            if validate(mega_df, 45):
                break

    # ---------- POWER ----------
    log("🔹 Fetching Power 6/55...")
    html = fetch_html(PRIMARY_POWER_URL, "POWER")
    if SAVE_DEBUG_HTML and html:
        open(os.path.join(save_dir,"debug_power.html"),"w",encoding="utf-8").write(html)

    power_df = parse_power(html, limit)
    if not validate(power_df, 55):
        log("⚠ Power main source invalid → trying backups...")
        for url in BACKUP_POWER_URLS:
            h = fetch_html(url, "POWER-BACKUP")
            if not h:
                continue
            power_df = parse_power(h, limit)
            if validate(power_df, 55):
                break

    # ---------- SAVE ----------
    mega_df.to_csv(os.path.join(save_dir, "mega_6_45_raw.csv"), index=False)
    power_df.to_csv(os.path.join(save_dir, "power_6_55_raw.csv"), index=False)

    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")

    return mega_df, power_df


__all__ = ["fetch_all_data", "parse_mega", "parse_power"]
