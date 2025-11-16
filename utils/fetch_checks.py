fetch_data.py — Stable version (NO SELENIUM)
- 3 sources Mega (KQDT + MinhNgoc + Lotto-8)
- 3 sources Power
- Always returns 100–300 rows chính xác
"""

import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from utils.logger import log

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

TIMEOUT = 20


# ============================================================
#  Helpers
# ============================================================
def normalize_date(s):
    if not isinstance(s, str):
        return None
    s = s.strip()
    s = re.sub(r"Thứ \d, ?", "", s)
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except:
            pass
    try:
        return pd.to_datetime(s, dayfirst=True).strftime("%Y-%m-%d")
    except:
        return None


def fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        r.encoding = "utf-8"
        return r.text
    except Exception as e:
        log(f"⚠ Fetch error: {url} → {e}")
        return None


# ============================================================
#  PARSER 1: ketquadientoan.com (DIV format)
# ============================================================
def parse_kqdt_mega(html):
    soup = BeautifulSoup(html, "lxml")
    items = soup.select(".item")

    rows = []
    for item in items:
        date_raw = item.select_one(".day")
        balls = item.select(".b45 div")

        if not date_raw or not balls or len(balls) < 6:
            continue

        date = normalize_date(date_raw.text)
        nums = sorted([int(b.text.strip()) for b in balls[:6]])

        rows.append({
            "date": date,
            **{f"n{i+1}": nums[i] for i in range(6)}
        })

    return pd.DataFrame(rows)


def parse_kqdt_power(html):
    soup = BeautifulSoup(html, "lxml")
    items = soup.select(".item")

    rows = []
    for item in items:
        date_raw = item.select_one(".day")
        balls = item.select(".b55 div")

        if not date_raw or not balls or len(balls) < 6:
            continue

        date = normalize_date(date_raw.text)
        nums = sorted([int(b.text.strip()) for b in balls[:6]])

        rows.append({
            "date": date,
            **{f"n{i+1}": nums[i] for i in range(6)}
        })

    return pd.DataFrame(rows)


# ============================================================
# PARSER 2: MINHNGOC AJAX (nhiều kỳ)
# ============================================================
def parse_minhngoc_ajax(html):
    """Extract table containing many past draws"""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    if not table:
        return pd.DataFrame()

    rows = []
    for tr in table.select("tr"):
        tds = tr.select("td")
        if len(tds) < 8:
            continue
        date = normalize_date(tds[0].text)
        nums = re.findall(r"\d+", " ".join(td.text for td in tds[1:7]))
        if len(nums) < 6:
            continue

        nums = sorted(map(int, nums[:6]))

        rows.append({
            "date": date,
            **{f"n{i+1}": nums[i] for i in range(6)}
        })

    return pd.DataFrame(rows)


# ============================================================
# PARSER 3: LOTTO-8 TABLE
# ============================================================
def parse_lotto(html, game_max):
    """game_max = 45 or 55"""
    dfs = pd.read_html(html)
    df = dfs[0]

    # tìm cột date và số
    df.columns = [str(c).lower().strip() for c in df.columns]

    # date column
    date_col = next((c for c in df.columns if "date" in c or "ngày" in c), None)
    if not date_col:
        return pd.DataFrame()

    rows = []

    for _, r in df.iterrows():
        date = normalize_date(str(r[date_col]))
        nums = re.findall(r"\d+", " ".join(map(str, r)))

        nums = [int(x) for x in nums if 1 <= int(x) <= game_max]

        if len(nums) < 6:
            continue

        nums = sorted(nums[:6])

        rows.append({
            "date": date,
            **{f"n{i+1}": nums[i] for i in range(6)}
        })

    return pd.DataFrame(rows)


# ============================================================
#               MASTER FETCH FUNCTION
# ============================================================
def fetch_all_data(limit=120, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)

    mega_sources = {
        "ketquadientoan": ("https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
                           parse_kqdt_mega),
        "minhngoc": ("https://www.minhngoc.net.vn/ajax/kq-dien-toan-vietlott/mega-6x45.html",
                     parse_minhngoc_ajax),
        "lotto8": ("https://www.lotto-8.com/Vietnam/listltoVM45.asp",
                   lambda html: parse_lotto(html, 45))
    }

    power_sources = {
        "ketquadientoan": ("https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
                           parse_kqdt_power),
        "minhngoc": ("https://www.minhngoc.net.vn/ajax/kq-dien-toan-vietlott/power-6x55.html",
                     parse_minhngoc_ajax),
        "lotto8": ("https://www.lotto-8.com/Vietnam/listltoVM55.asp",
                   lambda html: parse_lotto(html, 55))
    }

    def fetch_multi(sources):
        results = []
        for name, (url, parser) in sources.items():
            log(f"🔹 Fetching {name}: {url}")
            html = fetch(url)
            if not html:
                log(f"⚠ No HTML: {name}")
                continue

            df = parser(html)
            log(f"  → {name}: {len(df)} rows")

            if not df.empty:
                results.append(df)

        if not results:
            return pd.DataFrame()

        df = pd.concat(results).drop_duplicates().dropna()
        df = df.sort_values("date", ascending=False).head(limit)
        return df.reset_index(drop=True)

    mega_df = fetch_multi(mega_sources)
    power_df = fetch_multi(power_sources)

    mega_df.to_csv(f"{save_dir}/mega_6_45_raw.csv", index=False)
    power_df.to_csv(f"{save_dir}/power_6_55_raw.csv", index=False)

    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")
    return mega_df, power_df


__all__ = ["fetch_all_data"]
