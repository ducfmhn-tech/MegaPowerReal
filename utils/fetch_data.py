import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from utils.logger import log

HEADERS = {"User-Agent": "Mozilla/5.0"}
MEGA_MAX = 45
POWER_MAX = 55

URLS = {
    "mega": [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
        "https://www.lotto-8.com/Vietnam/listltoVM45.asp",
    ],
    "power": [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html",
        "https://www.lotto-8.com/Vietnam/listltoVM55.asp",
    ]
}

# ------------------------------
# Helper - fetch HTML
# ------------------------------
def fetch_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        log(f"[fetch_html] ERROR {url}: {e}")
        return None

# ------------------------------
# Helper - extract date
# ------------------------------
def extract_date(text):
    if not text:
        return None
    match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%d/%m/%Y").strftime("%Y-%m-%d")
    except:
        return None

# ==========================================================
#  PARSER 1 – KETQUADIEN TOAN (Mega)
# ==========================================================
def parse_mega_ketqua(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select(".result-list.mega .result-row")
    data = []

    for row in rows:
        date_text = row.select_one(".draw-date")
        nums = row.select(".ball-mega")

        if not date_text:
            continue
        if len(nums) < 6:
            continue

        date = extract_date(date_text.get_text(strip=True))
        if not date:
            continue

        numbers = [int(n.get_text(strip=True)) for n in nums[:6]]
        numbers = sorted(numbers)

        data.append({
            "date": date,
            "n1": numbers[0],
            "n2": numbers[1],
            "n3": numbers[2],
            "n4": numbers[3],
            "n5": numbers[4],
            "n6": numbers[5],
            "source": "ketquadientoan"
        })

    return pd.DataFrame(data)

# ==========================================================
#  PARSER 2 – KETQUADIEN TOAN (Power)
# ==========================================================
def parse_power_ketqua(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select(".result-list.power .result-row")
    data = []

    for row in rows:
        date_text = row.select_one(".draw-date")
        nums = row.select(".ball-power")
        bonus = row.select_one(".ball-bonus")

        if not date_text:
            continue
        if len(nums) < 6:
            continue
        date = extract_date(date_text.get_text(strip=True))
        if not date:
            continue

        numbers = [int(n.get_text(strip=True)) for n in nums[:6]]
        numbers = sorted(numbers)
        bonus_val = int(bonus.get_text(strip=True)) if bonus else None

        data.append({
            "date": date,
            "n1": numbers[0],
            "n2": numbers[1],
            "n3": numbers[2],
            "n4": numbers[3],
            "n5": numbers[4],
            "n6": numbers[5],
            "bonus": bonus_val,
            "source": "ketquadientoan"
        })

    return pd.DataFrame(data)

# ==========================================================
#  PARSER 3 – MINH NGOC (Mega + Power)
# ==========================================================
def parse_minhngoc(html, max_val):
    try:
        dfs = pd.read_html(html)
    except:
        return pd.DataFrame()

    df = dfs[0]
    df.columns = [str(c).strip().lower() for c in df.columns]

    date_col = None
    for c in df.columns:
        if df[c].astype(str).str.contains(r"\d{4}-\d{2}-\d{2}", na=False).any():
            date_col = c
            break

    if not date_col:
        return pd.DataFrame()

    num_cols = [c for c in df.columns if c.startswith("n")]
    if len(num_cols) < 6:
        return pd.DataFrame()

    data = []

    for _, row in df.iterrows():
        date = str(row[date_col]).strip()
        try:
            d2 = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")
        except:
            continue

        nums = []
        ok = True

        for i in range(1, 7):
            val = row.get(f"n{i}")
            try:
                val = int(val)
                if 1 <= val <= max_val:
                    nums.append(val)
                else:
                    ok = False
            except:
                ok = False

        if not ok or len(nums) < 6:
            continue

        nums = sorted(nums)

        data.append({
            "date": d2,
            "n1": nums[0],
            "n2": nums[1],
            "n3": nums[2],
            "n4": nums[3],
            "n5": nums[4],
            "n6": nums[5],
            "source": "minhngoc"
        })

    return pd.DataFrame(data)

# ==========================================================
#  PARSER 4 – LOTTO-8 (Mega + Power)
# ==========================================================
def parse_lotto(html):
    try:
        dfs = pd.read_html(html)
        df = dfs[0]
    except:
        return pd.DataFrame()

    df.columns = [str(c).strip().lower() for c in df.columns]

    if "date" not in df.columns:
        return pd.DataFrame()

    if not any(col.startswith("no") or col.startswith("n1") for col in df.columns):
        return pd.DataFrame()

    num_cols = [c for c in df.columns if c.startswith("n")]
    if len(num_cols) < 6:
        return pd.DataFrame()

    data = []

    for _, row in df.iterrows():
        date = row["date"]
        try:
            date = datetime.strptime(date, "%Y/%m/%d").strftime("%Y-%m-%d")
        except:
            continue

        nums = []
        for i in range(1, 7):
            try:
                nums.append(int(row[f"n{i}"]))
            except:
                nums = []
                break

        if len(nums) < 6:
            continue

        nums = sorted(nums)
        data.append({
            "date": date,
            "n1": nums[0], "n2": nums[1], "n3": nums[2],
            "n4": nums[3], "n5": nums[4], "n6": nums[5],
            "source": "lotto8"
        })

    return pd.DataFrame(data)

# ==========================================================
#  MERGE SOURCE
# ==========================================================
def merge_sources(dfs):
    dfs = [d for d in dfs if d is not None and not d.empty]
    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    df.drop_duplicates(subset=["date", "n1", "n2", "n3", "n4", "n5", "n6"], inplace=True)
    df.sort_values("date", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

# ==========================================================
#  PUBLIC API
# ==========================================================
def fetch_all_data(limit=120, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)

    # === MEGA ===
    mega_dfs = []
    for url in URLS["mega"]:
        log(f"🔹 Fetching Mega: {url}")
        html = fetch_html(url)
        if not html:
            continue

        if "ketquadientoan" in url:
            mega_dfs.append(parse_mega_ketqua(html))
        elif "minhngoc" in url:
            mega_dfs.append(parse_minhngoc(html, MEGA_MAX))
        else:
            mega_dfs.append(parse_lotto(html))

    mega_df = merge_sources(mega_dfs).head(limit)

    # === POWER ===
    power_dfs = []
    for url in URLS["power"]:
        log(f"🔹 Fetching Power: {url}")
        html = fetch_html(url)
        if not html:
            continue

        if "ketquadientoan" in url:
            power_dfs.append(parse_power_ketqua(html))
        elif "minhngoc" in url:
            power_dfs.append(parse_minhngoc(html, POWER_MAX))
        else:
            power_dfs.append(parse_lotto(html))

    power_df = merge_sources(power_dfs).head(limit)

    # SAVE
    mega_df.to_csv(f"{save_dir}/mega_6_45_raw.csv", index=False)
    power_df.to_csv(f"{save_dir}/power_6_55_raw.csv", index=False)

    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")
    return mega_df, power_df
