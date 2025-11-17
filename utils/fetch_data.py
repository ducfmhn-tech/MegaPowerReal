# utils/fetch_data.py
import os, time, re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime
from utils.logger import log

HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

MEGA_URLS = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
    "https://www.lotto-8.com/Vietnam/listltoVM45.asp"
]
POWER_URLS = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html",
    "https://www.lotto-8.com/Vietnam/listltoVM55.asp"
]

def get_html(url, retry=3, timeout=15):
    for i in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.encoding = r.apparent_encoding or "utf-8"
            r.raise_for_status()
            log(f"✔ Fetched HTML OK: {url}")
            return r.text
        except Exception as e:
            log(f"[Retry {i+1}/{retry}] fetch error {url}: {e}")
            time.sleep(1)
    return None

# Normalize date: try several formats and return YYYY-MM-DD
def normalize_date(text):
    if not isinstance(text, str):
        return None
    text = text.strip()
    # try dd/mm/YYYY
    m = re.search(r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})", text)
    if m:
        for fmt in ("%d/%m/%Y","%d-%m-%Y","%Y-%m-%d"):
            try:
                return datetime.strptime(m.group(1), fmt).strftime("%Y-%m-%d")
            except:
                pass
    # try yyyy-mm-dd inside text
    m2 = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m2:
        return m2.group(1)
    # fallback
    try:
        return pd.to_datetime(text, dayfirst=True).strftime("%Y-%m-%d")
    except:
        return None

# Parse ketquadientoan Mega
def parse_mega_ketquad(html):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    items = soup.select(".result-list.mega .result-row")
    for it in items:
        date_el = it.select_one(".draw-date")
        balls = it.select(".ball-mega")
        if not date_el or len(balls) < 6:
            continue
        date = normalize_date(date_el.get_text(strip=True))
        if not date:
            continue
        nums = [int(b.get_text(strip=True)) for b in balls[:6]]
        nums = sorted(nums)
        rows.append({"date":date, "n1":nums[0],"n2":nums[1],"n3":nums[2],
                     "n4":nums[3],"n5":nums[4],"n6":nums[5], "source":"ketquadientoan"})
    return pd.DataFrame(rows)

# Parse ketquadientoan Power
def parse_power_ketquad(html):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    items = soup.select(".result-list.power .result-row")
    for it in items:
        date_el = it.select_one(".draw-date")
        balls = it.select(".ball-power")
        bonus_el = it.select_one(".ball-bonus")
        if not date_el or len(balls) < 6:
            continue
        date = normalize_date(date_el.get_text(strip=True))
        if not date:
            continue
        nums = [int(b.get_text(strip=True)) for b in balls[:6]]
        nums = sorted(nums)
        bonus = int(bonus_el.get_text(strip=True)) if bonus_el else None
        rows.append({"date":date,"n1":nums[0],"n2":nums[1],"n3":nums[2],
                     "n4":nums[3],"n5":nums[4],"n6":nums[5],
                     "bonus":bonus, "source":"ketquadientoan"})
    return pd.DataFrame(rows)

# Parse table-based pages using pandas.read_html safely via StringIO
def parse_table_html(html, mega=True):
    try:
        df_list = pd.read_html(StringIO(html))
    except Exception:
        return pd.DataFrame()
    if not df_list:
        return pd.DataFrame()
    df = df_list[0]
    # try to normalize columns heuristically
    cols = [str(c).lower() for c in df.columns]
    df.columns = cols
    # look for date column and number columns
    date_col = None
    for c in cols:
        if df[c].astype(str).str.contains(r"\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4}", na=False).any():
            date_col = c
            break
    if date_col is None and "ngay" in cols:
        date_col = "ngay"
    # find number columns n1..n6
    num_cols = [c for c in cols if re.match(r"n\d", c)]
    if len(num_cols) < 6:
        # try fallback: assume next 6 cols are numbers
        pass
    rows = []
    for _, r in df.iterrows():
        try:
            date_raw = str(r.get(date_col,""))
            date = normalize_date(date_raw)
            if not date:
                continue
            nums = []
            # collect up to 6 numeric values from row
            for v in r.values:
                s = str(v)
                ss = re.findall(r"\d+", s)
                for x in ss:
                    nums.append(int(x))
                if len(nums) >= 6:
                    break
            if len(nums) < 6:
                continue
            nums = nums[:6]
            nums = sorted(nums)
            rec = {"date":date, "n1":nums[0],"n2":nums[1],"n3":nums[2],
                  "n4":nums[3],"n5":nums[4],"n6":nums[5], "source":"table"}
            rows.append(rec)
        except Exception:
            continue
    return pd.DataFrame(rows)

# Merge multiple sources
def merge_dfs(dfs, limit=None):
    dfs = [d for d in dfs if d is not None and not d.empty]
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    # normalize date string
    if "date" in df.columns:
        df["date"] = df["date"].astype(str).apply(lambda x: normalize_date(x))
        df = df.dropna(subset=["date"])
    df = df.drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"])
    df = df.sort_values("date", ascending=False).reset_index(drop=True)
    if limit:
        df = df.head(limit).reset_index(drop=True)
    return df

# Public API — fetch_all_data (backward compatible)
def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)
    log("🔹 Fetching data (no selenium)...")

    mega_dfs = []
    for url in MEGA_URLS:
        log(f"🔹 Fetching Mega: {url}")
        html = get_html(url)
        if not html:
            continue
        if "ketquadientoan" in url:
            mega_dfs.append(parse_mega_ketquad(html))
        else:
            mega_dfs.append(parse_table_html(html, mega=True))

    power_dfs = []
    for url in POWER_URLS:
        log(f"🔹 Fetching Power: {url}")
        html = get_html(url)
        if not html:
            continue
        if "ketquadientoan" in url:
            power_dfs.append(parse_power_ketquad(html))
        else:
            power_dfs.append(parse_table_html(html, mega=False))

    mega_df = merge_dfs(mega_dfs, limit=limit)
    power_df = merge_dfs(power_dfs, limit=limit)

    # Save raw copies
    try:
        mega_df.to_csv(os.path.join(save_dir, "mega_6_45_raw.csv"), index=False)
        power_df.to_csv(os.path.join(save_dir, "power_6_55_raw.csv"), index=False)
    except Exception as e:
        log(f"⚠ Failed to save raw CSV: {e}")

    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")
    return mega_df, power_df
    
with open(f"debug_{int(time.time())}.html","w",encoding="utf8") as f:
    f.write(html)
