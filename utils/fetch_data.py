# utils/fetch_data.py
import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from utils.logger import log

# Try to import selenium (may be absent locally)
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
except Exception:
    webdriver = None

HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/100 Safari/537.36"}

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

def normalize_date(text):
    if not isinstance(text, str):
        return None
    s = text.strip()
    s = re.sub(r"^(Thứ|Thu|CN|Chủ nhật)\s*,?", "", s, flags=re.I).strip()
    s = s.replace(".", "/").replace("-", "/")
    fmts = ["%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y", "%d %m %Y"]
    for f in fmts:
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except:
            pass
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except:
        return None

def extract_numbers_from_string(s, want=6):
    if not isinstance(s, str):
        return None
    nums = re.findall(r"\d+", s)
    nums = [int(x) for x in nums]
    if len(nums) < want:
        return None
    return sorted(nums[:want])

def fetch_html(url, retry=2, wait=1.0):
    for i in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.encoding = r.apparent_encoding or "utf-8"
            if r.status_code == 200 and r.text:
                log(f"✔ Fetched HTML OK: {url}")
                return r.text
            else:
                log(f"⚠ HTTP {r.status_code} for {url}")
        except Exception as e:
            log(f"⚠ Fetch error ({i+1}/{retry}) {url}: {e}")
        time.sleep(wait)
    return None

def try_selenium_fetch(url, timeout=20):
    if webdriver is None:
        log("⚠ Selenium not installed.")
        return None
    options = Options()
    try:
        options.add_argument("--headless=new")
    except Exception:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"user-agent={HEADERS['User-Agent']}")
    chromedriver_path = None
    for p in ["/usr/bin/chromedriver", "/usr/local/bin/chromedriver"]:
        if os.path.exists(p):
            chromedriver_path = p
            break
    try:
        if chromedriver_path:
            service = Service(chromedriver_path)
            driver = webdriver.Chrome(service=service, options=options)
        else:
            driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(timeout)
        driver.get(url)
        time.sleep(2)
        html = driver.page_source
        driver.quit()
        log(f"✔ Selenium fetched (rendered) {url}")
        return html
    except Exception as e:
        log(f"⚠ Selenium fetch failed for {url}: {e}")
        try:
            driver.quit()
        except:
            pass
        return None

from bs4 import BeautifulSoup

def parse_generic_table(html, max_num=55, limit=120, source_name=None):
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for tr in soup.find_all("tr"):
        t = " ".join(td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"]))
        if not t:
            continue
        nums = extract_numbers_from_string(t, 6)
        if not nums:
            continue
        if max(nums) > max_num:
            continue
        m = re.search(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", t)
        date = normalize_date(m.group(0)) if m else None
        if not date:
            parent_text = tr.get_text(" ", strip=True)
            m2 = re.search(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", parent_text)
            if m2:
                date = normalize_date(m2.group(0))
        if not date:
            continue
        rows.append({
            "date": date,
            "draw_id": None,
            "jackpot": None,
            "n1": nums[0], "n2": nums[1], "n3": nums[2],
            "n4": nums[3], "n5": nums[4], "n6": nums[5],
            "source": source_name or "generic"
        })
        if len(rows) >= limit:
            break
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"])
    df["date"] = df["date"].apply(normalize_date)
    df = df.sort_values("date", ascending=False).reset_index(drop=True)
    return df.head(limit)

def fetch_from_source(url, is_power=False, limit=120):
    html = fetch_html(url)
    parsed = pd.DataFrame()
    if html:
        parsed = parse_generic_table(html, max_num=(55 if is_power else 45), limit=limit, source_name=url)
    if parsed.empty:
        log(f"ℹ No result from requests parser for {url}. Trying Selenium fallback...")
        html2 = try_selenium_fetch(url)
        if html2:
            parsed = parse_generic_table(html2, max_num=(55 if is_power else 45), limit=limit, source_name=url)
            if parsed.empty:
                log(f"⚠ Selenium parsed but no rows for {url}")
        else:
            log(f"⚠ Selenium fallback failed for {url}")
         
    return parsed

def finalize_df(df, limit=120):
    if df.empty:
        return df
    for i in range(1,7):
        df[f"n{i}"] = pd.to_numeric(df.get(f"n{i}"), errors="coerce").astype('Int64')
    df = df.dropna(subset=["date"])
    for i in range(1,7):
        df = df[df[f"n{i}"].notna()]
    if df.empty:
        return df
    def sort_row(r):
        nums = sorted([int(r[f"n{i}"]) for i in range(1,7)])
        return pd.Series({f"n{i}": nums[i-1] for i in range(1,7)})
    nums_sorted = df.apply(sort_row, axis=1)
    for i in range(1,7):
        df[f"n{i}"] = nums_sorted[f"n{i}"]
    df = df.drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"])
    df["date"] = df["date"].apply(normalize_date)
    df = df.sort_values("date", ascending=False).reset_index(drop=True)
    return df.head(limit)

def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)
    mega_frames = []
    power_frames = []

    for url in MEGA_SOURCES:
        df = fetch_from_source(url, is_power=False, limit=limit)
        if not df.empty:
            df["source_url"] = url
            mega_frames.append(df)
    for url in POWER_SOURCES:
        df = fetch_from_source(url, is_power=True, limit=limit)
        if not df.empty:
            df["source_url"] = url
            power_frames.append(df)

    mega_df = pd.concat(mega_frames, ignore_index=True) if mega_frames else pd.DataFrame()
    power_df = pd.concat(power_frames, ignore_index=True) if power_frames else pd.DataFrame()

    mega_df = finalize_df(mega_df, limit=limit)
    power_df = finalize_df(power_df, limit=limit)

    mega_path = os.path.join(save_dir, "mega_6_45_raw.csv")
    power_path = os.path.join(save_dir, "power_6_55_raw.csv")
    try:
        mega_df.to_csv(mega_path, index=False)
        power_df.to_csv(power_path, index=False)
        log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")
    except Exception as e:
        log(f"⚠ Error saving CSVs: {e}")

    return mega_df, power_df

__all__ = ["fetch_all_data"]
