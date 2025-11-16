# utils/fetch_data.py
"""
Robust fetcher for Mega 6/45 and Power 6/55.
Supports multiple sources and extracts:
 - date (ISO YYYY-MM-DD)
 - draw_id (if available)
 - jackpot (if available)
 - n1..n6 sorted ascending
 - source_url
Saves CSVs:
 - data/mega_6_45_raw.csv
 - data/power_6_55_raw.csv
"""

import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from utils.logger import log

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


# ---------------------------
# Helpers
# ---------------------------
def _safe_get_text(elem):
    try:
        return elem.get_text(strip=True)
    except:
        return str(elem)


def normalize_date(text):
    """Normalize many date formats to YYYY-MM-DD or return None."""
    if not isinstance(text, str):
        return None
    s = text.strip()
    # Remove weekday names, Vietnamese prefixes like "Thứ", "CN"
    s = re.sub(r"^(Thứ|Thu|CN|Chủ nhật|Thứ)\s*\S*,?\s*", "", s, flags=re.I).strip()
    s = s.replace(".", "/").replace("-", "/")
    # try common formats
    fmts = ["%d/%m/%Y", "%Y/%m/%d", "%Y-%m-%d", "%d-%m-%Y", "%d %m %Y"]
    for f in fmts:
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except:
            pass
    # try pandas parser
    try:
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except:
        return None


def extract_numbers_from_string(s, want_count=6):
    """Return sorted list of first want_count ints found in string, or None."""
    if not isinstance(s, str):
        return None
    nums = re.findall(r"\d+", s)
    nums = [int(x) for x in nums]
    if len(nums) < want_count:
        return None
    res = sorted(nums[:want_count])
    return res


def fetch_html(url, retry=3, wait=1.0):
    for i in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
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


# ---------------------------
# Parsers per-site (Mega)
# ---------------------------
def parse_ketquadientoan_mega(html, limit=120):
    """
    ketquadientoan often has blocks containing date and numbers,
    but structure may vary. Use both BS selectors and fallback regex.
    """
    soup = BeautifulSoup(html, "lxml")
    results = []

    # Approach A: table rows
    try:
        # find tables and rows
        tables = soup.find_all("table")
        for tbl in tables:
            for tr in tbl.find_all("tr"):
                text = " ".join([_safe_get_text(td) for td in tr.find_all(["td", "th"])])
                if not text:
                    continue
                nums = extract_numbers_from_string(text, 6)
                if not nums:
                    continue
                # find date token in row
                date_token = None
                for token in re.split(r"\s{2,}|\|", text):
                    d = normalize_date(token)
                    if d:
                        date_token = d
                        break
                # fallback: search within the row text for date-like pattern
                if not date_token:
                    m = re.search(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", text)
                    if m:
                        date_token = normalize_date(m.group(0))
                if not date_token:
                    continue
                results.append({
                    "date": date_token,
                    "draw_id": None,
                    "jackpot": None,
                    "n1": nums[0], "n2": nums[1], "n3": nums[2],
                    "n4": nums[3], "n5": nums[4], "n6": nums[5],
                    "source": "ketquadientoan"
                })
    except Exception as e:
        log(f"⚠ ketquadientoan_mega parse-table error: {e}")

    # Approach B: look for textual blocks
    try:
        blocks = soup.find_all(["div", "p", "li"])
        for b in blocks:
            txt = _safe_get_text(b)
            if len(txt) < 10:
                continue
            nums = extract_numbers_from_string(txt, 6)
            if not nums:
                continue
            # date
            d = None
            # try immediate siblings or parents
            # check preceding text
            prev = b.find_previous(string=True)
            if prev:
                d = normalize_date(prev.strip())
            # fallback search in block
            if not d:
                m = re.search(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", txt)
                if m:
                    d = normalize_date(m.group(0))
            if not d:
                continue
            results.append({
                "date": d,
                "draw_id": None,
                "jackpot": None,
                "n1": nums[0], "n2": nums[1], "n3": nums[2],
                "n4": nums[3], "n5": nums[4], "n6": nums[5],
                "source": "ketquadientoan"
            })
    except Exception as e:
        log(f"⚠ ketquadientoan_mega parse-block error: {e}")

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results).drop_duplicates()
    # ensure date normalized
    df["date"] = df["date"].apply(lambda x: normalize_date(x))
    df = df.dropna(subset=["date"])
    df = df.sort_values("date", ascending=False).head(limit).reset_index(drop=True)
    return df


def parse_minhngoc_mega(html, limit=120):
    soup = BeautifulSoup(html, "lxml")
    results = []

    # minhngoc often uses tables: try pandas.read_html fallback
    try:
        dfs = pd.read_html(html)
        for df in dfs:
            # join row cells to single text
            for _, row in df.iterrows():
                rowtxt = " ".join(map(str, row.values))
                nums = extract_numbers_from_string(rowtxt, 6)
                if not nums:
                    continue
                # try find date cell
                date_cell = None
                for val in row.values:
                    d = normalize_date(str(val))
                    if d:
                        date_cell = d
                        break
                if not date_cell:
                    # regex fallback
                    m = re.search(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", rowtxt)
                    if m:
                        date_cell = normalize_date(m.group(0))
                if not date_cell:
                    continue
                results.append({
                    "date": date_cell,
                    "draw_id": None,
                    "jackpot": None,
                    "n1": nums[0], "n2": nums[1], "n3": nums[2],
                    "n4": nums[3], "n5": nums[4], "n6": nums[5],
                    "source": "minhngoc"
                })
    except Exception as e:
        log(f"⚠ minhngoc_mega read_html error: {e}")

    # fallback HTML scan
    try:
        for tr in soup.find_all("tr"):
            txt = " ".join([_safe_get_text(td) for td in tr.find_all(["td", "th"])])
            nums = extract_numbers_from_string(txt, 6)
            if not nums:
                continue
            date_token = None
            for tok in re.split(r"\s{2,}|\|", txt):
                d = normalize_date(tok)
                if d:
                    date_token = d
                    break
            if not date_token:
                m = re.search(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", txt)
                if m:
                    date_token = normalize_date(m.group(0))
            if not date_token:
                continue
            results.append({
                "date": date_token,
                "draw_id": None,
                "jackpot": None,
                "n1": nums[0], "n2": nums[1], "n3": nums[2],
                "n4": nums[3], "n5": nums[4], "n6": nums[5],
                "source": "minhngoc"
            })
    except Exception as e:
        log(f"⚠ minhngoc_mega parse-tr error: {e}")

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results).drop_duplicates()
    df["date"] = df["date"].apply(lambda x: normalize_date(x))
    df = df.dropna(subset=["date"])
    df = df.sort_values("date", ascending=False).head(limit).reset_index(drop=True)
    return df


def parse_lotto8_mega(html, limit=120):
    # lotto-8 has table rows with date and numbers
    soup = BeautifulSoup(html, "lxml")
    results = []
    try:
        for tr in soup.find_all("tr"):
            cols = tr.find_all("td")
            if not cols or len(cols) < 2:
                continue
            text = " ".join([_safe_get_text(c) for c in cols])
            nums = extract_numbers_from_string(text, 6)
            if not nums:
                continue
            # date usually in first col
            date_token = normalize_date(_safe_get_text(cols[0]))
            if not date_token:
                # try anywhere in text
                m = re.search(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", text)
                if m:
                    date_token = normalize_date(m.group(0))
            if not date_token:
                continue
            results.append({
                "date": date_token,
                "draw_id": None,
                "jackpot": None,
                "n1": nums[0], "n2": nums[1], "n3": nums[2],
                "n4": nums[3], "n5": nums[4], "n6": nums[5],
                "source": "lotto8"
            })
    except Exception as e:
        log(f"⚠ lotto8_mega parse error: {e}")

    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results).drop_duplicates()
    df["date"] = df["date"].apply(lambda x: normalize_date(x))
    df = df.dropna(subset=["date"])
    df = df.sort_values("date", ascending=False).head(limit).reset_index(drop=True)
    return df


# ---------------------------
# Parsers per-site (Power)
# ---------------------------
def parse_ketquadientoan_power(html, limit=120):
    # Similar approach to mega but allow numbers up to 55
    soup = BeautifulSoup(html, "lxml")
    results = []

    try:
        tables = soup.find_all("table")
        for tbl in tables:
            for tr in tbl.find_all("tr"):
                text = " ".join([_safe_get_text(td) for td in tr.find_all(["td", "th"])])
                nums = extract_numbers_from_string(text, 6)
                if not nums:
                    continue
                # ensure numbers <= 55
                if max(nums) > 55:
                    continue
                date_token = None
                for token in re.split(r"\s{2,}|\|", text):
                    d = normalize_date(token)
                    if d:
                        date_token = d
                        break
                if not date_token:
                    m = re.search(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", text)
                    if m:
                        date_token = normalize_date(m.group(0))
                if not date_token:
                    continue
                # try extract jackpot (keyword 'jackpot' or 'tiền thưởng' or numbers with currency)
                jackpot = None
                m_j = re.search(r"jackpot[:\s]*([\d,.]+)", text, re.I)
                if m_j:
                    jackpot = m_j.group(1)
                results.append({
                    "date": date_token,
                    "draw_id": None,
                    "jackpot": jackpot,
                    "n1": nums[0], "n2": nums[1], "n3": nums[2],
                    "n4": nums[3], "n5": nums[4], "n6": nums[5],
                    "source": "ketquadientoan"
                })
    except Exception as e:
        log(f"⚠ ketquadientoan_power parse error: {e}")

    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results).drop_duplicates()
    df["date"] = df["date"].apply(lambda x: normalize_date(x))
    df = df.dropna(subset=["date"])
    df = df.sort_values("date", ascending=False).head(limit).reset_index(drop=True)
    return df


def parse_minhngoc_power(html, limit=120):
    # use pandas.read_html first
    results = []
    try:
        dfs = pd.read_html(html)
        for df in dfs:
            for _, row in df.iterrows():
                txt = " ".join(map(str, row.values))
                nums = extract_numbers_from_string(txt, 6)
                if not nums:
                    continue
                # skip if >55
                if max(nums) > 55:
                    continue
                date_cell = None
                for v in row.values:
                    d = normalize_date(str(v))
                    if d:
                        date_cell = d
                        break
                if not date_cell:
                    m = re.search(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", txt)
                    if m:
                        date_cell = normalize_date(m.group(0))
                if not date_cell:
                    continue
                jackpot = None
                mj = re.search(r"jackpot[:\s]*([\d,.]+)", txt, re.I)
                if mj:
                    jackpot = mj.group(1)
                results.append({
                    "date": date_cell,
                    "draw_id": None,
                    "jackpot": jackpot,
                    "n1": nums[0], "n2": nums[1], "n3": nums[2],
                    "n4": nums[3], "n5": nums[4], "n6": nums[5],
                    "source": "minhngoc"
                })
    except Exception as e:
        log(f"⚠ minhngoc_power read_html error: {e}")

    # fallback simple parse
    soup = BeautifulSoup(html, "lxml")
    try:
        for tr in soup.find_all("tr"):
            txt = " ".join([_safe_get_text(td) for td in tr.find_all(["td", "th"])])
            nums = extract_numbers_from_string(txt, 6)
            if not nums or max(nums) > 55:
                continue
            date_token = None
            m = re.search(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", txt)
            if m:
                date_token = normalize_date(m.group(0))
            if not date_token:
                continue
            results.append({
                "date": date_token,
                "draw_id": None,
                "jackpot": None,
                "n1": nums[0], "n2": nums[1], "n3": nums[2],
                "n4": nums[3], "n5": nums[4], "n6": nums[5],
                "source": "minhngoc"
            })
    except Exception as e:
        log(f"⚠ minhngoc_power parse-tr error: {e}")

    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results).drop_duplicates()
    df["date"] = df["date"].apply(lambda x: normalize_date(x))
    df = df.dropna(subset=["date"])
    df = df.sort_values("date", ascending=False).head(limit).reset_index(drop=True)
    return df


def parse_lotto8_power(html, limit=120):
    soup = BeautifulSoup(html, "lxml")
    results = []
    try:
        for tr in soup.find_all("tr"):
            cols = tr.find_all("td")
            if not cols or len(cols) < 2:
                continue
            text = " ".join([_safe_get_text(c) for c in cols])
            nums = extract_numbers_from_string(text, 6)
            if not nums or max(nums) > 55:
                continue
            date_token = normalize_date(_safe_get_text(cols[0]))
            if not date_token:
                m = re.search(r"\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}", text)
                if m:
                    date_token = normalize_date(m.group(0))
            if not date_token:
                continue
            results.append({
                "date": date_token,
                "draw_id": None,
                "jackpot": None,
                "n1": nums[0], "n2": nums[1], "n3": nums[2],
                "n4": nums[3], "n5": nums[4], "n6": nums[5],
                "source": "lotto8"
            })
    except Exception as e:
        log(f"⚠ lotto8_power parse error: {e}")

    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results).drop_duplicates()
    df["date"] = df["date"].apply(lambda x: normalize_date(x))
    df = df.dropna(subset=["date"])
    df = df.sort_values("date", ascending=False).head(limit).reset_index(drop=True)
    return df


# ---------------------------
# Public fetch_all_data
# ---------------------------
def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)
    all_mega = []
    all_power = []

    # Mega sources
    for url in MEGA_SOURCES:
        html = fetch_html(url)
        if not html:
            continue
        if "ketquadientoan" in url:
            df = parse_ketquadientoan_mega(html, limit=limit)
        elif "minhngoc" in url:
            df = parse_minhngoc_mega(html, limit=limit)
        elif "lotto-8" in url:
            df = parse_lotto8_mega(html, limit=limit)
        else:
            df = pd.DataFrame()

        if not df.empty:
            df["source_url"] = url
            all_mega.append(df)

    # Power sources
    for url in POWER_SOURCES:
        html = fetch_html(url)
        if not html:
            continue
        if "ketquadientoan" in url:
            df = parse_ketquadientoan_power(html, limit=limit)
        elif "minhngoc" in url:
            df = parse_minhngoc_power(html, limit=limit)
        elif "lotto-8" in url:
            df = parse_lotto8_power(html, limit=limit)
        else:
            df = pd.DataFrame()

        if not df.empty:
            df["source_url"] = url
            all_power.append(df)

    # merge & dedupe
    mega_df = pd.concat(all_mega, ignore_index=True) if all_mega else pd.DataFrame()
    power_df = pd.concat(all_power, ignore_index=True) if all_power else pd.DataFrame()

    def finalize(df, max_num):
        if df.empty:
            return df
        # trim to 6 numeric columns and normalize types
        for i in range(1, 7):
            df[f"n{i}"] = pd.to_numeric(df[f"n{i}"], errors="coerce").astype('Int64')
        df["date"] = df["date"].astype(str).apply(lambda x: normalize_date(x))
        df = df.dropna(subset=["date"])
        # ensure numbers within range
        for i in range(1, 7):
            df = df[df[f"n{i}"].notna()]
        if df.empty:
            return df
        # sort numbers inside each row
        def sort_row_nums(r):
            nums = [int(r[f"n{i}"]) for i in range(1, 7)]
            nums = sorted(nums)
            out = {f"n{i}": nums[i-1] for i in range(1, 7)}
            return pd.Series(out)
        nums_sorted = df.apply(sort_row_nums, axis=1)
        for i in range(1, 7):
            df[f"n{i}"] = nums_sorted[f"n{i}"]
        # drop exact duplicates (date + numbers)
        df = df.drop_duplicates(subset=["date", "n1", "n2", "n3", "n4", "n5", "n6"])
        # keep newest first
        df = df.sort_values("date", ascending=False).reset_index(drop=True)
        return df.head(limit)

    mega_df = finalize(mega_df, 45)
    power_df = finalize(power_df, 55)

    # save to CSV
    mega_path = os.path.join(save_dir, "mega_6_45_raw.csv")
    power_path = os.path.join(save_dir, "power_6_55_raw.csv")
    try:
        mega_df.to_csv(mega_path, index=False)
        power_df.to_csv(power_path, index=False)
        log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")
    except Exception as e:
        log(f"⚠ Error saving CSV: {e}")

    return mega_df, power_df


__all__ = ["fetch_all_data"]
