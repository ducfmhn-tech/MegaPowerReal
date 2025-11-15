# utils/fetch_data.py
import os, re, time, requests, json
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime
from utils.logger import log

HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
SAVE_DEBUG_HTML = True
TIMEOUT = 20
RETRY = 3

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

def normalize_date(text):
    if not isinstance(text, str): return None
    s = text.strip()
    s = re.sub(r'^[^\d]*,\s*', '', s)
    for fmt in ("%d/%m/%Y","%d-%m-%Y","%Y-%m-%d"):
        try: return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except: pass
    try: return pd.to_datetime(s, dayfirst=True).strftime("%Y-%m-%d")
    except: return None

def fetch_html(url, name=None, retry=RETRY, timeout=TIMEOUT):
    for i in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.encoding = r.apparent_encoding or "utf-8"
            r.raise_for_status()
            log(f"✔ Fetched HTML OK: {url}")
            return r.text
        except Exception as e:
            log(f"[{name or url}] fetch attempt {i+1}/{retry} failed: {e}")
            time.sleep(1.2)
    return None

def extract_6_numbers_from_text(txt, max_val):
    tokens = re.findall(r'\b\d{1,2}\b', txt)
    nums = [int(t) for t in tokens if 1 <= int(t) <= max_val]
    if len(nums) < 6: return None
    return sorted(nums[-6:])

def cleanup_df(df, max_val, limit):
    if df is None or df.empty: return pd.DataFrame()
    if 'date' in df.columns:
        df['date'] = df['date'].astype(str).apply(normalize_date)
        df = df.dropna(subset=['date'])
    for i in range(1,7):
        col = f"n{i}"
        if col not in df.columns: df[col] = pd.NA
    for i in range(1,7):
        col = f"n{i}"
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    df = df.dropna(subset=[f"n{i}" for i in range(1,7)]).reset_index(drop=True)
    ok = df[[f"n{i}" for i in range(1,7)]].apply(lambda col: col.between(1, max_val))
    # ok per row
    ok_rows = ok.all(axis=1)
    df = df[ok_rows].copy()
    def sort_row(r):
        arr = sorted([int(r[f"n{i}"]) for i in range(1,7)])
        return {f"n{i}": arr[i-1] for i in range(1,7)}
    sorted_cols = df.apply(sort_row, axis=1, result_type='expand')
    for i in range(1,7): df[f"n{i}"] = sorted_cols[f"n{i}"].astype('Int64')
    df = df.drop_duplicates().sort_values("date", ascending=False).head(limit).reset_index(drop=True)
    return df

# parsing functions: try div.item, then read_html, then tr-scan
def parse_mega_from_html(html, limit=120):
    if not html: return pd.DataFrame()
    soup = BeautifulSoup(html, "lxml")
    # div.item style
    items = soup.select(".item, .list-result .item, .box-result .item")
    rows=[]
    if items:
        for it in items:
            date_tag = it.select_one(".col-date, .ngay, .date, .kqdate, .list-date")
            date_txt = date_tag.get_text(strip=True) if date_tag else None
            date = normalize_date(date_txt) if date_txt else None
            sel_candidates = [".col-number .number", ".col-number .num", ".number", ".bong", ".roll_bong", ".so", ".result-number", ".kq"]
            found=[]
            for sel in sel_candidates:
                nodes = it.select(sel)
                if nodes:
                    found = [n.get_text(strip=True) for n in nodes if n.get_text(strip=True).strip()]
                    break
            extracted = extract_6_numbers_from_text(" ".join(found) if found else it.get_text(" ", strip=True), 45)
            if not extracted or not date: continue
            rows.append({"date": date, "n1":extracted[0],"n2":extracted[1],"n3":extracted[2],"n4":extracted[3],"n5":extracted[4],"n6":extracted[5]})
        if rows:
            return cleanup_df(pd.DataFrame(rows),45,limit)
    # read_html fallback
    try:
        dfs = pd.read_html(StringIO(html))
        for df in dfs:
            cand=[]
            for _, r in df.iterrows():
                text = " ".join([str(x) for x in r.values])
                nums = extract_6_numbers_from_text(text,45)
                if not nums: continue
                date_search = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', text)
                date = normalize_date(date_search.group()) if date_search else None
                if not date: continue
                cand.append({"date":date,"n1":nums[0],"n2":nums[1],"n3":nums[2],"n4":nums[3],"n5":nums[4],"n6":nums[5]})
            if cand: return cleanup_df(pd.DataFrame(cand),45,limit)
    except Exception as e:
        log(f"parse_mega read_html failed: {e}")
    # tr fallback
    rows=[]
    for tr in soup.find_all("tr"):
        text = " ".join(td.get_text(" ",strip=True) for td in tr.find_all(["td","th"]))
        nums = extract_6_numbers_from_text(text,45)
        if not nums: continue
        date_search = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', text)
        date = normalize_date(date_search.group()) if date_search else None
        if not date: continue
        rows.append({"date":date,"n1":nums[0],"n2":nums[1],"n3":nums[2],"n4":nums[3],"n5":nums[4],"n6":nums[5]})
    if rows: return cleanup_df(pd.DataFrame(rows),45,limit)
    return pd.DataFrame()

def parse_power_from_html(html, limit=120):
    if not html: return pd.DataFrame()
    soup = BeautifulSoup(html, "lxml")
    items = soup.select(".item, .list-result .item, .box-result .item")
    rows=[]
    if items:
        for it in items:
            date_tag = it.select_one(".col-date, .ngay, .date, .kqdate, .list-date")
            date_txt = date_tag.get_text(strip=True) if date_tag else None
            date = normalize_date(date_txt) if date_txt else None
            sel_candidates = [".col-number .number", ".col-number .num", ".number", ".bong", ".roll_bong", ".result-number"]
            found=[]
            for sel in sel_candidates:
                nodes = it.select(sel)
                if nodes:
                    found=[n.get_text(strip=True) for n in nodes if n.get_text(strip=True).strip()]
                    break
            extracted = extract_6_numbers_from_text(" ".join(found) if found else it.get_text(" ",strip=True),55)
            if not extracted or not date: continue
            rows.append({"date":date,"n1":extracted[0],"n2":extracted[1],"n3":extracted[2],"n4":extracted[3],"n5":extracted[4],"n6":extracted[5]})
        if rows: return cleanup_df(pd.DataFrame(rows),55,limit)
    try:
        dfs = pd.read_html(StringIO(html))
        for df in dfs:
            cand=[]
            for _, r in df.iterrows():
                text = " ".join([str(x) for x in r.values])
                nums = extract_6_numbers_from_text(text,55)
                if not nums: continue
                date_search = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', text)
                date = normalize_date(date_search.group()) if date_search else None
                if not date: continue
                cand.append({"date":date,"n1":nums[0],"n2":nums[1],"n3":nums[2],"n4":nums[3],"n5":nums[4],"n6":nums[5]})
            if cand: return cleanup_df(pd.DataFrame(cand),55,limit)
    except Exception as e:
        log(f"parse_power read_html failed: {e}")
    rows=[]
    for tr in soup.find_all("tr"):
        text = " ".join(td.get_text(" ",strip=True) for td in tr.find_all(["td","th"]))
        nums = extract_6_numbers_from_text(text,55)
        if not nums: continue
        date_search = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', text)
        date = normalize_date(date_search.group()) if date_search else None
        if not date: continue
        rows.append({"date":date,"n1":nums[0],"n2":nums[1],"n3":nums[2],"n4":nums[3],"n5":nums[4],"n6":nums[5]})
    if rows: return cleanup_df(pd.DataFrame(rows),55,limit)
    return pd.DataFrame()

def fetch_all_data(limit=120, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)
    mega_df = pd.DataFrame(); power_df = pd.DataFrame()
    # mega
    for url in MEGA_SOURCES:
        log("🔹 Fetching Mega: "+url)
        html = fetch_html(url, name="MEGA")
        if not html: continue
        if SAVE_DEBUG_HTML:
            open(os.path.join(save_dir,"debug_mega.html"),"w",encoding="utf-8").write(html)
        df = parse_mega_from_html(html, limit)
        if not df.empty:
            mega_df = df; break
    # power
    for url in POWER_SOURCES:
        log("🔹 Fetching Power: "+url)
        html = fetch_html(url, name="POWER")
        if not html: continue
        if SAVE_DEBUG_HTML:
            open(os.path.join(save_dir,"debug_power.html"),"w",encoding="utf-8").write(html)
        df = parse_power_from_html(html, limit)
        if not df.empty:
            power_df = df; break
    mega_df.to_csv(os.path.join(save_dir,"mega_6_45_raw.csv"), index=False)
    power_df.to_csv(os.path.join(save_dir,"power_6_55_raw.csv"), index=False)
    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")
    return mega_df, power_df
