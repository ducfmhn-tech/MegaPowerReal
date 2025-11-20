# utils/fetch_data.py
"""
Parser chuyên biệt cho các nguồn:
 - ketquadientoan.com
 - minhngoc.net.vn
 - lotto-8.com

Mục tiêu:
 - Không dùng pandas.read_html()
 - Dùng BeautifulSoup để bóc chính xác ngày + 6 số mỗi kỳ
 - Trả về DataFrame chuẩn: draw_date (datetime), n1..n6 (Int64)
 - Retry, timeout, fallback an toàn
"""

from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
from datetime import datetime
import time
from utils.logger import log

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/123.0 Safari/537.36"
}
TIMEOUT = 30
RETRIES = 3
SLEEP_BETWEEN = 1.0


# ---------------------------
# Helpers chung
# ---------------------------
def _to_int_safe(x):
    try:
        return int(x)
    except Exception:
        return pd.NA


def _clean_number_str(s):
    """Chuẩn hóa chuỗi, lấy chỉ các chữ số và dấu phân tách."""
    if s is None:
        return ""
    return re.sub(r'[^0-9\s,.-]', ' ', str(s)).strip()


def _parse_date_flexible(s):
    """Thử parse nhiều định dạng phổ biến (d/m/Y, Y-m-d, d-m-Y)."""
    if s is None:
        return pd.NaT
    s = str(s).strip()
    # Common patterns
    patterns = [
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%Y/%m/%d",
    ]
    for p in patterns:
        try:
            return datetime.strptime(s, p)
        except Exception:
            continue
    # Try regex find dd/mm/yyyy inside string
    m = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%d/%m/%Y")
        except:
            pass
    # Try digits only like YYYYMMDD
    m2 = re.search(r'(\d{4})(\d{2})(\d{2})', s)
    if m2:
        try:
            return datetime(int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
        except:
            pass
    return pd.NaT


# ---------------------------
# Parser cho ketquadientoan.com
# (website cấu trúc nhiều dạng; ta dò tìm các block chứa ngày + dãy số)
# ---------------------------
def parse_ketquadientoan(html):
    soup = BeautifulSoup(html, "lxml")
    results = []

    # Nhiều trang có danh sách kết quả nằm trong <div class="box"> hoặc <ul>...
    # Tìm mọi đoạn chứa chuỗi số (các số từ 0-99) và ngày kèm theo
    # Chiến lược: tìm các block <li>, <tr>, <div> có pattern 6 số
    candidates = soup.find_all(['li', 'tr', 'div', 'p', 'article', 'section'])

    for tag in candidates:
        text = tag.get_text(" ", strip=True)
        # tìm ngày (dd/mm/yyyy)
        date_match = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', text)
        # tìm dãy 6 số (cách nhau bởi space hoặc , hoặc -)
        nums = re.findall(r'\b\d{1,2}\b', text)
        if date_match and len(nums) >= 6:
            # dùng first 6 numbers found in that block (giả sử đúng)
            date_str = date_match.group(0)
            dt = _parse_date_flexible(date_str)
            if pd.isna(dt):
                continue
            # choose first six numbers that are reasonable (1..99)
            chosen = [int(x) for x in nums if 1 <= int(x) <= 99][:6]
            if len(chosen) == 6:
                results.append({
                    "draw_date": dt,
                    "n1": chosen[0],
                    "n2": chosen[1],
                    "n3": chosen[2],
                    "n4": chosen[3],
                    "n5": chosen[4],
                    "n6": chosen[5],
                })
    # Nếu không thấy bằng trên, try khác: tìm bảng có class chứa "table" hoặc "ket-qua"
    if not results:
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for r in rows:
                cells = [c.get_text(" ", strip=True) for c in r.find_all(['td','th'])]
                if not cells:
                    continue
                combined = " ".join(cells)
                date_match = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', combined)
                nums = re.findall(r'\b\d{1,2}\b', combined)
                if date_match and len(nums) >= 6:
                    dt = _parse_date_flexible(date_match.group(0))
                    if pd.isna(dt):
                        continue
                    chosen = [int(x) for x in nums if 1 <= int(x) <= 99][:6]
                    if len(chosen) == 6:
                        results.append({
                            "draw_date": dt,
                            "n1": chosen[0],
                            "n2": chosen[1],
                            "n3": chosen[2],
                            "n4": chosen[3],
                            "n5": chosen[4],
                            "n6": chosen[5],
                        })

    # Deduplicate by date
    if not results:
        return pd.DataFrame(columns=["draw_date","n1","n2","n3","n4","n5","n6"])

    df = pd.DataFrame(results)
    df = df.sort_values("draw_date", ascending=False).drop_duplicates(subset=["draw_date","n1","n2","n3","n4","n5","n6"])
    return df


# ---------------------------
# Parser cho minhngoc.net.vn (cấu trúc khá rõ: bảng kết quả hoặc div list)
# ---------------------------
def parse_minhngoc(html):
    soup = BeautifulSoup(html, "lxml")
    results = []

    # Tìm các row trong table nếu có
    tables = soup.find_all("table")
    for table in tables:
        # tìm hàng <tr> có ít nhất 7 cột (ngày + 6 số)
        for tr in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(['td','th'])]
            if not cells:
                continue
            combined = " ".join(cells)
            # date
            date_match = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', combined)
            nums = re.findall(r'\b\d{1,2}\b', combined)
            if date_match and len(nums) >= 6:
                dt = _parse_date_flexible(date_match.group(0))
                if pd.isna(dt):
                    continue
                chosen = [int(x) for x in nums if 1 <= int(x) <= 99][:6]
                if len(chosen) == 6:
                    results.append({
                        "draw_date": dt,
                        "n1": chosen[0],
                        "n2": chosen[1],
                        "n3": chosen[2],
                        "n4": chosen[3],
                        "n5": chosen[4],
                        "n6": chosen[5],
                    })

    # Nếu không có table hợp lệ, tìm các block li
    if not results:
        lis = soup.find_all('li')
        for li in lis:
            text = li.get_text(" ", strip=True)
            date_match = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', text)
            nums = re.findall(r'\b\d{1,2}\b', text)
            if date_match and len(nums) >= 6:
                dt = _parse_date_flexible(date_match.group(0))
                if pd.isna(dt):
                    continue
                chosen = [int(x) for x in nums if 1 <= int(x) <= 99][:6]
                if len(chosen) == 6:
                    results.append({
                        "draw_date": dt,
                        "n1": chosen[0],
                        "n2": chosen[1],
                        "n3": chosen[2],
                        "n4": chosen[3],
                        "n5": chosen[4],
                        "n6": chosen[5],
                    })

    if not results:
        return pd.DataFrame(columns=["draw_date","n1","n2","n3","n4","n5","n6"])

    df = pd.DataFrame(results)
    df = df.sort_values("draw_date", ascending=False).drop_duplicates(subset=["draw_date","n1","n2","n3","n4","n5","n6"])
    return df


# ---------------------------
# Parser cho lotto-8.com (thô, đôi khi mã hóa charset)
# ---------------------------
def parse_lotto8(html):
    # ensure correct encoding by reparsing
    soup = BeautifulSoup(html, "lxml")
    results = []

    # Many lotto-8 pages have rows like: "<tr><td>01/01/2025</td><td>01 02 03 04 05 06</td></tr>"
    for tr in soup.find_all("tr"):
        tds = tr.find_all(["td","th"])
        if not tds:
            continue
        text = " ".join([td.get_text(" ", strip=True) for td in tds])
        date_match = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', text)
        nums = re.findall(r'\b\d{1,2}\b', text)
        if date_match and len(nums) >= 6:
            dt = _parse_date_flexible(date_match.group(0))
            if pd.isna(dt):
                continue
            chosen = [int(x) for x in nums if 1 <= int(x) <= 99][:6]
            if len(chosen) == 6:
                results.append({
                    "draw_date": dt,
                    "n1": chosen[0],
                    "n2": chosen[1],
                    "n3": chosen[2],
                    "n4": chosen[3],
                    "n5": chosen[4],
                    "n6": chosen[5],
                })

    # fallback: search any block containing 6 numbers + date
    if not results:
        tags = soup.find_all(['p','div','li'])
        for tag in tags:
            text = tag.get_text(" ", strip=True)
            date_match = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', text)
            nums = re.findall(r'\b\d{1,2}\b', text)
            if date_match and len(nums) >= 6:
                dt = _parse_date_flexible(date_match.group(0))
                if pd.isna(dt):
                    continue
                chosen = [int(x) for x in nums if 1 <= int(x) <= 99][:6]
                if len(chosen) == 6:
                    results.append({
                        "draw_date": dt,
                        "n1": chosen[0],
                        "n2": chosen[1],
                        "n3": chosen[2],
                        "n4": chosen[3],
                        "n5": chosen[4],
                        "n6": chosen[5],
                    })

    if not results:
        return pd.DataFrame(columns=["draw_date","n1","n2","n3","n4","n5","n6"])

    df = pd.DataFrame(results)
    df = df.sort_values("draw_date", ascending=False).drop_duplicates(subset=["draw_date","n1","n2","n3","n4","n5","n6"])
    return df


# ---------------------------
# Dispatch parser theo hostname / url pattern
# ---------------------------
def _select_and_parse(url, html):
    url_l = url.lower()
    if "ketquadientoan" in url_l:
        return parse_ketquadientoan(html)
    if "minhngoc" in url_l:
        return parse_minhngoc(html)
    if "lotto-8" in url_l or "lotto8" in url_l:
        return parse_lotto8(html)
    # default: try common parsers
    df = parse_minhngoc(html)
    if not df.empty:
        return df
    df = parse_lotto8(html)
    if not df.empty:
        return df
    df = parse_ketquadientoan(html)
    return df


# ---------------------------
# Fetch URL with retry + parse
# ---------------------------
def fetch_one_source(url, timeout=TIMEOUT, retries=RETRIES):
    log(f"🔹 Fetching {url} ...")
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            # ensure we have text
            html = r.text
            df = _select_and_parse(url, html)
            if df is None or df.empty:
                log(f"⚠ Bảng không hợp lệ trên {url} (attempt {attempt})")
                time.sleep(SLEEP_BETWEEN)
                continue
            # ensure columns standardized
            expected = ["draw_date","n1","n2","n3","n4","n5","n6"]
            df = df.loc[:, [c for c in expected if c in df.columns]]
            # coerce types
            for c in expected:
                if c in df.columns and c != "draw_date":
                    df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
            # drop rows with missing values
            df = df.dropna(subset=expected)
            if df.empty:
                log(f"⚠ Parsed but no valid rows for {url} (attempt {attempt})")
                time.sleep(SLEEP_BETWEEN)
                continue
            # success
            log(f"✔ Fetched {len(df)} rows from {url}")
            return df
        except Exception as e:
            log(f"❌ Lỗi fetch {url} (attempt {attempt}): {e}")
            time.sleep(SLEEP_BETWEEN)
    log(f"❌ Bỏ qua {url} sau {retries} lần thử.")
    return pd.DataFrame(columns=["draw_date","n1","n2","n3","n4","n5","n6"])


# ---------------------------
# Fetch all sources and merge (Mega or Power)
# ---------------------------
def fetch_all_sources(urls, limit=400):
    REQUIRED = ["draw_date","n1","n2","n3","n4","n5","n6"]
    rows = []
    log(f"==== BẮT ĐẦU FETCH {len(urls)} NGUỒN ====")
    for url in urls:
        df = fetch_one_source(url)
        if df is None or df.empty:
            log(f"⚠ Không lấy được dữ liệu từ {url}")
            continue
        # keep only required columns
        missing = [c for c in REQUIRED if c not in df.columns]
        if missing:
            log(f"⚠ Bỏ qua {url} vì thiếu cột {missing}")
            continue
        df = df[REQUIRED]
        rows.append(df)
    if not rows:
        log("❌ Không có dữ liệu hợp lệ từ bất kỳ nguồn nào!")
        return pd.DataFrame(columns=REQUIRED)
    out = pd.concat(rows, ignore_index=True)
    # dedupe by draw_date + numbers
    try:
        out = out.drop_duplicates(subset=REQUIRED)
    except Exception as e:
        log(f"⚠ Không thể drop duplicates: {e}")
    # parse draw_date to datetime if not already
    try:
        out["draw_date"] = pd.to_datetime(out["draw_date"], errors="coerce")
    except:
        pass
    out = out.sort_values("draw_date", ascending=False)
    out = out.head(limit).reset_index(drop=True)
    log(f"📌 Fetch xong: {len(out)} rows hợp lệ")
    return out
    
