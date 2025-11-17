# utils/fetch_data.py
import requests
import pandas as pd
from io import StringIO
from time import sleep

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def normalize_columns(df):
    """Chuẩn hóa tên cột để không lỗi khi là MultiIndex."""
    if isinstance(df.columns, pd.MultiIndex):
        # flatten MultiIndex -> "col1_col2"
        df.columns = [
            "_".join([str(c).strip() for c in col if str(c).strip() != ""])
            for col in df.columns
        ]
    else:
        df.columns = [str(c).strip() for c in df.columns]
    return df

def fetch_html(url, timeout_sec=30, retries=3, wait=1):
    """Fetch HTML with retry. Return text or None."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout_sec)
            r.raise_for_status()
            r.encoding = 'utf-8'
            return r.text
        except Exception as e:
            print(f"❌ Lỗi fetch {url} (attempt {attempt}): {e}")
            sleep(wait)
    return None

def parse_table(html, url):
    from io import StringIO
    import pandas as pd

    try:
        tables = pd.read_html(StringIO(html), flavor="lxml")
    except Exception as e:
        print(f"❌ pd.read_html error ({url}): {e}")
        return pd.DataFrame()

    if len(tables) == 0:
        print(f"⚠ Không tìm thấy bảng nào trong {url}")
        return pd.DataFrame()

    # Ưu tiên bảng lớn nhất
    df = max(tables, key=lambda t: len(t))

    # --- FIX LỖI MULTIINDEX ---
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join([str(c).strip() for c in col if str(c).strip() != ""])
            for col in df.columns
        ]
    else:
        df.columns = [str(c).strip() for c in df.columns]

    # Loại bỏ các dòng trống
    df = df.dropna(how="all")

    return df
    
    # Choose table: prefer table index 2 if many tables, else largest table
    if len(tables) >= 3:
        df = tables[2]
    else:
        df = max(tables, key=lambda x: x.shape[0], default=pd.DataFrame())

    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df = normalize_columns(df)
    
    # Heuristic to find date column
    date_col = None
    for c in df.columns:
        low = c.lower()
        if "ngày" in low or "date" in low or "time" in low:
            date_col = c
            break
    if date_col is None:
        date_col = df.columns[0]

    # Rename to draw_date
    df = df.rename(columns={date_col: "draw_date"})

    # Heuristic to find numbers column: search columns containing at least 6 numbers in a cell
    nums_col = None
    for c in df.columns:
        try:
            sample = df[c].astype(str).head(20).tolist()
        except Exception:
            sample = []
        matches = sum(1 for s in sample if len([t for t in s.replace('-', ' ').replace(',', ' ').split() if t.isdigit()]) >= 6)
        if matches >= 1:
            nums_col = c
            break

    # If not found, assume second column
    if nums_col is None and len(df.columns) >= 2:
        nums_col = df.columns[1]
    if nums_col is None:
        print(f"⚠ Không tìm thấy cột số tại {url}")
        return pd.DataFrame()

    def extract_nums(x):
        if pd.isna(x):
            return [None]*6
        s = str(x)
        parts = s.replace('-', ' ').replace(',', ' ').split()
        nums = [int(p) for p in parts if p.isdigit()]
        if len(nums) >= 6:
            return nums[:6]
        # try to parse digits inside punctuation
        import re
        nums2 = re.findall(r'\d+', s)
        nums2 = [int(n) for n in nums2]
        if len(nums2) >= 6:
            return nums2[:6]
        # fallback None
        return [None]*6

    # apply extraction
    nums_df = df[nums_col].apply(lambda v: pd.Series(extract_nums(v), index=["n1","n2","n3","n4","n5","n6"]))
    df = pd.concat([df, nums_df], axis=1)

    # parse draw_date
    df["draw_date"] = pd.to_datetime(df["draw_date"], dayfirst=True, errors="coerce")
    df = df.dropna(subset=["draw_date"])

    # convert n1..n6 to integers where possible
    for i in range(1,7):
        col = f"n{i}"
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # keep only draw_date and n1..n6
    result = df[["draw_date","n1","n2","n3","n4","n5","n6"]].copy()
    return result

def fetch_all_sources(urls, limit=120):
    """Fetch multiple URLs and merge results. Return latest `limit` rows."""
    out = pd.DataFrame()
    for url in urls:
        print(f"🔹 Fetching {url} ...")
        html = fetch_html(url)
        df = parse_table(html, url)
        print(f"✔ Fetched {len(df)} rows from {url}")
        if not df.empty:
            out = pd.concat([out, df], ignore_index=True)
    if out.empty:
        print("⚠ Không có dữ liệu hợp lệ từ các nguồn.")
        return out
    out = out.drop_duplicates(subset=["draw_date","n1","n2","n3","n4","n5","n6"])
    out = out.sort_values(by="draw_date", ascending=False).reset_index(drop=True)
    return out.head(limit)
