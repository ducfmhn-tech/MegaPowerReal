import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from utils.logger import log

# -------------------------------------------------------------
# Cấu hình request
# -------------------------------------------------------------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/123.0 Safari/537.36"
}

TIMEOUT = 30


# -------------------------------------------------------------
# Chuẩn hóa bảng về dạng (draw_date, n1..n6)
# -------------------------------------------------------------
def normalize_dataframe(df, url):
    df.columns = df.columns.map(lambda x: str(x).strip())

    # Tự động tìm cột ngày
    date_cols = [c for c in df.columns if "ngày" in c.lower() or "date" in c.lower()]
    if not date_cols:
        # Một số website không có header → cột 0 là ngày
        date_col = df.columns[0]
    else:
        date_col = date_cols[0]

    # Lấy 7 cột đầu nếu không rõ
    df = df.iloc[:, :7]
    df = df.copy()
    df.columns = ["draw_date", "n1", "n2", "n3", "n4", "n5", "n6"]

    # Chuẩn hóa ngày
    try:
        df["draw_date"] = df["draw_date"].astype(str)
        df["draw_date"] = df["draw_date"].str.extract(r"(\d{1,2}/\d{1,2}/\d{4})")[0]
        df["draw_date"] = pd.to_datetime(df["draw_date"], format="%d/%m/%Y", errors="coerce")
    except:
        pass

    # Ép số
    for col in ["n1", "n2", "n3", "n4", "n5", "n6"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df = df.dropna(subset=["draw_date", "n1", "n2", "n3", "n4", "n5", "n6"])

    log(f"✔ Chuẩn hóa thành công bảng từ {url}: {len(df)} rows")
    return df


# -------------------------------------------------------------
# Parse bảng HTML → DataFrame (không lỗi MultiIndex)
# -------------------------------------------------------------
def parse_table(html, url):
    try:
        tables = pd.read_html(StringIO(html))
    except Exception as e:
        log(f"❌ Không đọc được bảng HTML từ {url}: {e}")
        return pd.DataFrame()

    if not tables:
        log(f"⚠ Không tìm thấy bảng HTML trên {url}")
        return pd.DataFrame()

    # Lấy bảng có nhiều dòng nhất
    df = max(tables, key=lambda t: len(t))

    if df.empty or len(df.columns) < 7:
        log(f"⚠ Bảng không hợp lệ trên {url}")
        return pd.DataFrame()

    return normalize_dataframe(df, url)


# -------------------------------------------------------------
# Fetch 1 nguồn
# -------------------------------------------------------------
def fetch_one_source(url, limit=200):
    log(f"🔹 Fetching {url} ...")

    html = None

    # Retry 3 lần
    for attempt in range(1, 4):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                html = r.text
                break
            else:
                log(f"⚠ Lỗi HTTP {r.status_code} ({url}) attempt {attempt}")
        except Exception as e:
            log(f"❌ Lỗi fetch {url} (attempt {attempt}): {e}")

    if not html:
        log(f"❌ Bỏ qua {url} vì không fetch được.")
        return pd.DataFrame()

    df = parse_table(html, url)

    if df.empty:
        log(f"⚠ Không lấy được dữ liệu từ {url}")
        return df

    df = df.sort_values("draw_date", ascending=False).head(limit)

    log(f"✔ Fetched {len(df)} rows from {url}")
    return df


# -------------------------------------------------------------
# Fetch toàn bộ nguồn Mega / Power
# -------------------------------------------------------------
def fetch_all_sources(urls, limit=200):
    all_rows = []

    REQUIRED = ["draw_date", "n1", "n2", "n3", "n4", "n5", "n6"]

    log(f"==== BẮT ĐẦU FETCH {len(urls)} NGUỒN ====")

    for url in urls:
        df = fetch_one_source(url, limit)
        if df is None or df.empty:
            continue

        # Đảm bảo cột đúng
        df.columns = df.columns.map(str)

        missing = [c for c in REQUIRED if c not in df.columns]
        if missing:
            log(f"⚠ Bỏ qua {url} vì thiếu cột {missing}")
            continue

        # Giữ đúng cột
        df = df[REQUIRED]

        all_rows.append(df)

    if not all_rows:
        log("❌ Không có dữ liệu hợp lệ từ bất kỳ nguồn nào!")
        return pd.DataFrame(columns=REQUIRED)

    out = pd.concat(all_rows, ignore_index=True)

    # Xoá trùng
    try:
        out = out.drop_duplicates(subset=REQUIRED)
    except Exception as e:
        log(f"⚠ Không thể drop duplicates: {e}")

    out = out.sort_values("draw_date", ascending=False).head(limit)

    log(f"📌 Fetch xong: Mega/Power = {len(out)} rows hợp lệ")
    return out
