import pandas as pd
import requests
from io import StringIO
from time import sleep

SITE_CONFIGS = {
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html": 2,
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html": 2,
    "https://www.lotto-8.com/Vietnam/listltoVM45.asp": 0,
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html": 2,
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html": 2,
    "https://www.lotto-8.com/Vietnam/listltoVM55.asp": 0,
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def fetch_table(url, table_index=2, timeout_sec=30, retries=2):
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout_sec)
            r.raise_for_status()
            tables = pd.read_html(StringIO(r.text))  # fix FutureWarning, yêu cầu lxml hoặc html5lib
            if len(tables) > table_index:
                df = tables[table_index]
                return df, len(df)
            else:
                print(f"⚠ Không tìm thấy bảng thứ {table_index} trên {url}")
                return pd.DataFrame(), 0
        except Exception as e:
            print(f"❌ Lỗi fetch {url} (attempt {attempt+1}): {e}")
            sleep(2)
    return pd.DataFrame(), 0

def fetch_all_data(limit_mega=100, limit_power=100):
    mega_urls = [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
        "https://www.lotto-8.com/Vietnam/listltoVM45.asp",
    ]
    power_urls = [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html",
        "https://www.lotto-8.com/Vietnam/listltoVM55.asp",
    ]

    mega_df = pd.DataFrame()
    for url in mega_urls:
        idx = SITE_CONFIGS.get(url, 2)
        df, n_rows = fetch_table(url, table_index=idx)
        mega_df = pd.concat([mega_df, df], ignore_index=True).drop_duplicates()
        print(f"🔹 Tổng số rows Mega hiện tại: {len(mega_df)}")
        if len(mega_df) >= limit_mega:
            break
    mega_df = mega_df.head(limit_mega)

    power_df = pd.DataFrame()
    for url in power_urls:
        idx = SITE_CONFIGS.get(url, 2)
        df, n_rows = fetch_table(url, table_index=idx)
        power_df = pd.concat([power_df, df], ignore_index=True).drop_duplicates()
        print(f"🔹 Tổng số rows Power hiện tại: {len(power_df)}")
        if len(power_df) >= limit_power:
            break
    power_df = power_df.head(limit_power)

    return mega_df, power_df
