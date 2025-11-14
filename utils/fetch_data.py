import os
import re
import time
import requests
import pandas as pd
from io import StringIO
from utils.logger import log

# ---------------------------------------------
# CONFIG
# ---------------------------------------------
N_PERIODS = 120

MEGA_URLS = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html"
]

POWER_URLS = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-6-55.html"
]

HEADERS = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# ---------------------------------------------
# FETCH HTML WITH RETRY
# ---------------------------------------------
def fetch_html(url, retry=3):
    for i in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.encoding = "utf-8"
            r.raise_for_status()
            return r.text
        except Exception as e:
            log(f"[Retry {i+1}/{retry}] fetch error {url}: {e}")
            time.sleep(2)
    return None


# ---------------------------------------------
# SUPPORT FUNCTIONS
# ---------------------------------------------
def parse_numbers_from_string(text: str):
    """Extract all numbers inside a string"""
    if not isinstance(text, str):
        return []
    cleaned = re.sub(r"[^\d ,]", "", text)
    return [int(x) for x in re.findall(r"\d+", cleaned)]


# ---------------------------------------------
# PARSE HTML → CLEAN DATAFRAME
# ---------------------------------------------
def parse_html_to_df(html_text, url, limit=100):
    try:
        dfs = pd.read_html(StringIO(html_text))
    except Exception:
        return pd.DataFrame()

    final_df = pd.DataFrame()

    for df in dfs:
        df.columns = [str(c).strip().lower() for c in df.columns]

        # -----------------------------------
        # 1) Tìm cột ngày
        # -----------------------------------
        date_col = None
        for col in df.columns:
            if df[col].astype(str).str.contains(r"\d{1,2}/\d{1,2}/\d{2,4}", na=False).any():
                date_col = col
                break
        if not date_col:
            continue

        # -----------------------------------
        # 2) Tìm cột chứa chuỗi 6 số (merged)
        # -----------------------------------
        num_col = None
        for col in df.columns:
            if df[col].astype(str).str.contains(r"(\d+[, ]+){5}\d+", na=False).any():
                num_col = col
                break

        if num_col:
            # extract merged-col numbers
            nums = df[num_col].astype(str).apply(parse_numbers_from_string)
            nums = nums[nums.apply(lambda x: len(x) >= 6)]

            if nums.empty:
                continue

            nums_df = pd.DataFrame([x[:6] for x in nums.tolist()],
                                   index=nums.index,
                                   columns=[f"n{i}" for i in range(1, 7)])
            out = pd.concat([df.loc[nums.index, date_col].rename("draw_date"), nums_df], axis=1)
        else:
            # fallback: 6 numeric columns
            num_candidates = []
            for col in df.columns:
                numeric_ratio = pd.to_numeric(df[col], errors="coerce").notna().mean()
                if numeric_ratio > 0.7:
                    num_candidates.append(col)

            if len(num_candidates) < 6:
                continue

            num_candidates = num_candidates[:6]
            rename_map = {num_candidates[i]: f"n{i+1}" for i in range(6)}

            out = df.rename(columns=rename_map)
            out = out[[date_col] + [f"n{i}" for i in range(1, 7)]].copy()
            out.rename(columns={date_col: "draw_date"}, inplace=True)

        # -----------------------------------
        # Chuẩn hóa ngày
        # -----------------------------------
        out["draw_date"] = (
            out["draw_date"]
            .astype(str)
            .str.replace(r"^\w{1,2},\s*", "", regex=True)
        )
        extracted = out["draw_date"].str.extract(r"(\d{1,2}/\d{1,2}/\d{2,4})")
        out["draw_date"] = extracted[0].fillna(out["draw_date"])
        out["draw_date"] = pd.to_datetime(out["draw_date"], format="%d/%m/%Y", errors="coerce")
        out.dropna(subset=["draw_date"], inplace=True)

        # -----------------------------------
        # Clean n1..n6
        # -----------------------------------
        for i in range(1, 7):
            c = f"n{i}"
            out[c] = pd.to_numeric(out[c], errors="coerce")
        out.dropna(subset=[f"n{i}" for i in range(1, 7)], inplace=True)

        # -----------------------------------
        # SORT n1..n6 (yêu cầu mới)
        # -----------------------------------
        for idx in out.index:
            nums = sorted([int(out.loc[idx, f"n{i}"]) for i in range(1, 7)])
            for i in range(6):
                out.loc[idx, f"n{i+1}"] = nums[i]

        # Append
        final_df = pd.concat([final_df, out], ignore_index=True)

    if final_df.empty:
        return pd.DataFrame()

    final_df.drop_duplicates(
        subset=["draw_date"] + [f"n{i}" for i in range(1, 7)], inplace=True
    )

    final_df.sort_values(by="draw_date", ascending=False, inplace=True)

    return final_df.head(limit).copy()


# ---------------------------------------------
# FETCH MULTIPLE PAGES
# ---------------------------------------------
def fetch_multiple(urls, limit=100):
    merged = pd.DataFrame()

    for url in urls:
        html = fetch_html(url)
        if not html:
            continue

        df = parse_html_to_df(html, url, limit)
        if not df.empty:
            df["source"] = url
            merged = pd.concat([merged, df], ignore_index=True)

    if merged.empty:
        return pd.DataFrame()

    merged.drop_duplicates(
        subset=["draw_date"] + [f"n{i}" for i in range(1, 7)],
        inplace=True
    )

    merged.sort_values(by="draw_date", ascending=False, inplace=True)

    return merged.head(limit).copy()
# ---------------------------------------------
# PUBLIC FUNCTIONS
# ---------------------------------------------
def fetch_mega():
    log("🔹 Fetching Mega 6/45...")
    return fetch_multiple(MEGA_URLS, limit=N_PERIODS)


def fetch_power():
    log("🔹 Fetching Power 6/55...")
    return fetch_multiple(POWER_URLS, limit=N_PERIODS)
