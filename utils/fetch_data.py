"""
fetch_data.py
- Fetch Vietlott Mega 6/45 & Power 6/55
- Parse HTML table into clean DataFrame
- Sort n1–n6 (required)
- Provide fetch_all_data() for main.py
"""

import os
import re
import time
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from utils.logger import log

N_PERIODS = 120

MEGA_URLS = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html"
]

POWER_URLS = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-6-55.html"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# -------------------------------------------------------------------
# 1) FETCH HTML
# -------------------------------------------------------------------
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


# -------------------------------------------------------------------
# 2) Parse helper functions
# -------------------------------------------------------------------
def parse_numbers_from_string(text: str):
    if not isinstance(text, str):
        return []
    cleaned = re.sub(r"[^\d ,]", "", text)
    return [int(x) for x in re.findall(r"\d+", cleaned)]


# -------------------------------------------------------------------
# 3) Parse HTML → DataFrame
# -------------------------------------------------------------------
def parse_html_to_df(html_text, url, limit=100):
    try:
        dfs = pd.read_html(StringIO(html_text))
    except Exception:
        return pd.DataFrame()

    final_df = pd.DataFrame()

    for df in dfs:
        df.columns = [str(c).strip().lower() for c in df.columns]

        # find date column
        date_col = next(
            (c for c in df.columns
             if df[c].astype(str).str.contains(r"\d{1,2}/\d{1,2}/\d{2,4}", na=False).any()),
            None
        )
        if not date_col:
            continue

        # find merged numbers column
        num_col = next(
            (c for c in df.columns
             if df[c].astype(str).str.contains(r"(\d+[, ]+){5}\d+", na=False).any()),
            None
        )

        if num_col:
            # Extract merged numbers
            nums = df[num_col].astype(str).apply(parse_numbers_from_string)
            nums = nums[nums.apply(lambda x: len(x) >= 6)]
            if nums.empty:
                continue

            nums_df = pd.DataFrame([x[:6] for x in nums.tolist()],
                                   index=nums.index,
                                   columns=[f"n{i}" for i in range(1, 7)])

            out = pd.concat(
                [df.loc[nums.index, date_col].rename("draw_date"), nums_df],
                axis=1
            )
        else:
            # Fallback: detect columns with numeric content
            numeric_cols = [c for c in df.columns if
                            pd.to_numeric(df[c], errors="coerce").notna().mean() > 0.7]

            if len(numeric_cols) < 6:
                continue

            numeric_cols = numeric_cols[:6]
            out = df[[date_col] + numeric_cols].copy()
            out.rename(columns={date_col: "draw_date"}, inplace=True)
            for i, c in enumerate(numeric_cols):
                out.rename(columns={c: f"n{i+1}"}, inplace=True)

        # normalize date
        out["draw_date"] = (
            out["draw_date"]
            .astype(str)
            .str.replace(r"^\w{1,2},\s*", "", regex=True)
        )
        extracted = out["draw_date"].str.extract(r"(\d{1,2}/\d{1,2}/\d{2,4})")
        out["draw_date"] = extracted[0].fillna(out["draw_date"])
        out["draw_date"] = pd.to_datetime(out["draw_date"],
                                          format="%d/%m/%Y",
                                          errors="coerce")
        out.dropna(subset=["draw_date"], inplace=True)

        # numeric clean
        for i in range(1, 7):
            out[f"n{i}"] = pd.to_numeric(out[f"n{i}"], errors="coerce")
        out.dropna(subset=[f"n{i}" for i in range(1, 7)], inplace=True)

        # SORT numbers
        for idx in out.index:
            nums = sorted([int(out.loc[idx, f"n{i}"]) for i in range(1, 7)])
            for i in range(6):
                out.loc[idx, f"n{i+1}"] = nums[i]

        final_df = pd.concat([final_df, out], ignore_index=True)

    if final_df.empty:
        return pd.DataFrame()

    final_df.drop_duplicates(
        subset=["draw_date"] + [f"n{i}" for i in range(1, 7)],
        inplace=True
    )
    final_df.sort_values(by="draw_date", ascending=False, inplace=True)

    return final_df.head(limit).copy()


# -------------------------------------------------------------------
# 4) MULTI-SOURCE fetch
# -------------------------------------------------------------------
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


# -------------------------------------------------------------------
# 5) PUBLIC FUNCTIONS
# -------------------------------------------------------------------
def fetch_mega():
    log("🔹 Fetching Mega 6/45...")
    return fetch_multiple(MEGA_URLS, limit=N_PERIODS)


def fetch_power():
    log("🔹 Fetching Power 6/55...")
    return fetch_multiple(POWER_URLS, limit=N_PERIODS)


# -------------------------------------------------------------------
# 6) MAIN FUNCTION for main.py (required)
# -------------------------------------------------------------------
def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)

    mega_df = fetch_mega()
    power_df = fetch_power()

    # save CSV
    mega_df.to_csv(os.path.join(save_dir, "mega_6_45_raw.csv"), index=False)
    power_df.to_csv(os.path.join(save_dir, "power_6_55_raw.csv"), index=False)

    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")

    return mega_df, power_df


__all__ = ["fetch_all_data", "fetch_mega", "fetch_power"]
