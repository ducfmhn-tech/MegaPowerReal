import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from io import StringIO

# --------------------------
# UTILS
# --------------------------
def get_html(url, timeout=10):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


# ============================================================
# 1. PARSER Mega / Power CHUẨN CHO KETQUADIENTOAN.COM
# ============================================================

def parse_ketquadt_mega(html):
    """
    HTML mẫu:
        <div class="result-list mega">
            <div class="result-row"> ... <span class="ball-mega">03</span> ...
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []

    for row in soup.select(".result-list.mega .result-row"):
        date_el = row.select_one(".draw-date")
        balls = row.select(".ball-mega")

        if not date_el or len(balls) != 6:
            continue

        date_text = date_el.get_text(strip=True)
        nums = [int(b.get_text(strip=True)) for b in balls]

        rows.append({
            "date": date_text,
            "n1": nums[0],
            "n2": nums[1],
            "n3": nums[2],
            "n4": nums[3],
            "n5": nums[4],
            "n6": nums[5],
        })

    return rows


def parse_ketquadt_power(html):
    """
    HTML mẫu:
        <div class="result-list power">
            <span class="ball-power">05</span> ... <span class="ball-bonus">55</span>
    """
    soup = BeautifulSoup(html, "lxml")
    rows = []

    for row in soup.select(".result-list.power .result-row"):
        date_el = row.select_one(".draw-date")
        balls = row.select(".ball-power")
        bonus = row.select_one(".ball-bonus")

        if not date_el or len(balls) != 6 or not bonus:
            continue

        date_text = date_el.get_text(strip=True)
        nums = [int(b.get_text(strip=True)) for b in balls]
        bonus_num = int(bonus.get_text(strip=True))

        rows.append({
            "date": date_text,
            "n1": nums[0],
            "n2": nums[1],
            "n3": nums[2],
            "n4": nums[3],
            "n5": nums[4],
            "n6": nums[5],
            "bonus": bonus_num,
        })

    return rows


# ============================================================
# 2. PARSER CHO MINHNGOC.NET.VN (CÓ TABLE)
# ============================================================

def parse_minhngoc(html, mega=True):
    """
    Dùng read_html vì minhngoc có table chuẩn.
    """
    try:
        tables = pd.read_html(StringIO(html))
    except:
        return []

    if not tables:
        return []

    df = tables[0]

    # Mega
    if mega and df.shape[1] >= 8:
        df.columns = ["ki", "ngay", "n1", "n2", "n3", "n4", "n5", "n6"]
        df = df.dropna()
        df["date"] = df["ngay"]
        return df[["date", "n1", "n2", "n3", "n4", "n5", "n6"]].to_dict("records")

    # Power
    if not mega and df.shape[1] >= 9:
        df.columns = ["ki", "ngay", "n1", "n2", "n3", "n4", "n5", "n6", "bonus"]
        df = df.dropna()
        df["date"] = df["ngay"]
        return df[["date", "n1", "n2", "n3", "n4", "n5", "n6", "bonus"]].to_dict("records")

    return []


# ============================================================
# 3. PARSER CHO LOTTO-8.COM (TABLE CỔ)
# ============================================================

def parse_lotto(html, mega=True):
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")

    if not table:
        return []

    rows = []
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if mega and len(tds) >= 8:
            rows.append({
                "date": tds[1].get_text(strip=True),
                "n1": int(tds[2].get_text()),
                "n2": int(tds[3].get_text()),
                "n3": int(tds[4].get_text()),
                "n4": int(tds[5].get_text()),
                "n5": int(tds[6].get_text()),
                "n6": int(tds[7].get_text()),
            })
        if not mega and len(tds) >= 9:
            rows.append({
                "date": tds[1].get_text(strip=True),
                "n1": int(tds[2].get_text()),
                "n2": int(tds[3].get_text()),
                "n3": int(tds[4].get_text()),
                "n4": int(tds[5].get_text()),
                "n5": int(tds[6].get_text()),
                "n6": int(tds[7].get_text()),
                "bonus": int(tds[8].get_text()),
            })

    return rows


# ============================================================
# 4. HÀM LẤY DỮ LIỆU CHÍNH
# ============================================================

def fetch_all_mega():
    urls = {
        "ketquadientoan": "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
        "minhngoc": "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
        "lotto8": "https://www.lotto-8.com/Vietnam/listltoVM45.asp",
    }

    all_rows = []

    for name, url in urls.items():
        try:
            html = get_html(url)
            if name == "ketquadientoan":
                rows = parse_ketquadt_mega(html)
            elif name == "minhngoc":
                rows = parse_minhngoc(html, mega=True)
            else:
                rows = parse_lotto(html, mega=True)

            all_rows.extend(rows)

        except Exception as e:
            print(f"[!] Failed: {name} | {e}")

    df = pd.DataFrame(all_rows).drop_duplicates()
    return df


def fetch_all_power():
    urls = {
        "ketquadientoan": "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
        "minhngoc": "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html",
        "lotto8": "https://www.lotto-8.com/Vietnam/listltoVM55.asp",
    }

    all_rows = []
    for name, url in urls.items():
        try:
            html = get_html(url)
            if name == "ketquadientoan":
                rows = parse_ketquadt_power(html)
            elif name == "minhngoc":
                rows = parse_minhngoc(html, mega=False)
            else:
                rows = parse_lotto(html, mega=False)

            all_rows.extend(rows)

        except Exception as e:
            print(f"[!] Failed: {name} | {e}")

    df = pd.DataFrame(all_rows).drop_duplicates()
    return df


# ============================================================
# MAIN CALL
# ============================================================

def fetch_mega_and_power():
    mega_df = fetch_all_mega()
    power_df = fetch_all_power()
    return mega_df, power_df
