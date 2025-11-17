# main.py
import os
import pandas as pd
from datetime import datetime
from io import StringIO
import requests
import smtplib
from email.message import EmailMessage
from time import sleep

REPORT_DIR = "./reports"
os.makedirs(REPORT_DIR, exist_ok=True)

# --- CONFIG URL và table_index cho từng website ---
SITE_CONFIGS = {
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html": 2,
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html": 2,
    "https://www.lotto-8.com/Vietnam/listltoVM45.asp": 0,
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html": 2,
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html": 2,
    "https://www.lotto-8.com/Vietnam/listltoVM55.asp": 0,
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# --- FETCH TABLE ---
def fetch_table(url, table_index=2, timeout_sec=30, retries=2):
    """Fetch bảng kết quả từ URL, trả về DataFrame và số rows"""
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout_sec)
            r.raise_for_status()
            tables = pd.read_html(StringIO(r.text))  # Fix FutureWarning
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

# --- FETCH ALL DATA ---
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

# --- SEND EMAIL ---
def send_email(report_path):
    EMAIL_TO = os.getenv("EMAIL_TO")
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASS = os.getenv("EMAIL_PASS")
    EMAIL_HOST = os.getenv("EMAIL_HOST")
    EMAIL_PORT = int(os.getenv("EMAIL_PORT") or 465)  # Fix lỗi khi rỗng

    if not EMAIL_TO:
        print("⚠ Thiếu cấu hình email, bỏ qua gửi email")
        return

    msg = EmailMessage()
    msg['Subject'] = f"Báo cáo Mega/Power {datetime.now().strftime('%Y-%m-%d')}"
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    msg.set_content("Vui lòng xem báo cáo đính kèm.")

    with open(report_path, "rb") as f:
        msg.add_attachment(f.read(),
                           maintype="application",
                           subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           filename=os.path.basename(report_path))

    with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
    print("✅ Email đã gửi thành công!")

# --- MAIN ---
def main():
    print("=== BẮT ĐẦU PIPELINE DỰ ĐOÁN MEGA/POWER ===")
    mega_df, power_df = fetch_all_data(limit_mega=100, limit_power=100)
    print(f"🔥 Mega rows: {len(mega_df)}, Power rows: {len(power_df)}")

    # Tạo báo cáo Excel
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(REPORT_DIR, f"mega_power_report_{now_str}.xlsx")
    with pd.ExcelWriter(report_path) as writer:
        mega_df.to_excel(writer, sheet_name="Mega", index=False)
        power_df.to_excel(writer, sheet_name="Power", index=False)
    print(f"✅ Báo cáo đã lưu tại {report_path}")

    # Gửi email nếu có cấu hình
    send_email(report_path)

    print("=== PIPELINE HOÀN THÀNH ===")

if __name__ == "__main__":
    main()
