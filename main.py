# main.py
import os
import pandas as pd
from datetime import datetime
from utils.fetch_data import fetch_all_data

import smtplib
from email.message import EmailMessage

REPORT_DIR = "./reports"
os.makedirs(REPORT_DIR, exist_ok=True)

def send_email(report_path):
    EMAIL_TO = os.getenv("EMAIL_TO")
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASS = os.getenv("EMAIL_PASS")
    EMAIL_HOST = os.getenv("EMAIL_HOST")
    EMAIL_PORT = int(os.getenv("EMAIL_PORT", "465"))

    if not EMAIL_TO:
        print("⚠ Thiếu cấu hình email, bỏ qua gửi email")
        return

    msg = EmailMessage()
    msg['Subject'] = f"Báo cáo Mega/Power {datetime.now().strftime('%Y-%m-%d')}"
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    msg.set_content("Vui lòng xem báo cáo đính kèm.")

    with open(report_path, "rb") as f:
        msg.add_attachment(f.read(), maintype="application", subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=os.path.basename(report_path))

    with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
    print("✅ Email đã gửi thành công!")

def main():
    print("=== BẮT ĐẦU PIPELINE DỰ ĐOÁN MEGA/POWER ===")
    mega_df, power_df = fetch_all_data()

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
