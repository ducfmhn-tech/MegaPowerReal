import os
import pandas as pd
from utils.fetch_data import fetch_all_data
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

REPORT_DIR = "./reports"
os.makedirs(REPORT_DIR, exist_ok=True)

def send_email(file_path):
    try:
        EMAIL_USER = os.getenv("EMAIL_USER")
        EMAIL_PASS = os.getenv("EMAIL_PASS")
        EMAIL_TO = os.getenv("EMAIL_TO")
        EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
        EMAIL_PORT = int(os.getenv("EMAIL_PORT", 465))

        msg = MIMEMultipart()
        msg["From"] = EMAIL_USER
        msg["To"] = EMAIL_TO
        msg["Subject"] = "MegaPowerReal Report"

        part = MIMEBase("application", "octet-stream")
        with open(file_path, "rb") as f:
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(file_path)}"')
        msg.attach(part)

        server = smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT)
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, EMAIL_TO, msg.as_string())
        server.quit()
        print(f"✅ Email gửi thành công tới {EMAIL_TO}")
    except Exception as e:
        print(f"❌ Lỗi gửi email: {e}")

def main():
    print("=== BẮT ĐẦU PIPELINE DỰ ĐOÁN MEGA/POWER ===")
    mega_df, power_df = fetch_all_data(limit_mega=100, limit_power=100)
    print(f"🔥 Mega rows: {len(mega_df)}, Power rows: {len(power_df)}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(REPORT_DIR, f"mega_power_report_{timestamp}.xlsx")

    with pd.ExcelWriter(report_path) as writer:
        mega_df.to_excel(writer, sheet_name="Mega", index=False)
        power_df.to_excel(writer, sheet_name="Power", index=False)

    print(f"✅ Báo cáo đã lưu tại {report_path}")

    # Gửi email
    if all([os.getenv("EMAIL_USER"), os.getenv("EMAIL_PASS"), os.getenv("EMAIL_TO")]):
        send_email(report_path)
    else:
        print("⚠ Thiếu cấu hình email, bỏ qua gửi email")

    print("=== PIPELINE HOÀN THÀNH ===")

if __name__ == "__main__":
    main()
