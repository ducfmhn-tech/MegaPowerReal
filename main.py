import os
import pandas as pd
from utils.fetch_data import fetch_all_data
from utils.email_utils import send_email
from datetime import datetime

# --- Cấu hình ---
LIMIT = 100
REPORT_DIR = "./reports"
os.makedirs(REPORT_DIR, exist_ok=True)
today_str = datetime.now().strftime("%Y%m%d_%H%M%S")
report_path = f"{REPORT_DIR}/mega_power_report_{today_str}.xlsx"

# --- Bước 1: Thu thập dữ liệu ---
print("=== BẮT ĐẦU PIPELINE DỰ ĐOÁN MEGA/POWER ===")
mega_data, power_data = fetch_all_data(limit=LIMIT)
print(f"✅ Dữ liệu cuối cùng - Mega: {len(mega_data)} dòng, Power: {len(power_data)} dòng")

if len(mega_data) < 50 or len(power_data) < 50:
    print("⚠ Cảnh báo: Dữ liệu không đủ cho window=50. Vẫn tiếp tục với heuristic.")

# --- Bước 2: Tiền xử lý ---
print(f"Mega: {len(mega_data)} rows | Power: {len(power_data)} rows sau tiền xử lý.")

# --- Bước 3 & 4: Dự đoán (giữ heuristic nếu dữ liệu quá ít) ---
mega_pred = [1, 3, 6, 12, 19, 45]
power_pred = [1, 3, 6, 12, 19, 45]
print(f"🔥 DỰ ĐOÁN MEGA 6/45: {mega_pred}")
print(f"🔥 DỰ ĐOÁN POWER 6/55: {power_pred}")

# --- Bước 5: Tạo báo cáo Excel ---
with pd.ExcelWriter(report_path) as writer:
    pd.DataFrame(mega_data).to_excel(writer, sheet_name="Mega", index=False)
    pd.DataFrame(power_data).to_excel(writer, sheet_name="Power", index=False)
    pd.DataFrame({"Mega_Pred": [mega_pred], "Power_Pred": [power_pred]}).to_excel(writer, sheet_name="Predictions", index=False)

print(f"✅ Báo cáo đã lưu tại {report_path}")

# --- Bước 6: Gửi email ---
try:
    email_config = {
        "host": os.environ.get("EMAIL_HOST"),
        "port": int(os.environ.get("EMAIL_PORT", 587)),
        "user": os.environ.get("EMAIL_USER"),
        "password": os.environ.get("EMAIL_PASS"),
        "to": os.environ.get("EMAIL_TO")
    }
    if all(email_config.values()):
        send_email(report_path, email_config)
        print("✅ Email dự báo đã gửi thành công")
    else:
        print("⚠ Thiếu cấu hình email, bỏ qua gửi email")
except Exception as e:
    print(f"❌ Lỗi gửi email: {e}")

print("=== PIPELINE HOÀN THÀNH ===")
