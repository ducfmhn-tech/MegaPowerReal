import os
import pandas as pd
from datetime import datetime
from utils.fetch_data import fetch_all_data
from utils.email_utils import send_email

LIMIT = 100
mega_df, power_df = fetch_all_data(limit=LIMIT)

print(f"🔥 Mega rows: {len(mega_df)}, Power rows: {len(power_df)}")
LIMIT = 100
REPORT_DIR = "./reports"
os.makedirs(REPORT_DIR, exist_ok=True)
today_str = datetime.now().strftime("%Y%m%d_%H%M%S")
report_path = f"{REPORT_DIR}/mega_power_report_{today_str}.xlsx"

# --- Fetch dữ liệu ---
mega_data, power_data = fetch_all_data(limit=LIMIT)
print(f"✅ Dữ liệu cuối cùng - Mega: {len(mega_data)} dòng, Power: {len(power_data)} dòng")

# --- Dự đoán heuristic ---
mega_pred = [1, 3, 6, 12, 19, 45]
power_pred = [1, 3, 6, 12, 19, 45]

# --- Tạo báo cáo Excel ---
with pd.ExcelWriter(report_path) as writer:
    pd.DataFrame(mega_data).to_excel(writer, sheet_name="Mega", index=False)
    pd.DataFrame(power_data).to_excel(writer, sheet_name="Power", index=False)
    pd.DataFrame({"Mega_Pred": [mega_pred], "Power_Pred": [power_pred]}).to_excel(writer, sheet_name="Predictions", index=False)

print(f"✅ Báo cáo đã lưu tại {report_path}")

# --- Gửi email ---
try:
    email_config = {
        "host": os.environ.get("EMAIL_HOST"),
        "port": int(os.environ.get("EMAIL_PORT") or 587),
        "user": os.environ.get("EMAIL_USER"),
        "password": os.environ.get("EMAIL_PASS"),
        "to": os.environ.get("EMAIL_TO")
    }
    if all(email_config.values()):
        send_email(report_path, email_config)
    else:
        print("⚠ Thiếu cấu hình email, bỏ qua gửi email")
except Exception as e:
    print(f"❌ Lỗi gửi email: {e}")

print("=== PIPELINE HOÀN THÀNH ===")
