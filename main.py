"""
MegaPowerReal - Main Pipeline
Author: ChatGPT
Version: 2025-11
Description:
  - Fetch Mega 6/45 & Power 6/55 data from ketquadientoan.com
  - Train + Predict + Evaluate + Auto Retrain
  - Generate Excel report + Send Email automatically (GitHub Actions)
"""

import os, json, pandas as pd
from datetime import datetime
from utils.fetch_data import fetch_all_data
from utils.train_model import train_models_and_save, ensemble_predict_topk
from utils.email_utils import send_email_with_report
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# === CONFIG ===
SAVE_DIR = "data"
MODELS_DIR = "models"
LOG_FILE = os.path.join(SAVE_DIR, "daily_log.txt")
LAST_PRED_PATH = os.path.join(SAVE_DIR, "last_prediction.json")
os.makedirs(SAVE_DIR, exist_ok=True)
os.makedirs(MODELS_DIR, exist_ok=True)

CFG = {
    "n_periods": 100,
    "window": 50,
    "gmail_user": os.getenv("GMAIL_USER"),
    "gmail_pass": os.getenv("GMAIL_PASS"),
    "receiver_email": os.getenv("RECEIVER_EMAIL"),
}

# === UTILS ===
def log(msg: str):
    t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{t}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def load_last_prediction():
    if not os.path.exists(LAST_PRED_PATH):
        return {}
    try:
        with open(LAST_PRED_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_last_prediction(pred_mega, pred_power):
    data = {
        "timestamp": datetime.now().isoformat(),
        "mega_pred": pred_mega,
        "power_pred": pred_power,
    }
    with open(LAST_PRED_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log(f"Saved last_prediction.json: {data}")

# === MAIN PIPELINE ===
def run_pipeline():
    log("🚀 Starting MegaPowerReal Pipeline...")

    # 1️⃣ Fetch data
    log("🔹 Fetching Mega 6/45...")
    mega_df, power_df = fetch_all_data(limit=CFG["n_periods"], save_dir=SAVE_DIR)
    log(f"✅ Mega rows: {len(mega_df)}, Power rows: {len(power_df)}")

    if len(mega_df) < 50 or len(power_df) < 50:
        log("❌ Not enough data to train.")
        return

    # 2️⃣ Train models
    log("🧠 Training models...")
    rf_path, gb_path, metrics = train_models_and_save(
        mega_df, power_df, window=CFG["window"], save_dir=SAVE_DIR, models_dir=MODELS_DIR
    )
    log(f"✅ Training completed | RF acc={metrics.get('acc_rf'):.3f} | GB acc={metrics.get('acc_gb'):.3f}")

    # 3️⃣ Predict next draw
    log("🎯 Predicting next numbers...")
    pred_mega, pred_power, probs = ensemble_predict_topk(
        mega_df, power_df, rf_path, gb_path, topk=6, save_dir=SAVE_DIR
    )
    save_last_prediction(pred_mega, pred_power)

    log(f"🎲 Mega 6/45 predicted: {pred_mega}")
    log(f"💫 Power 6/55 predicted: {pred_power}")

    # 4️⃣ Compare with last real draw (if exists)
    real_mega_path = os.path.join(SAVE_DIR, "mega_6_45_raw.csv")
    real_power_path = os.path.join(SAVE_DIR, "power_6_55_raw.csv")
    real_nums_mega, real_nums_power = [], []

    try:
        if os.path.exists(real_mega_path):
            dfm = pd.read_csv(real_mega_path)
            last = dfm.iloc[-1]
            real_nums_mega = sorted([int(last[f"n{i}"]) for i in range(1,7)])
        if os.path.exists(real_power_path):
            dfp = pd.read_csv(real_power_path)
            lastp = dfp.iloc[-1]
            real_nums_power = sorted([int(lastp[f"n{i}"]) for i in range(1,7)])
    except Exception as e:
        log(f"⚠️ Error reading real numbers: {e}")

    def accuracy(pred, real):
        if not real:
            return 0.0
        return len(set(pred) & set(real)) / 6

    acc_mega = accuracy(pred_mega, real_nums_mega)
    acc_power = accuracy(pred_power, real_nums_power)
    log(f"📊 Accuracy: Mega={acc_mega:.2%} | Power={acc_power:.2%}")

    # 5️⃣ Generate Excel report
    now_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(SAVE_DIR, f"mega_power_report_{now_tag}.xlsx")

    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        mega_df.to_excel(writer, index=False, sheet_name="Mega_6_45")
        power_df.to_excel(writer, index=False, sheet_name="Power_6_55")
        pd.DataFrame({
            "Mega_Pred": [pred_mega],
            "Power_Pred": [pred_power],
            "Real_Mega": [real_nums_mega],
            "Real_Power": [real_nums_power],
            "Acc_Mega": [acc_mega],
            "Acc_Power": [acc_power],
            "Timestamp": [datetime.now().isoformat()]
        }).to_excel(writer, index=False, sheet_name="Prediction_Report")

    log(f"📁 Report saved to: {report_path}")

    # 6️⃣ Send email
    try:
        send_email_with_report(
            sender=CFG["gmail_user"],
            password=CFG["gmail_pass"],
            recipient=CFG["receiver_email"],
            subject=f"[MegaPowerReal] Báo cáo dự đoán {datetime.now().strftime('%d/%m/%Y')}",
            body=f"""
🔮 MegaPowerReal Report

🎲 Mega 6/45 dự đoán: {pred_mega}
💫 Power 6/55 dự đoán: {pred_power}

🎯 Kết quả thật:
   Mega: {real_nums_mega}
   Power: {real_nums_power}

📊 Độ chính xác:
   Mega: {acc_mega:.2%}
   Power: {acc_power:.2%}

🕓 Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """,
            attachment_path=report_path
        )
        log("📨 Email sent successfully.")
    except Exception as e:
        log(f"⚠️ Email sending failed: {e}")

    log("✅ Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
