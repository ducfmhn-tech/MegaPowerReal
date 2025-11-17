# main.py
import os, json
from datetime import datetime
from utils.logger import log
from utils.fetch_data import fetch_all_data
from utils.train_model import train_models_and_save, ensemble_predict_topk
from utils.report import generate_report
from utils.email_utils import send_email_with_attachment

# CONFIG (env-overridable)
DATA_DIR = os.getenv("DATA_DIR", "data")
MODELS_DIR = os.getenv("MODELS_DIR", "models")
N_PERIODS = int(os.getenv("N_PERIODS", "100"))
TOPK = int(os.getenv("TOPK", "6"))
WINDOW = int(os.getenv("WINDOW", "50"))

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(MODELS_DIR, exist_ok=True)

def run_pipeline():
    log("🚀 Starting MegaPowerReal Pipeline...")
    mega_df, power_df = fetch_all_data(limit=N_PERIODS, save_dir=DATA_DIR)
    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")

    enough_for_train = len(mega_df) >= 30 and len(power_df) >= 30

    rf_path = gb_path = None
    metrics = {}

    if enough_for_train:
        log("🧠 Training models...")
        rf_path, gb_path, metrics = train_models_and_save(
            mega_df, power_df, window=WINDOW, save_dir=MODELS_DIR
        )
        log(f"✅ Training completed | metrics: {metrics}")
    else:
        log("⚠️ Not enough data to train (need >=30 rows each). Skipping training.")

    log("🎯 Predicting next numbers...")
    try:
        pred_mega, pred_power, probs = ensemble_predict_topk(
            mega_df, power_df, rf_path=rf_path, gb_path=gb_path, topk=TOPK, window=WINDOW
        )
    except Exception as e:
        log(f"⚠ Prediction failed: {e}")
        pred_mega, pred_power, probs = [], [], {}

    log(f"🎲 Predicted Mega: {pred_mega}")
    log(f"💫 Predicted Power: {pred_power}")

    last = {
        "timestamp": datetime.utcnow().isoformat(),
        "mega_pred": pred_mega,
        "power_pred": pred_power,
        "metrics": metrics,
        "probs": probs
    }
    last_path = os.path.join(DATA_DIR, "last_prediction.json")
    with open(last_path, "w", encoding="utf-8") as f:
        json.dump(last, f, ensure_ascii=False, indent=2)
    log(f"Saved last_prediction.json: {last_path}")

    report_path = generate_report(mega_df, power_df, metrics, DATA_DIR,
                                  pred_mega=pred_mega, pred_power=pred_power)
    if report_path:
        log(f"📁 Report saved to: {report_path}")
    else:
        log("⚠️ Report generation failed.")

    email_status = send_email_with_attachment(
        subject=f"MegaPowerReal Report {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        body=f"Predicted Mega: {pred_mega}\nPredicted Power: {pred_power}\nMetrics: {metrics}",
        attachment_path=report_path
    )
    if email_status == "ok":
        log("📧 Email sent successfully.")
    else:
        log(f"⚠️ Email sending failed: {email_status}")

    log("✅ Pipeline completed.")

if __name__ == "__main__":
    run_pipeline()
