# main.py
import os, json
from datetime import datetime
from utils.logger import log
from utils.fetch_data import fetch_all_data
from utils.train_model import train_models_and_save, ensemble_predict_topk
from utils.report import generate_report
from utils.email_utils import send_email_with_attachment

SAVE_DIR = os.getenv("DATA_DIR", "data")
MODELS_DIR = os.getenv("MODELS_DIR", "models")
N_PERIODS = int(os.getenv("N_PERIODS", "100"))
EMAIL_TO = os.getenv("EMAIL_RECEIVER", "")

LAST_FILE = os.path.join(SAVE_DIR, "last_prediction.json")

def save_last_prediction(pred_mega, pred_power, metrics, path=LAST_FILE):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = {"timestamp": datetime.utcnow().isoformat(), "mega_pred": pred_mega, "power_pred": pred_power, "metrics": metrics}
    with open(path, "w", encoding="utf-8") as f: json.dump(data, f, ensure_ascii=False, indent=2)
    log(f"Saved last_prediction.json: {data}")

def run_pipeline():
    log("🚀 Starting MegaPowerReal Pipeline...")
    mega_df, power_df = fetch_all_data(limit=N_PERIODS, save_dir=SAVE_DIR)
    log(f"✅ Mega rows: {len(mega_df)}, Power rows: {len(power_df)}")
    rf_path, gb_path, metrics = train_models_and_save(mega_df, power_df, window=50, save_dir=SAVE_DIR, models_dir=MODELS_DIR)
    if not rf_path:
        log("❌ Not enough data to train.")
    pred_mega, pred_power, probs = ensemble_predict_topk(mega_df, power_df, rf_path, gb_path, topk=6)
    log(f"🎲 Mega 6/45 predicted: {pred_mega}")
    log(f"💫 Power 6/55 predicted: {pred_power}")
    save_last_prediction(pred_mega, pred_power, metrics)
    report_path = generate_report(mega_df, power_df, pred_mega, pred_power, metrics, save_dir=SAVE_DIR)
    subj = f"MegaPower Report {datetime.utcnow().strftime('%Y-%m-%d')}"
    body = f"Predicted Mega: {pred_mega}\nPredicted Power: {pred_power}\nMetrics: {metrics}"
    ok, err = send_email_with_attachment(subj, body, os.getenv("EMAIL_RECEIVER", ""), attachment_path=report_path)
    if not ok:
        log(f"⚠️ Email sending failed: {err}")
    log("✅ Pipeline completed successfully.")

if __name__ == "__main__":
    run_pipeline()
