# main.py (UPDATED)
import os, json
from datetime import datetime
from utils.logger import log
from utils.fetch_data import fetch_all_data
from utils.train_model import train_models_and_save, ensemble_predict_topk
from utils.report import generate_report
from utils.email_utils import send_email_with_attachment

# Config
CFG = {
    "data_dir": "data",
    "models_dir": "models",
    "n_periods": 150,
    "topk": 6,
    "window": 50
}

def run_pipeline():
    os.makedirs(CFG["data_dir"], exist_ok=True)
    os.makedirs(CFG["models_dir"], exist_ok=True)
    log("🚀 Starting MegaPowerReal Pipeline...")

    # 1: fetch
    log("🔹 Fetching data (no selenium)...")
    mega_df, power_df = fetch_all_data(limit=CFG["n_periods"], save_dir=CFG["data_dir"])
    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")

    # 2: check data
    if len(mega_df) < 30 or len(power_df) < 30:
        log("❌ Not enough data to train. Generating report of current data.")
        report_path = generate_report(mega_df, power_df, {}, CFG["data_dir"], pred_mega=[], pred_power=[])
        log(f"📁 Report saved to: {report_path}")
        return

    # 3: train
    log("🧠 Training models...")
    rf_path, gb_path, metrics = train_models_and_save(mega_df, power_df, window=CFG["window"], save_dir=CFG["models_dir"])
    log(f"✅ Models trained: {metrics}")

    # 4: predict
    log("🎯 Predicting next numbers...")
    pred_mega, pred_power, probs = ensemble_predict_topk(mega_df, power_df, rf_path=rf_path, gb_path=gb_path, topk=CFG["topk"], window=CFG["window"])
    log(f"🎲 Predicted Mega: {pred_mega}")
    log(f"💫 Predicted Power: {pred_power}")

    # 5: save last prediction
    last = {
        "timestamp": datetime.now().isoformat(),
        "mega_pred": pred_mega, "power_pred": pred_power,
        "metrics": metrics, "probs": probs
    }
    last_path = os.path.join(CFG["data_dir"], "last_prediction.json")
    with open(last_path, "w", encoding="utf-8") as f:
        json.dump(last, f, ensure_ascii=False, indent=2)
    log(f"Saved last_prediction.json: {last_path}")

    # 6: report
    report_path = generate_report(mega_df, power_df, metrics, CFG["data_dir"], pred_mega=pred_mega, pred_power=pred_power)
    log(f"📁 Report saved to: {report_path}")

    # 7: send email (optional: require secrets)
    status = send_email_with_attachment(
        subject=f"MegaPowerReal Report {datetime.now().strftime('%Y-%m-%d')}",
        body=f"Mega: {pred_mega}\nPower: {pred_power}\nMetrics: {metrics}",
        attachment_path=report_path
    )
    if status == "ok":
        log("📧 Email sent.")
    else:
        log(f"⚠ Email not sent: {status}")

    log("✅ Pipeline finished successfully.")

if __name__ == "__main__":
    run_pipeline()
