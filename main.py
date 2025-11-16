# main.py
import os
import json
from datetime import datetime
import pandas as pd

from utils.logger import log
from utils.fetch_data import fetch_all_data
from utils.train_model import train_models_and_save, ensemble_predict_topk
from utils.report import generate_report
from utils.email_utils import send_email_with_attachment

# Config
CFG = {
    "data_dir": "data",
    "n_periods": 100,
    "models_dir": "models",
    "topk": 6,
    "report_dir": "data"
}

def run_pipeline():
    log("🚀 Starting MegaPowerReal Pipeline...")
    os.makedirs(CFG["data_dir"], exist_ok=True)
    os.makedirs(CFG["models_dir"], exist_ok=True)

    # 1) Fetch data
    log("🔹 Fetching data sources...")
    mega_df, power_df = fetch_all_data(limit=CFG["n_periods"], save_dir=CFG["data_dir"])
    log(f"✅ Mega rows: {len(mega_df)}, Power rows: {len(power_df)}")

    if len(mega_df) < 20 or len(power_df) < 20:
        log("❌ Not enough data to train.")
        # still generate report of current raw data
        report_path = generate_report(mega_df, power_df, {}, CFG["report_dir"], pred_mega=[], pred_power=[])
        log(f"📁 Report saved to: {report_path}")
        return

    # 2) Train models & save
    log("🧠 Training models...")
    rf_path, gb_path, metrics = train_models_and_save(mega_df, power_df, save_dir=CFG["models_dir"])
    log(f"✅ Models trained: RF={metrics.get('acc_rf'):.3f}, GB={metrics.get('acc_gb'):.3f}")

    # 3) Predict (ensemble)
    log("🎯 Predicting next numbers...")
    pred_mega, pred_power, probs = ensemble_predict_topk(
        mega_df, power_df, rf_path=rf_path, gb_path=gb_path, topk=CFG["topk"]
    )
    log(f"🎲 Mega 6/45 predicted: {pred_mega}")
    log(f"💫 Power 6/55 predicted: {pred_power}")

    # 4) Save last_prediction.json
    info = {
        "timestamp": datetime.now().isoformat(),
        "mega_pred": pred_mega,
        "power_pred": pred_power,
        "metrics": metrics,
        "probs": probs
    }
    json_path = os.path.join(CFG["data_dir"], "last_prediction.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(info, f, ensure_ascii=False, indent=2)
    log(f"Saved last_prediction.json: {json_path}")

    # 5) Generate and save report
    report_path = generate_report(mega_df, power_df, metrics, CFG["report_dir"],
                                  pred_mega=pred_mega, pred_power=pred_power)
    log(f"📁 Report saved to: {report_path}")

    # 6) Send email (optional)
    send_status = send_email_with_attachment(
        subject=f"MegaPowerReal Report {datetime.now().strftime('%Y-%m-%d')}",
        body=f"Prediction Mega: {pred_mega}\nPrediction Power: {pred_power}\nMetrics: {metrics}",
        attachment_path=report_path
    )
    if send_status == "ok":
        log("📧 Email sent successfully.")
    else:
        log(f"⚠️ Email sending failed: {send_status}")

    log("✅ Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
