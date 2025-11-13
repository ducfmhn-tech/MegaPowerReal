"""
error_analysis.py
- compare last_prediction.json with latest real results
- compute match count and accuracy, log daily
- if accuracy < threshold then retrain using train_models_and_save (auto-retrain)
"""
import os, json
import pandas as pd
from datetime import datetime
from utils.train_model import train_models_and_save

def _read_last_pred(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return None

def _read_latest_actuals(save_dir):
    mega_path = os.path.join(save_dir, "mega_6_45_raw.csv")
    power_path = os.path.join(save_dir, "power_6_55_raw.csv")
    mega_df = pd.read_csv(mega_path)
    power_df = pd.read_csv(power_path)
    mega_real = [int(mega_df.iloc[-1][f"n{i}"]) for i in range(1,7)]
    power_real = [int(power_df.iloc[-1][f"n{i}"]) for i in range(1,7)]
    return mega_real, power_real

def check_and_retrain_if_needed(save_dir="data", models_dir="models", config=None):
    last_pred_path = os.path.join(save_dir, "last_prediction.json")
    last = _read_last_pred(last_pred_path)
    if not last:
        return {"status":"no_prev_pred"}
    mega_pred = last.get("Mega", [])
    power_pred = last.get("Power", [])
    mega_real, power_real = _read_latest_actuals(save_dir)
    matched_m = len(set(mega_pred) & set(mega_real))
    matched_p = len(set(power_pred) & set(power_real))
    acc_m = matched_m / 6.0 * 100.0
    acc_p = matched_p / 6.0 * 100.0
    log_line = f"[{datetime.now().isoformat()}] prev_pred_match Mega:{matched_m}/6 ({acc_m:.1f}%), Power:{matched_p}/6 ({acc_p:.1f}%)\n"
    logfile = os.path.join(save_dir, "daily_log.txt")
    with open(logfile, "a", encoding="utf-8") as f:
        f.write(log_line)
    # retrain policy
    threshold = config.get("threshold_retrain_pct", 50) if config else 50
    retrain_taken = False
    retrain_details = {}
    if acc_m < threshold or acc_p < threshold:
        # retrain models
        try:
            mega_df = pd.read_csv(os.path.join(save_dir, "mega_6_45_raw.csv"))
            power_df = pd.read_csv(os.path.join(save_dir, "power_6_55_raw.csv"))
            rf_p, gb_p, metrics = train_models_and_save(mega_df, power_df, window=config.get("window",50), save_dir=save_dir, models_dir=models_dir)
            retrain_taken = True
            retrain_details = {"rf_path": rf_p, "gb_path": gb_p, "metrics": metrics}
            with open(logfile, "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now().isoformat()}] Auto retrain done. metrics: {metrics}\n")
        except Exception as e:
            with open(logfile, "a", encoding="utf-8") as f:
                f.write(f"[{datetime.now().isoformat()}] Auto retrain FAILED: {e}\n")
    return {"status":"checked", "acc_m":acc_m, "acc_p":acc_p, "retrain":retrain_taken, "details":retrain_details}
