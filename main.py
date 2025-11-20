# main.py
import os
import pandas as pd
import numpy as np
from datetime import datetime
import utils.debug_wrapper
from utils.fetch_data import fetch_all_sources
from utils.stats import frequency_stats, pair_frequency_stats, repeat_stats
from utils.heuristic import heuristic_predict
from utils.predict import build_features, train_multioutput_rf, predict_next
from utils.predict_advanced import load_model, ensemble_predict
import ssl
from email.message import EmailMessage
import smtplib

REPORT_DIR = "./reports"
os.makedirs(REPORT_DIR, exist_ok=True)

MEGA_URLS = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
    "https://www.lotto-8.com/Vietnam/listltoVM45.asp",
]

POWER_URLS = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html",
    "https://www.lotto-8.com/Vietnam/listltoVM55.asp",
]

def try_load_models(prefix):
    models = []
    for suffix in ["lgb.joblib", "cat.joblib", "mlp.joblib"]:
        path = f"models/{prefix}_{suffix}"
        if os.path.exists(path):
            try:
                models.append(load_model(path))
            except Exception as e:
                print(f"⚠ Failed to load {path}: {e}")
                models.append(None)
        else:
            models.append(None)
    return models

def send_email_with_attachments(report_path, extra_files=None):
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASS = os.getenv("EMAIL_PASS")
    EMAIL_TO = os.getenv("EMAIL_TO")
    EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
    EMAIL_PORT = int(os.getenv("EMAIL_PORT") or 465)

    if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO:
        print("⚠ Missing email config; skip sending")
        return

    msg = EmailMessage()
    msg["Subject"] = f"MegaPower Report {datetime.now().strftime('%Y-%m-%d')}"
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO
    msg.set_content("Attached: report and metrics (if available).")

    # attach report
    with open(report_path, "rb") as f:
        msg.add_attachment(f.read(), maintype="application", subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet", filename=os.path.basename(report_path))

    # attach extras
    if extra_files:
        for ef in extra_files:
            if os.path.exists(ef):
                with open(ef, "rb") as f:
                    subtype = "csv" if ef.endswith(".csv") else "octet-stream"
                    msg.add_attachment(f.read(), maintype="text", subtype=subtype, filename=os.path.basename(ef))

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT, context=ctx) as smtp:
        smtp.login(EMAIL_USER, EMAIL_PASS)
        smtp.send_message(msg)
    print("📧 Email sent to", EMAIL_TO)

def main():
    print("=== BẮT ĐẦU PIPELINE DỰ ĐOÁN MEGA/POWER ===")
    mega_df = fetch_all_sources(MEGA_URLS, limit=400)
    power_df = fetch_all_sources(POWER_URLS, limit=400)
    print(f"🔥 Mega rows: {len(mega_df)}, Power rows: {len(power_df)}")

    # stats
    m_freq = frequency_stats(mega_df)
    p_freq = frequency_stats(power_df)
    m_pairs = pair_frequency_stats(mega_df)
    p_pairs = pair_frequency_stats(power_df)
    m_rep = repeat_stats(mega_df)
    p_rep = repeat_stats(power_df)

    # try load ensemble models
    mega_models = try_load_models("mega")
    power_models = try_load_models("power")
    mega_feat = None
    power_feat = None
    if os.path.exists("models/mega_last_feat.npy"):
        try:
            mega_feat = np.load("models/mega_last_feat.npy")
        except Exception:
            mega_feat = None
    if os.path.exists("models/power_last_feat.npy"):
        try:
            power_feat = np.load("models/power_last_feat.npy")
        except Exception:
            power_feat = None

    mega_ensemble_pred = None
    power_ensemble_pred = None
    try:
        if mega_feat is not None and any(m is not None for m in mega_models):
            mega_ensemble_pred = ensemble_predict(mega_models, mega_feat, max_num=45)
    except Exception as e:
        print("⚠ Ensemble mega failed:", e)

    try:
        if power_feat is not None and any(m is not None for m in power_models):
            power_ensemble_pred = ensemble_predict(power_models, power_feat, max_num=55)
    except Exception as e:
        print("⚠ Ensemble power failed:", e)

    # fallback: on-the-fly ML
    mega_pred_ml = None
    power_pred_ml = None
    try:
        Xm, Ym = build_features(mega_df, window=50, max_num=45)
        if Xm is not None and len(Xm) > 0:
            m_model = train_multioutput_rf(Xm, Ym)
            mega_pred_ml = predict_next(m_model, Xm[-1])
    except Exception as e:
        print("⚠ On-the-fly ML mega failed:", e)

    try:
        Xp, Yp = build_features(power_df, window=50, max_num=55)
        if Xp is not None and len(Xp) > 0:
            p_model = train_multioutput_rf(Xp, Yp)
            power_pred_ml = predict_next(p_model, Xp[-1])
    except Exception as e:
        print("⚠ On-the-fly ML power failed:", e)

    # heuristic fallback
    mega_pred_heur = heuristic_predict(m_freq, k=6, max_num=45)
    power_pred_heur = heuristic_predict(p_freq, k=6, max_num=55)

    # final selection
    mega_final = mega_ensemble_pred or mega_pred_ml or mega_pred_heur
    power_final = power_ensemble_pred or power_pred_ml or power_pred_heur

    # write excel
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"{REPORT_DIR}/mega_power_report_{ts}.xlsx"
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        mega_df.to_excel(writer, sheet_name="Mega", index=False)
        power_df.to_excel(writer, sheet_name="Power", index=False)
        m_freq.to_excel(writer, sheet_name="Mega_Freq", index=False)
        p_freq.to_excel(writer, sheet_name="Power_Freq", index=False)
        m_pairs.to_excel(writer, sheet_name="Mega_Pairs", index=False)
        p_pairs.to_excel(writer, sheet_name="Power_Pairs", index=False)
        pd.DataFrame({"Repeated": m_rep}).to_excel(writer, sheet_name="Mega_Repeat", index=False)
        pd.DataFrame({"Repeated": p_rep}).to_excel(writer, sheet_name="Power_Repeat", index=False)
        pd.DataFrame({"Mega_Ensemble":[mega_ensemble_pred], "Mega_ML":[mega_pred_ml], "Mega_Heur":[mega_pred_heur], "Mega_Final":[mega_final]}).to_excel(writer, sheet_name="Mega_Predictions", index=False)
        pd.DataFrame({"Power_Ensemble":[power_ensemble_pred], "Power_ML":[power_pred_ml], "Power_Heur":[power_pred_heur], "Power_Final":[power_final]}).to_excel(writer, sheet_name="Power_Predictions", index=False)

    print("✅ Report saved at", report_path)

    # attach metrics if available
    extras = []
    if os.path.exists("metrics/mega_metrics.csv"):
        extras.append("metrics/mega_metrics.csv")
    if os.path.exists("metrics/power_metrics.csv"):
        extras.append("metrics/power_metrics.csv")

    # send email if configured
    if os.getenv("EMAIL_USER") and os.getenv("EMAIL_PASS") and os.getenv("EMAIL_TO"):
        try:
            send_email_with_attachments(report_path, extra_files=extras)
        except Exception as e:
            print("⚠ Send email failed:", e)
    else:
        print("⚠ Missing email config; skip send")

    print("=== PIPELINE HOÀN THÀNH ===")

if __name__ == "__main__":
    main()
