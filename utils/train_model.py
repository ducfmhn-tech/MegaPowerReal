"""
train_model.py
- Build dataset X,y per-number
- Train RandomForest and GradientBoosting (XGBoost)
- Save models
- Provide ensemble predict top-k
"""

import os
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, roc_auc_score
from xgboost import XGBClassifier


# ===============================
# Build training features (Mega vs Power)
# ===============================
def build_Xy(mega_df, power_df, window=50, max_num=45):
    X, y = [], []
    minlen = min(len(mega_df), len(power_df))
    if minlen <= window:
        print(f"⚠️ Not enough data to build features. Need > {window} rows.")
        return None, None

    for end in range(window, minlen):
        mw = mega_df.iloc[end - window:end]
        pw = power_df.iloc[end - window:end]

        m_counts, p_counts = [], []
        for n in range(1, max_num + 1):
            m_counts.append(sum(mw[f"n{i}"].isin([n]).sum() for i in range(1, 7)))
            p_counts.append(sum(pw[f"n{i}"].isin([n]).sum() for i in range(1, 7)))

        m_counts = np.array(m_counts)
        p_counts = np.array(p_counts)

        try:
            next_draw = [int(mega_df.iloc[end][f"n{i}"]) for i in range(1, 7)]
        except Exception:
            next_draw = []

        for n in range(1, max_num + 1):
            feats = [
                int(m_counts[n - 1]),
                int(p_counts[n - 1]),
                float(m_counts[n - 1] / (window * 6)),
                float(p_counts[n - 1] / (window * 6)),
            ]
            X.append(feats)
            y.append(1 if n in next_draw else 0)

    return np.array(X), np.array(y)


# ===============================
# Train both models (RF + XGB)
# ===============================
def train_models_and_save(mega_df, power_df, window=50,
                          save_dir="data", models_dir="models"):
    os.makedirs(models_dir, exist_ok=True)

    X, y = build_Xy(mega_df, power_df, window=window, max_num=45)
    if X is None or len(X) == 0:
        print("⚠️ Not enough data to train models.")
        return None, None, {}

    Xtr, Xval, ytr, yval = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # RandomForest
    rf = RandomForestClassifier(
        n_estimators=300, random_state=42, class_weight="balanced"
    )
    rf.fit(Xtr, ytr)
    rf_path = os.path.join(models_dir, "rf_pernum_mega.joblib")
    joblib.dump(rf, rf_path)

    # Gradient Boosting (XGBoost)
    gb = XGBClassifier(
        n_estimators=200,
        use_label_encoder=False,
        eval_metric="logloss",
        verbosity=0,
        random_state=42
    )
    gb.fit(Xtr, ytr)
    gb_path = os.path.join(models_dir, "gb_pernum_mega.joblib")
    joblib.dump(gb, gb_path)

    # Validation metrics
    ypred_rf = rf.predict(Xval)
    ypred_gb = gb.predict(Xval)
    acc_rf = accuracy_score(yval, ypred_rf)
    acc_gb = accuracy_score(yval, ypred_gb)

    metrics = {"acc_rf": acc_rf, "acc_gb": acc_gb}
    print(f"✅ RF acc={acc_rf:.3f}, GB acc={acc_gb:.3f}")
    return rf_path, gb_path, metrics


# ===============================
# Ensemble predict (Top-k)
# ===============================
def ensemble_predict_topk(mega_df, power_df,
                          rf_path="models/rf_pernum_mega.joblib",
                          gb_path="models/gb_pernum_mega.joblib",
                          topk=6, window=50):
    max_num = 45
    max_p = 55

    if len(mega_df) < window or len(power_df) < window:
        print("⚠️ Not enough data for prediction.")
        return [], [], {}

    mw = mega_df.tail(window)
    pw = power_df.tail(window)

    m_counts = [sum(mw[f"n{i}"].isin([n]).sum() for i in range(1, 7)) for n in range(1, max_num + 1)]
    p_counts = [sum(pw[f"n{i}"].isin([n]).sum() for i in range(1, 7)) for n in range(1, max_num + 1)]

    Xcur = np.array([
        [int(m_counts[idx]), int(p_counts[idx]),
         float(m_counts[idx] / (window * 6)),
         float(p_counts[idx] / (window * 6))]
        for idx in range(max_num)
    ])

    rf = joblib.load(rf_path) if rf_path and os.path.exists(rf_path) else None
    gb = joblib.load(gb_path) if gb_path and os.path.exists(gb_path) else None

    if rf is None or gb is None:
        print("⚠️ Models not found — please train first.")
        return [], [], {}

    probs_rf = rf.predict_proba(Xcur)[:, 1]
    probs_gb = gb.predict_proba(Xcur)[:, 1]
    probs = (probs_rf + probs_gb) / 2.0

    # Mega prediction
    idxs = probs.argsort()[-topk:][::-1]
    pred_nums = sorted([int(i + 1) for i in idxs])

    # Power prediction (1..55)
    mp_counts = [sum(mw[f"n{i}"].isin([n]).sum() for i in range(1, 7)) if n <= 45 else 0 for n in range(1, max_p + 1)]
    pp_counts = [sum(pw[f"n{i}"].isin([n]).sum() for i in range(1, 7)) for n in range(1, max_p + 1)]
    sp = [pp_counts[i] + 0.3 * mp_counts[i] for i in range(max_p)]
    idxs_p = np.array(sp).argsort()[-topk:][::-1]
    pred_power = sorted([int(i + 1) for i in idxs_p])

    probs_dict = {"mega_probs": probs.tolist(), "power_scores": sp}
    return pred_nums, pred_power, probs_dict
