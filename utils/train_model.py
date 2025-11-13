"""
train_model.py
- Build dataset X,y per-number
- Train RandomForest and GradientBoosting (XGBoost)
- Save models
- Provide ensemble predict top-k
"""
import os, joblib, numpy as np, pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier

def build_Xy(mega_df, power_df, window=50, max_num=45):
    X = []
    y = []
    minlen = min(len(mega_df), len(power_df))
    if minlen <= window:
        return None, None
    for end in range(window, minlen):
        mw = mega_df.iloc[end-window:end]
        pw = power_df.iloc[end-window:end]
        # counts
        m_counts = []
        p_counts = []
        for n in range(1, max_num+1):
            m_counts.append(sum(mw[f"n{i}"].isin([n]).sum() for i in range(1,7)))
            p_counts.append(sum(pw[f"n{i}"].isin([n]).sum() for i in range(1,7)))
        # normalized
        m_counts = np.array(m_counts)
        p_counts = np.array(p_counts)
        # next draw (label)
        try:
            next_draw = [int(mega_df.iloc[end][f"n{i}"]) for i in range(1,7)]
        except:
            next_draw = []
        for n in range(1, max_num+1):
            feats = [int(m_counts[n-1]), int(p_counts[n-1]), float(m_counts[n-1]/(window*6)), float(p_counts[n-1]/(window*6))]
            X.append(feats)
            y.append(1 if n in next_draw else 0)
    return np.array(X), np.array(y)

def train_models_and_save(mega_df, power_df, window=50, save_dir="data", models_dir="models"):
    os.makedirs(models_dir, exist_ok=True)
    # Mega models
    X, y = build_Xy(mega_df, power_df, window=window, max_num=45)
    if X is None:
        print("Not enough data to train")
        return None, None, {}
    Xtr, Xval, ytr, yval = train_test_split(X, y, test_size=0.2, random_state=42)
    rf = RandomForestClassifier(n_estimators=300, random_state=42, class_weight="balanced")
    rf.fit(Xtr, ytr)
    rf_path = os.path.join(models_dir, "rf_pernum_mega.joblib")
    joblib.dump(rf, rf_path)
    # gradient boosting via xgboost
    gb = XGBClassifier(n_estimators=200, use_label_encoder=False, eval_metric="logloss", verbosity=0)
    gb.fit(Xtr, ytr)
    gb_path = os.path.join(models_dir, "gb_pernum_mega.joblib")
    joblib.dump(gb, gb_path)
    # metrics on val
    from sklearn.metrics import roc_auc_score, accuracy_score
    ypred_rf = rf.predict(Xval)
    ypred_gb = gb.predict(Xval)
    acc_rf = accuracy_score(yval, ypred_rf)
    acc_gb = accuracy_score(yval, ypred_gb)
    metrics = {"acc_rf": acc_rf, "acc_gb": acc_gb}
    print("Trained RF acc:", acc_rf, "GB acc:", acc_gb)
    return rf_path, gb_path, metrics

def ensemble_predict_topk(mega_df, power_df, rf_path, gb_path, topk=6, save_dir="data"):
    # build current features for each number
    import numpy as np
    # for mega numbers 1..45
    max_num = 45
    # compute counts across last window saved in config via features; fallback window=50
    window = 50
    # reuse build_Xy pattern but for current state: counts based on tail(window)
    mw = mega_df.tail(window)
    pw = power_df.tail(window)
    m_counts = [sum(mw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_num+1)]
    p_counts = [sum(pw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_num+1)]
    Xcur = []
    for idx in range(max_num):
        Xcur.append([int(m_counts[idx]), int(p_counts[idx]), float(m_counts[idx]/(window*6)), float(p_counts[idx]/(window*6))])
    Xcur = np.array(Xcur)
    rf = joblib.load(rf_path) if os.path.exists(rf_path) else None
    gb = joblib.load(gb_path) if os.path.exists(gb_path) else None
    probs_rf = rf.predict_proba(Xcur)[:,1] if rf is not None else np.zeros(len(Xcur))
    probs_gb = gb.predict_proba(Xcur)[:,1] if gb is not None else np.zeros(len(Xcur))
    probs = (probs_rf + probs_gb) / 2.0
    # topk indices
    idxs = probs.argsort()[-topk:][::-1]
    pred_nums = sorted([int(i+1) for i in idxs])
    # Power predictions: for 1..55, simpler: use frequency from power only + mapped ensemble
    # build power current features
    max_p = 55
    mp_counts = [sum(mw[f"n{i}"].isin([n]).sum() for i in range(1,7)) if n<=45 else 0 for n in range(1, max_p+1)]
    pp_counts = [sum(pw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_p+1)]
    sp = [pp_counts[i] + 0.3*mp_counts[i] for i in range(max_p)]
    # choose topk power
    import numpy as np 
    idxs_p = np.array(sp).argsort()[-topk:][::-1]
    pred_power = sorted([int(i+1) for i in idxs_p])
    # return
    probs_dict = {"mega_probs": probs.tolist(), "power_scores": sp}
    return pred_nums, pred_power, probs_dict
 
