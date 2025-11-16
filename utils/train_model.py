# utils/train_model.py
import os
import joblib
import numpy as np
import pandas as pd
from collections import Counter
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier
from utils.logger import log

def compute_counts(df, max_num):
    counts = Counter()
    for _, row in df.iterrows():
        for i in range(1,7):
            try:
                v = int(row.get(f"n{i}"))
                counts[v] += 1
            except:
                pass
    stats = pd.DataFrame([{'num': n, 'count': counts.get(n,0)} for n in range(1, max_num+1)])
    stats['pct'] = (stats['count'] / (len(df)*6)) if len(df) > 0 else 0.0
    return stats

def build_Xy(mega_df, power_df, window=50, max_num=45):
    X = []
    y = []
    minlen = min(len(mega_df), len(power_df))
    if minlen <= window:
        return None, None
    for end in range(window, minlen):
        mw = mega_df.iloc[end-window:end].reset_index(drop=True)
        pw = power_df.iloc[end-window:end].reset_index(drop=True)
        m_stats = compute_counts(mw, max_num).set_index('num')
        p_stats = compute_counts(pw, max_num if max_num<=55 else 55).set_index('num')
        try:
            next_draw = [int(mega_df.iloc[end][f"n{i}"]) for i in range(1,7)]
        except:
            next_draw = []
        for n in range(1, max_num+1):
            feats = [
                int(m_stats.loc[n,'count']) if n in m_stats.index else 0,
                float(m_stats.loc[n,'pct']) if n in m_stats.index else 0.0,
                int(p_stats.loc[n,'count']) if n in p_stats.index else 0,
                float(p_stats.loc[n,'pct']) if n in p_stats.index else 0.0
            ]
            X.append(feats)
            y.append(1 if n in next_draw else 0)
    return np.array(X), np.array(y)

def train_models_and_save(mega_df, power_df, window=50, save_dir="models"):
    os.makedirs(save_dir, exist_ok=True)
    X, y = build_Xy(mega_df, power_df, window=window, max_num=45)
    if X is None:
        log("❌ Not enough data to train")
        return None, None, {}
    Xtr, Xval, ytr, yval = train_test_split(X, y, test_size=0.2, random_state=42)
    rf = RandomForestClassifier(n_estimators=200, random_state=42, class_weight="balanced")
    rf.fit(Xtr, ytr)
    rf_path = os.path.join(save_dir, "rf_pernum.joblib")
    joblib.dump(rf, rf_path)

    gb = XGBClassifier(n_estimators=200, use_label_encoder=False, eval_metric="logloss", verbosity=0)
    gb.fit(Xtr, ytr)
    gb_path = os.path.join(save_dir, "gb_pernum.joblib")
    joblib.dump(gb, gb_path)

    ypred_rf = rf.predict(Xval)
    ypred_gb = gb.predict(Xval)
    acc_rf = accuracy_score(yval, ypred_rf)
    acc_gb = accuracy_score(yval, ypred_gb)
    metrics = {"acc_rf": acc_rf, "acc_gb": acc_gb}
    return rf_path, gb_path, metrics

def ensemble_predict_topk(mega_df, power_df, rf_path, gb_path, topk=6, window=50):
    # build current features for each number 1..45
    max_num = 45
    mw = mega_df.tail(window)
    pw = power_df.tail(window)
    m_counts = [sum(mw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_num+1)]
    p_counts = [sum(pw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_num+1)]
    Xcur = []
    for idx in range(max_num):
        Xcur.append([int(m_counts[idx]), float(m_counts[idx]/(window*6)), int(p_counts[idx]), float(p_counts[idx]/(window*6))])
    Xcur = np.array(Xcur)

    rf = joblib.load(rf_path) if rf_path and os.path.exists(rf_path) else None
    gb = joblib.load(gb_path) if gb_path and os.path.exists(gb_path) else None

    probs_rf = rf.predict_proba(Xcur)[:,1] if rf is not None else np.zeros(len(Xcur))
    probs_gb = gb.predict_proba(Xcur)[:,1] if gb is not None else np.zeros(len(Xcur))
    probs = (probs_rf + probs_gb) / 2.0
    idxs = probs.argsort()[-topk:][::-1]
    pred_nums = sorted([int(i+1) for i in idxs])

    # power: build scores 1..55 using power counts + small mapping from mega
    max_p = 55
    mp_counts = [m_counts[i] if i < len(m_counts) else 0 for i in range(max_p)]
    pp_counts = [0]*max_p
    # compute power counts from power_df
    for n in range(1, max_p+1):
        cnt = 0
        for i in range(1,7):
            if f"n{i}" in power_df.columns:
                cnt += int(power_df[f"n{i}"].tail(window).isin([n]).sum())
        pp_counts[n-1] = cnt
    sp = [pp_counts[i] + 0.2*(mp_counts[i] if i < len(mp_counts) else 0) for i in range(max_p)]
    import numpy as _np
    idxs_p = _np.array(sp).argsort()[-topk:][::-1]
    pred_power = sorted([int(i+1) for i in idxs_p])
    probs_dict = {"mega_probs": probs.tolist(), "power_scores": sp}
    return pred_nums, pred_power, probs_dict
