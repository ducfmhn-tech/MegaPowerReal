# utils/train_model.py
import os, joblib, numpy as np, pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier
from utils.logger import log

def build_Xy(mega_df, power_df, window=50, max_num=45):
    X=[]; y=[]
    minlen = min(len(mega_df), len(power_df))
    if minlen <= window:
        return None, None
    for end in range(window, minlen):
        mw = mega_df.iloc[end-window:end]
        pw = power_df.iloc[end-window:end]
        m_counts = []
        p_counts = []
        for n in range(1, max_num+1):
            m_counts.append(sum(mw[f"n{i}"].isin([n]).sum() for i in range(1,7)))
            p_counts.append(sum(pw[f"n{i}"].isin([n]).sum() for i in range(1,7)))
        m_counts = np.array(m_counts); p_counts = np.array(p_counts)
        try:
            next_draw = [int(mega_df.iloc[end][f"n{i}"]) for i in range(1,7)]
        except:
            next_draw=[]
        for n in range(1, max_num+1):
            feats = [int(m_counts[n-1]), int(p_counts[n-1]), float(m_counts[n-1]/(window*6)), float(p_counts[n-1]/(window*6))]
            X.append(feats); y.append(1 if n in next_draw else 0)
    return np.array(X), np.array(y)

def train_models_and_save(mega_df, power_df, window=50, save_dir="data", models_dir="models"):
    os.makedirs(models_dir, exist_ok=True)
    X,y = build_Xy(mega_df, power_df, window=window, max_num=45)
    if X is None:
        log("❌ Not enough data to train")
        return None, None, {}
    Xtr, Xval, ytr, yval = train_test_split(X, y, test_size=0.2, random_state=42)
    rf = RandomForestClassifier(n_estimators=300, random_state=42, class_weight="balanced")
    rf.fit(Xtr, ytr)
    rf_path = os.path.join(models_dir, "rf_pernum_mega.joblib")
    joblib.dump(rf, rf_path)
    gb = XGBClassifier(n_estimators=200, use_label_encoder=False, eval_metric="logloss", verbosity=0)
    gb.fit(Xtr, ytr)
    gb_path = os.path.join(models_dir, "gb_pernum_mega.joblib")
    joblib.dump(gb, gb_path)
    ypred_rf = rf.predict(Xval); ypred_gb = gb.predict(Xval)
    acc_rf = accuracy_score(yval, ypred_rf); acc_gb = accuracy_score(yval, ypred_gb)
    metrics = {"acc_rf": acc_rf, "acc_gb": acc_gb}
    log(f"✅ Models trained: RF={acc_rf:.3f}, GB={acc_gb:.3f}")
    return rf_path, gb_path, metrics

def ensemble_predict_topk(mega_df, power_df, rf_path, gb_path, topk=6, window=50):
    import numpy as _np, joblib
    max_mega=45; max_power=55
    mw = mega_df.tail(window); pw = power_df.tail(window)
    m_counts = [sum(mw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_mega+1)]
    p_counts = [sum(pw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_mega+1)]
    Xcur = _np.array([[m_counts[i], p_counts[i], m_counts[i]/(window*6), p_counts[i]/(window*6)] for i in range(max_mega)])
    rf = joblib.load(rf_path) if (rf_path and os.path.exists(rf_path)) else None
    gb = joblib.load(gb_path) if (gb_path and os.path.exists(gb_path)) else None
    probs_rf = rf.predict_proba(Xcur)[:,1] if rf is not None else _np.zeros(max_mega)
    probs_gb = gb.predict_proba(Xcur)[:,1] if gb is not None else _np.zeros(max_mega)
    probs = (probs_rf + probs_gb)/2.0
    idxs = probs.argsort()[-topk:][::-1]; pred_mega = sorted([int(i+1) for i in idxs])
    # power
    mp_counts = [sum(mw[f"n{i}"].isin([n]).sum() for i in range(1,7)) if n<=45 else 0 for n in range(1, max_power+1)]
    pp_counts = [sum(pw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_power+1)]
    Xcur_p = _np.array([[mp_counts[i], pp_counts[i], (mp_counts[i]/(window*6)), (pp_counts[i]/(window*6))] for i in range(max_power)])
    if gb is not None:
        try:
            probs_p = gb.predict_proba(Xcur_p)[:,1]
            idxs_p = probs_p.argsort()[-topk:][::-1]
            pred_power = sorted([int(i+1) for i in idxs_p])
        except:
            score = [pp_counts[i] + 0.3*(mp_counts[i] if i<45 else 0) for i in range(max_power)]
            idxs_p = _np.array(score).argsort()[-topk:][::-1]; pred_power = sorted([int(i+1) for i in idxs_p])
    else:
        score = [pp_counts[i] + 0.3*(mp_counts[i] if i<45 else 0) for i in range(max_power)]
        idxs_p = _np.array(score).argsort()[-topk:][::-1]; pred_power = sorted([int(i+1) for i in idxs_p])
    probs_dict = {"mega_probs": probs.tolist(), "power_scores": pp_counts}
    return pred_mega, pred_power, probs_dict
