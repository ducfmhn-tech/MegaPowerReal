# utils/train_model.py
import os, joblib, numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import pandas as pd
from utils.logger import log

try:
    from xgboost import XGBClassifier
    HAS_XGB = True
except:
    HAS_XGB = False

def build_Xy(mega_df, power_df, window=50, max_num=45):
    minlen = min(len(mega_df), len(power_df))
    if minlen <= window:
        return None, None
    X=[]
    y=[]
    for end in range(window, minlen):
        mw = mega_df.iloc[end-window:end]
        pw = power_df.iloc[end-window:end]
        m_counts = [0]*max_num
        p_counts = [0]*max_num
        for i in range(1,7):
            if f"n{i}" in mw.columns:
                for v in mw[f"n{i}"].dropna().astype(int).tolist():
                    if 1<=v<=max_num:
                        m_counts[v-1]+=1
            if f"n{i}" in pw.columns:
                for v in pw[f"n{i}"].dropna().astype(int).tolist():
                    if 1<=v<=55:
                        if v<=max_num:
                            p_counts[v-1]+=1
        for n in range(1,max_num+1):
            feat = [m_counts[n-1], p_counts[n-1], m_counts[n-1]/(window*6), p_counts[n-1]/(window*6)]
            X.append(feat)
        try:
            next_draw = [int(mega_df.iloc[end][f"n{i}"]) for i in range(1,7)]
        except:
            next_draw=[]
        for n in range(1,max_num+1):
            y.append(1 if n in next_draw else 0)
    return np.array(X), np.array(y)

def train_models_and_save(mega_df, power_df, window=50, save_dir="models"):
    os.makedirs(save_dir, exist_ok=True)
    X,y = build_Xy(mega_df, power_df, window=window)
    if X is None:
        log("❌ Not enough data to train.")
        return None, None, {}
    Xtr,Xval,ytr,yval = train_test_split(X,y,test_size=0.2,random_state=42)
    rf = RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=-1)
    rf.fit(Xtr,ytr)
    rf_path = os.path.join(save_dir,"rf_pernum_mega.joblib")
    joblib.dump(rf,rf_path)

    metrics = {}
    metrics["acc_rf"] = accuracy_score(yval, rf.predict(Xval))

    gb_path = None
    if HAS_XGB:
        gb = XGBClassifier(n_estimators=200, use_label_encoder=False, eval_metric="logloss", verbosity=0)
        gb.fit(Xtr,ytr)
        gb_path = os.path.join(save_dir,"gb_pernum_mega.joblib")
        joblib.dump(gb,gb_path)
        metrics["acc_gb"] = accuracy_score(yval, gb.predict(Xval))

    return rf_path, gb_path, metrics

def ensemble_predict_topk(mega_df, power_df, rf_path=None, gb_path=None, topk=6, window=50):
    max_num = 45
    mw = mega_df.tail(window)
    pw = power_df.tail(window)
    m_counts = [0]*max_num
    p_counts = [0]*max_num
    for i in range(1,7):
        if f"n{i}" in mw.columns:
            for v in mw[f"n{i}"].dropna().astype(int).tolist():
                if 1<=v<=max_num:
                    m_counts[v-1]+=1
        if f"n{i}" in pw.columns:
            for v in pw[f"n{i}"].dropna().astype(int).tolist():
                if 1<=v<=55:
                    if v<=max_num:
                        p_counts[v-1]+=1
    Xcur = []
    for idx in range(max_num):
        Xcur.append([m_counts[idx], p_counts[idx], m_counts[idx]/(window*6), p_counts[idx]/(window*6)])
    Xcur = np.array(Xcur)
    rf = joblib.load(rf_path) if rf_path and os.path.exists(rf_path) else None
    gb = joblib.load(gb_path) if gb_path and gb_path is not None and os.path.exists(gb_path) else None

    if rf is None and gb is None:
        score = np.array(m_counts) + 0.3*np.array(p_counts)
    else:
        probs_rf = rf.predict_proba(Xcur)[:,1] if rf is not None else 0
        probs_gb = gb.predict_proba(Xcur)[:,1] if (gb is not None and HAS_XGB) else 0
        if gb is not None:
            score = (probs_rf + probs_gb)/2.0
        else:
            score = probs_rf

    idxs = score.argsort()[-topk:][::-1]
    pred_nums = sorted([int(i+1) for i in idxs])

    # power
    max_p = 55
    pp_counts = [0]*max_p
    for i in range(1,7):
        if f"n{i}" in pw.columns:
            for v in pw[f"n{i}"].dropna().astype(int).tolist():
                if 1<=v<=max_p:
                    pp_counts[v-1]+=1
    sp = np.array(pp_counts) + 0.2*np.array([m_counts[i] if i < len(m_counts) else 0 for i in range(max_p)])
    idxs_p = sp.argsort()[-topk:][::-1]
    pred_power = sorted([int(i+1) for i in idxs_p])

    probs_dict = {"mega_scores": score.tolist(), "power_scores": sp.tolist()}
    return pred_nums, pred_power, probs_dict
