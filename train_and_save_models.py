# train_and_save_models.py
import os
import numpy as np
import pandas as pd
from pathlib import Path
from utils.fetch_data import fetch_all_sources
from utils.predict_advanced import build_count_features, train_lightgbm, train_catboost, train_mlp, save_model
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

MODEL_DIR = Path("models")
METRICS_DIR = Path("metrics")
MODEL_DIR.mkdir(parents=True, exist_ok=True)
METRICS_DIR.mkdir(parents=True, exist_ok=True)

def evaluate_model(m, X_test, Y_test):
    preds = m.predict(X_test)
    res = {}
    for i in range(6):
        res[f"acc_pos{i+1}"] = float(accuracy_score(Y_test[:,i], preds[:,i]))
    res["exact_match"] = float((preds == Y_test).all(axis=1).mean())
    return res

def train_and_eval(name, urls, max_num, use_lgb=True, use_cat=True, use_mlp=True):
    print(f"=== Train for {name} ===")
    df = fetch_all_sources(urls, limit=400)
    if df is None or len(df) < 60:
        print("Not enough data for", name)
        return

    X, Y = build_count_features(df, window=50, max_num=max_num)
    if X is None:
        print("No features for", name)
        return

    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
    metrics_rows = []

    if use_lgb:
        try:
            print("Training LightGBM...")
            m_lgb = train_lightgbm(X_train, Y_train, n_estimators=100)
            save_model(m_lgb, str(MODEL_DIR / f"{name}_lgb.joblib"))
            metrics_rows.append({"model":"lgb", **evaluate_model(m_lgb, X_test, Y_test)})
            print("LightGBM saved.")
        except Exception as e:
            print("LightGBM failed:", e)

    if use_cat:
        try:
            print("Training CatBoost...")
            m_cat = train_catboost(X_train, Y_train, iterations=100, verbose=False)
            save_model(m_cat, str(MODEL_DIR / f"{name}_cat.joblib"))
            metrics_rows.append({"model":"cat", **evaluate_model(m_cat, X_test, Y_test)})
            print("CatBoost saved.")
        except Exception as e:
            print("CatBoost failed:", e)

    if use_mlp:
        try:
            print("Training MLP...")
            m_mlp = train_mlp(X_train, Y_train, hidden_layer_sizes=(128,64), max_iter=200)
            save_model(m_mlp, str(MODEL_DIR / f"{name}_mlp.joblib"))
            metrics_rows.append({"model":"mlp", **evaluate_model(m_mlp, X_test, Y_test)})
            print("MLP saved.")
        except Exception as e:
            print("MLP failed:", e)

    # save last feature for quick ensemble prediction
    np.save(MODEL_DIR / f"{name}_last_feat.npy", X[-1])

    if metrics_rows:
        pd.DataFrame(metrics_rows).to_csv(METRICS_DIR / f"{name}_metrics.csv", index=False)
        print("Metrics saved at", METRICS_DIR / f"{name}_metrics.csv")
    else:
        print("No metrics produced.")

if __name__ == "__main__":
    mega_urls = [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
        "https://www.lotto-8.com/Vietnam/listltoVM45.asp",
    ]
    power_urls = [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html",
        "https://www.lotto-8.com/Vietnam/listltoVM55.asp",
    ]
    train_and_eval("mega", mega_urls, max_num=45, use_lgb=True, use_cat=True, use_mlp=True)
    train_and_eval("power", power_urls, max_num=55, use_lgb=True, use_cat=True, use_mlp=True)
