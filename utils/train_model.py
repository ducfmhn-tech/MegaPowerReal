import os, joblib, numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import pandas as pd
from utils.logger import log

try:
    from xgboost import XGBClassifier
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    log("⚠ Thư viện XGBoost không được tìm thấy. Chỉ sử dụng RandomForest.")

def build_Xy(mega_df, power_df, window=50, max_num=45):
    """
    Xây dựng ma trận đặc trưng (X) và nhãn (y) cho mô hình dự đoán theo số (per-number prediction).
    X là tần suất của từng số trong window, y là 1 nếu số đó xuất hiện trong lượt quay tiếp theo.
    """
    # Lấy độ dài tối thiểu của 2 df
    minlen = min(len(mega_df), len(power_df))
    if minlen <= window:
        log(f"    -> Không đủ dữ liệu (chỉ có {minlen} dòng) cho window={window}.")
        return None, None
        
    X = []
    y = []
    
    # Lặp qua lịch sử từ điểm bắt đầu của cửa sổ
    for end in range(window, minlen):
        # Lấy cửa sổ lịch sử
        mw = mega_df.iloc[end-window:end]
        pw = power_df.iloc[end-window:end]
        
        # Tính tần suất của từng số trong cửa sổ
        m_counts = [0] * max_num  # Tần suất Mega (1-45)
        p_counts = [0] * 55      # Tần suất Power (1-55)
        
        for i in range(1, 7):
            # Tính tần suất Mega
            if f"n{i}" in mw.columns:
                for v in mw[f"n{i}"].dropna().astype(int).tolist():
                    if 1 <= v <= max_num:
                        m_counts[v-1] += 1
            # Tính tần suất Power (full 55)
            if f"n{i}" in pw.columns:
                for v in pw[f"n{i}"].dropna().astype(int).tolist():
                    if 1 <= v <= 55:
                        p_counts[v-1] += 1

        # Xây dựng ma trận X (Feature Matrix)
        for n in range(1, max_num + 1):
            # Tần suất Power, chỉ lấy 45 số đầu cho Mega model
            power_count_mapped = p_counts[n-1] if n-1 < len(p_counts) else 0

            feat = [
                m_counts[n-1],                           # 1. Tần suất tuyệt đối Mega
                power_count_mapped,                      # 2. Tần suất tuyệt đối Power (cho số n)
                m_counts[n-1] / (window * 6),            # 3. Tần suất chuẩn hóa Mega
                power_count_mapped / (window * 6)        # 4. Tần suất chuẩn hóa Power
            ]
            X.append(feat)

        # Xây dựng vector y (Target Labels) - Lượt quay tiếp theo
        try:
            next_draw = {int(mega_df.iloc[end][f"n{i}"]) for i in range(1, 7)}
        except Exception:
            next_draw = set()

        for n in range(1, max_num + 1):
            # Nhãn là 1 nếu số n xuất hiện trong lượt quay tiếp theo, 0 nếu không
            y.append(1 if n in next_draw else 0)
            
    return np.array(X), np.array(y)

def train_models_and_save(mega_df, power_df, window=50, save_dir="models"):
    """
    Huấn luyện Random Forest và (nếu có) XGBoost, sau đó lưu mô hình và trả về metrics.
    """
    log("    -> Xây dựng X và y...")
    X, y = build_Xy(mega_df, power_df, window=window)
    
    if X is None or y is None or len(X) == 0:
        log("❌ Dữ liệu không đủ hoặc có lỗi khi xây dựng X/y.")
        return None, None, {}
        
    log(f"    -> Kích thước tập huấn luyện: X={X.shape}, y={y.shape}")
    Xtr, Xval, ytr, yval = train_test_split(X, y, test_size=0.2, random_state=42)
    
    metrics = {}
    
    # 1. Random Forest (RF)
    log("    -> Huấn luyện Random Forest...")
    rf = RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=-1, max_depth=10)
    rf.fit(Xtr, ytr)
    rf_path = os.path.join(save_dir, "rf_pernum_mega.joblib")
    joblib.dump(rf, rf_path)
    metrics["acc_rf"] = accuracy_score(yval, rf.predict(Xval))

    # 2. XGBoost (GB)
    gb_path = None
    if HAS_XGB:
        log("    -> Huấn luyện XGBoost...")
        gb = XGBClassifier(n_estimators=200, use_label_encoder=False, 
                           eval_metric="logloss", verbosity=0, random_state=42, 
                           n_jobs=-1)
        gb.fit(Xtr, ytr)
        gb_path = os.path.join(save_dir, "gb_pernum_mega.joblib")
        joblib.dump(gb, gb_path)
        metrics["acc_gb"] = accuracy_score(yval, gb.predict(Xval))
    
    log(f"    -> Huấn luyện hoàn tất. RF Accuracy: {metrics.get('acc_rf'):.4f}")
    
    return rf_path, gb_path, metrics

def ensemble_predict_topk(mega_df, power_df, rf_path=None, gb_path=None, topk=6, window=50):
    """
    Sử dụng mô hình và heuristic để dự đoán 6 con số cho Mega và Power.
    """
    log("    -> Bắt đầu dự đoán...")

    # --- 1. DỰ ĐOÁN MEGA (Sử dụng Model Ensemble) ---
    max_num_mega = 45
    
    # Tính tần suất trên cửa sổ cuối cùng (window)
    mw = mega_df.tail(window)
    pw = power_df.tail(window)
    
    m_counts = [0] * max_num_mega
    p_counts_mega = [0] * max_num_mega
    p_counts_full = [0] * 55 # Tính full cho Power Heuristic

    for i in range(1, 7):
        if f"n{i}" in mw.columns:
            for v in mw[f"n{i}"].dropna().astype(int).tolist():
                if 1 <= v <= max_num_mega:
                    m_counts[v-1] += 1
        
        if f"n{i}" in pw.columns:
            for v in pw[f"n{i}"].dropna().astype(int).tolist():
                if 1 <= v <= 55:
                    p_counts_full[v-1] += 1
                    if 1 <= v <= max_num_mega:
                        p_counts_mega[v-1] += 1


    # Chuẩn bị ma trận đặc trưng cho lần dự đoán hiện tại
    Xcur = []
    for idx in range(max_num_mega):
        m_c = m_counts[idx]
        p_c = p_counts_mega[idx]
        Xcur.append([m_c, p_c, m_c / (window * 6), p_c / (window * 6)])
    Xcur = np.array(Xcur)

    # Tải mô hình
    rf = joblib.load(rf_path) if rf_path and os.path.exists(rf_path) else None
    gb = joblib.load(gb_path) if gb_path and os.path.exists(gb_path) and HAS_XGB else None

    # Tính điểm
    if rf is None and gb is None:
        log("    -> Cảnh báo: Không tìm thấy mô hình. Dùng Heuristic cơ bản cho Mega.")
        # Heuristic Fallback: ưu tiên Mega + một phần Power
        score_mega = np.array(m_counts) + 0.3 * np.array(p_counts_mega)
    else:
        probs_rf = rf.predict_proba(Xcur)[:,1] if rf is not None else 0
        probs_gb = gb.predict_proba(Xcur)[:,1] if gb is not None else 0
        
        # Ensemble: Lấy trung bình các xác suất dự đoán
        count = (1 if rf is not None else 0) + (1 if gb is not None else 0)
        score_mega = (probs_rf + probs_gb) / count

    # Chọn Top K cho Mega
    idxs_mega = score_mega.argsort()[-topk:][::-1]
    pred_nums_mega = sorted([int(i + 1) for i in idxs_mega])


    # --- 2. DỰ ĐOÁN POWER (Sử dụng Heuristic dựa trên tần suất) ---
    max_num_power = 55
    
    # Power Heuristic Score: Tần suất Power + trọng số nhỏ từ tần suất Mega
    # Sử dụng p_counts_full đã tính ở trên.
    score_power = np.array(p_counts_full)
    
    # Thêm trọng số từ Mega (chỉ cho các số 1-45, phần còn lại là 0)
    mega_weights = np.array([m_counts[i] * 0.2 if i < max_num_mega else 0 for i in range(max_num_power)])
    score_power = score_power + mega_weights

    # Chọn Top K cho Power
    idxs_power = score_power.argsort()[-topk:][::-1]
    pred_nums_power = sorted([int(i + 1) for i in idxs_power])

    probs_dict = {"mega_scores": score_mega.tolist(), "power_scores": score_power.tolist()}
    
    log("    -> Dự đoán hoàn tất.")
    return pred_nums_mega, pred_nums_power, probs_dict
