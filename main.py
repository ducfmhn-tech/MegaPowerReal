import pandas as pd
from utils.logger import log
import config
from utils.fetch_data import fetch_all_data
from utils.preprocess import preprocess_dfs
from utils.train_model import train_models_and_save, ensemble_predict_topk
from utils.report import generate_report
from utils.email_utils import send_report

def run_pipeline():
    log("=== BẮT ĐẦU PIPELINE DỰ ĐOÁN MEGA/POWER ===")

    # 1. THU THẬP DỮ LIỆU
    log("--- BƯỚC 1: Thu thập dữ liệu từ nhiều nguồn ---")
    mega_raw, power_raw = fetch_all_data(limit=config.FETCH_LIMIT, save_dir=str(config.DATA_DIR))
    
    if mega_raw.empty or power_raw.empty:
        log("❌ Dữ liệu không đủ. Dừng pipeline.")
        return

    # 2. TIỀN XỬ LÝ DỮ LIỆU
    log("--- BƯỚC 2: Tiền xử lý và Chuẩn hóa dữ liệu ---")
    mega_df, power_df = preprocess_dfs(mega_raw, power_raw, save_dir=str(config.DATA_DIR))
    log(f"Mega: {len(mega_df)} rows | Power: {len(power_df)} rows sau tiền xử lý.")

    # 3. HUẤN LUYỆN MÔ HÌNH VÀ ĐÁNH GIÁ
    log("--- BƯỚC 3: Huấn luyện mô hình dự đoán (RandomForest + XGBoost) ---")
    rf_path, gb_path, metrics = train_models_and_save(
        mega_df, 
        power_df, 
        window=config.MODEL_WINDOW_SIZE, 
        save_dir=str(config.MODELS_DIR)
    )
    log(f"Metrics: {metrics}")

    # 4. DỰ ĐOÁN
    log("--- BƯỚC 4: Dự đoán 6 con số may mắn tiếp theo ---")
    pred_mega, pred_power, probs = ensemble_predict_topk(
        mega_df, 
        power_df, 
        rf_path=rf_path, 
        gb_path=gb_path, 
        topk=config.PREDICTION_TOP_K, 
        window=config.MODEL_WINDOW_SIZE
    )
    
    log(f"🔥 DỰ ĐOÁN MEGA 6/45: {pred_mega}")
    log(f"🔥 DỰ ĐOÁN POWER 6/55: {pred_power}")

    # 5. TẠO BÁO CÁO
    log("--- BƯỚC 5: Tạo báo cáo Excel tổng hợp ---")
    report_path = generate_report(
        mega_df, 
        power_df, 
        metrics, 
        str(config.REPORTS_DIR), 
        pred_mega, 
        pred_power
    )

    # 6. GỬI EMAIL BÁO CÁO
    if report_path and config.EMAIL_RECEIVER:
        log("--- BƯỚC 6: Gửi email báo cáo ---")
        subject = f"Vietlott Prediction Report | Mega: {pred_mega} | Power: {pred_power}"
        body = (
            "Dự đoán cho kỳ quay tiếp theo đã sẵn sàng.\n"
            f"Mega 6/45 (Top {config.PREDICTION_TOP_K}): {pred_mega}\n"
            f"Power 6/55 (Top {config.PREDICTION_TOP_K}): {pred_power}\n\n"
            f"Chi tiết và Metrics được đính kèm trong file {os.path.basename(report_path)}"
        )
        send_report(subject, body, report_path)
    else:
        log("⚠ Bỏ qua bước gửi email do thiếu cấu hình hoặc báo cáo.")

    log("=== PIPELINE HOÀN THÀNH ===")

if __name__ == "__main__":
    run_pipeline()
