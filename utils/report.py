import os, pandas as pd
from datetime import datetime
from utils.logger import log
import io # Dùng cho StringIO nếu cần (mặc dù đã dùng ExcelWriter)

def generate_report(mega_df, power_df, metrics, save_dir, pred_mega=None, pred_power=None):
    """
    Tạo một báo cáo Excel chứa dữ liệu thô, các chỉ số của mô hình và kết quả dự đoán.
    
    Args:
        mega_df (pd.DataFrame): Dữ liệu cho Mega 6/45.
        power_df (pd.DataFrame): Dữ liệu cho Power 6/55.
        metrics (dict): Các chỉ số huấn luyện và đánh giá mô hình.
        save_dir (str): Thư mục lưu báo cáo.
        pred_mega (list): Các con số dự đoán cuối cùng cho Mega.
        pred_power (list): Các con số dự đoán cuối cùng cho Power.
        
    Returns:
        str or None: Đường dẫn tới file Excel đã tạo nếu thành công, None nếu thất bại.
    """
    os.makedirs(save_dir, exist_ok=True)
    # Tạo tên file duy nhất với timestamp UTC
    fname = f"mega_power_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    path = os.path.join(save_dir, fname)
    
    log(f"🔹 Đang tạo báo cáo tại {path}...")
    
    try:
        # Sử dụng pd.ExcelWriter với 'openpyxl' engine
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            
            # --- Sheet 1 & 2: Dữ liệu ---
            if mega_df is not None and not mega_df.empty:
                mega_df.to_excel(writer, sheet_name="Mega_raw", index=False)
            else:
                pd.DataFrame({"Status": ["Không tìm thấy dữ liệu Mega"]}).to_excel(writer, sheet_name="Mega_raw", index=False)

            if power_df is not None and not power_df.empty:
                power_df.to_excel(writer, sheet_name="Power_raw", index=False)
            else:
                pd.DataFrame({"Status": ["Không tìm thấy dữ liệu Power"]}).to_excel(writer, sheet_name="Power_raw", index=False)
                
            # --- Sheet 3: Tóm tắt và Metadata ---
            meta = {
                "Key": ["Predicted Mega Numbers", "Predicted Power Numbers", "Model Metrics", "Report Generated At (UTC)"],
                "Value": [
                    ", ".join(map(str, pred_mega or [])),
                    ", ".join(map(str, pred_power or [])),
                    str(metrics or {}),
                    datetime.utcnow().isoformat()
                ]
            }
            pd.DataFrame(meta).to_excel(writer, sheet_name="Summary", index=False, header=True)
            
        log(f"✅ Báo cáo đã lưu thành công tới {path}")
        return path
    except Exception as e:
        log(f"⚠ Lỗi tạo báo cáo: {e}")
        return None
