import pandas as pd
import os
import sys
from utils.logger import log

def load_saved(save_dir="data"):
    """Tải các file CSV đã lưu về (mega và power) vào DataFrame."""
    mega_path = os.path.join(save_dir, "mega_6_45_raw.csv")
    power_path = os.path.join(save_dir, "power_6_55_raw.csv")
    
    mega_df = pd.DataFrame()
    power_df = pd.DataFrame()

    try:
        if os.path.exists(mega_path):
            mega_df = pd.read_csv(mega_path)
    except Exception as e:
        log(f"⚠ Lỗi khi tải Mega CSV: {e}")
        
    try:
        if os.path.exists(power_path):
            power_df = pd.read_csv(power_path)
    except Exception as e:
        log(f"⚠ Lỗi khi tải Power CSV: {e}")
        
    return mega_df, power_df

def print_head(df, n=5):
    """In n dòng đầu tiên của DataFrame."""
    if df is None or df.empty:
        print("    [DataFrame rỗng]")
        return
    print(df.head(n).to_markdown(index=False))

def quick_validate(df, name, min_rows=30):
    """Thực hiện kiểm tra nhanh DataFrame."""
    if df.empty:
        log(f"❌ Validation {name}: DataFrame rỗng.")
        return False

    # 1. Kiểm tra số lượng dòng tối thiểu
    if len(df) < min_rows:
        log(f"❌ Validation {name}: Chỉ có {len(df)} dòng (Yêu cầu tối thiểu {min_rows}).")
        return False
        
    # 2. Kiểm tra các cột số có tồn tại không
    num_cols = [f"n{i}" for i in range(1, 7)]
    missing_cols = [col for col in num_cols if col not in df.columns]
    if missing_cols:
        log(f"❌ Validation {name}: Thiếu các cột số: {', '.join(missing_cols)}.")
        return False
    
    # 3. Kiểm tra kiểu dữ liệu của các cột số (phải là số nguyên)
    try:
        df[num_cols] = df[num_cols].astype(int)
    except:
        log(f"❌ Validation {name}: Các cột số không thể chuyển đổi sang số nguyên.")
        return False

    log(f"✅ Validation {name}: OK ({len(df)} dòng, có 6 cột số).")
    return True
