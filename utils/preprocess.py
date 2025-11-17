import pandas as pd
import os
from utils.logger import log

def _sort_row_numbers(row):
    """Sắp xếp lại các con số n1-n6 trong một hàng theo thứ tự tăng dần."""
    # Lấy 6 cột số
    try:
        # Chắc chắn là int trước khi sắp xếp
        nums = [int(row[f"n{i}"]) for i in range(1,7)]
    except ValueError:
        # Nếu có giá trị non-numeric (NaN, pd.NA, etc.) thì bỏ qua hàng này
        return pd.Series(row) 
        
    nums_sorted = sorted(nums)
    
    # Gán lại giá trị đã sắp xếp vào các cột n1, n2, ...
    for i,v in enumerate(nums_sorted, start=1):
        row[f"n{i}"] = v
        
    return pd.Series(row)

def preprocess_dfs(mega_df, power_df, save_dir="data"):
    """
    Chuẩn hóa và làm sạch DataFrames: đảm bảo cột số, sắp xếp số và loại bỏ trùng lặp.
    """
    log("    -> Bắt đầu tiền xử lý...")
    
    # 1. Đảm bảo các cột n1-n6 tồn tại
    for df in (mega_df, power_df):
        for i in range(1,7):
            if f"n{i}" not in df.columns:
                df[f"n{i}"] = pd.NA
                
    # 2. Loại bỏ các hàng thiếu ngày
    mega_df = mega_df.dropna(subset=["date", "n1", "n2", "n3", "n4", "n5", "n6"]).reset_index(drop=True)
    power_df = power_df.dropna(subset=["date", "n1", "n2", "n3", "n4", "n5", "n6"]).reset_index(drop=True)
    
    # Chuyển đổi các cột số sang integer (đảm bảo _sort_row_numbers hoạt động)
    num_cols = [f"n{i}" for i in range(1, 7)]
    try:
        mega_df[num_cols] = mega_df[num_cols].astype(int)
        power_df[num_cols] = power_df[num_cols].astype(int)
    except Exception as e:
        log(f"⚠ Lỗi chuyển đổi cột số sang INT: {e}. Dữ liệu có thể không sạch.")
        
    # 3. Thực hiện sắp xếp số trên từng hàng
    mega_df = mega_df.apply(_sort_row_numbers, axis=1)
    power_df = power_df.apply(_sort_row_numbers, axis=1)
    
    # 4. Loại bỏ các bản ghi trùng lặp (trùng ngày và 6 con số)
    mega_df = mega_df.drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"]).reset_index(drop=True)
    power_df = power_df.drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"]).reset_index(drop=True)
    
    log(f"    -> Mega sau xử lý: {len(mega_df)} dòng | Power sau xử lý: {len(power_df)} dòng")
    
    # 5. Lưu lại kết quả đã làm sạch
    os.makedirs(save_dir, exist_ok=True)
    mega_df.to_csv(os.path.join(save_dir, "mega_6_45_raw.csv"), index=False)
    power_df.to_csv(os.path.join(save_dir, "power_6_55_raw.csv"), index=False)
    
    return mega_df, power_df
