import os
import pandas as pd
from utils.fetch_data import fetch_all_data
from utils.fetch_checks import load_saved, quick_validate

def test_fetch_and_validate():
    """
    Kiểm tra quá trình thu thập dữ liệu: đảm bảo dữ liệu được lấy về, 
    đúng định dạng (DataFrame) và đạt số lượng tối thiểu.
    """
    TEMP_SAVE_DIR = "test_data_artifacts"
    MIN_ROWS = 30 # Yêu cầu tối thiểu để đảm bảo tính ổn định

    # 1. Thu thập dữ liệu
    print("🔹 Đang chạy fetch_all_data...")
    mega, power = fetch_all_data(limit=120, save_dir=TEMP_SAVE_DIR)

    # --- Assert Dataframes Exist and are Correct Type ---
    assert mega is not None, "Mega DataFrame fetch failed (returned None)."
    assert power is not None, "Power DataFrame fetch failed (returned None)."
    
    assert isinstance(mega, pd.DataFrame), "Mega is not a pandas DataFrame."
    assert isinstance(power, pd.DataFrame), "Power is not a pandas DataFrame."
    
    # --- Assert Minimum Row Counts using quick_validate ---
    assert quick_validate(mega, 'Mega Test', MIN_ROWS), f"Mega rows too few or invalid: {len(mega)}"
    assert quick_validate(power, 'Power Test', MIN_ROWS), f"Power rows too few or invalid: {len(power)}"

    # Tùy chọn: Xóa thư mục tạm sau khi test
    # import shutil
    # if os.path.exists(TEMP_SAVE_DIR):
    #     shutil.rmtree(TEMP_SAVE_DIR)

    print("✅ Test thu thập dữ liệu thành công.")
