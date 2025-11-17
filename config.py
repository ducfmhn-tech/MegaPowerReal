import os
from pathlib import Path

# --- Directory Configuration ---
ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
MODELS_DIR = ROOT_DIR / "models"
REPORTS_DIR = ROOT_DIR / "reports"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

# --- Data Parameters ---
# Số lượng dòng dữ liệu tối thiểu cần lấy về
FETCH_LIMIT = 120 

# --- Model Parameters ---
# Kích thước cửa sổ (số lượng bản ghi lịch sử) dùng để tính tần suất
MODEL_WINDOW_SIZE = 50 
# Số lượng con số được dự đoán (thường là 6)
PREDICTION_TOP_K = 6 
# Số lượng tối đa của Mega 6/45
MAX_NUM_MEGA = 45 

# --- Email Configuration (Lấy từ biến môi trường) ---
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")
GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
