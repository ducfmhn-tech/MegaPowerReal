#!/usr/bin/env bash
#
# Script để chạy quá trình thu thập và kiểm tra dữ liệu sơ bộ.
#

set -e

# Đảm bảo thư mục 'data' tồn tại
mkdir -p data

echo "=== BƯỚC 1: Thu thập Dữ liệu Xổ số (limit=120) ==="
# Sử dụng Python để gọi hàm fetch_all_data
python -c "from utils.fetch_data import fetch_all_data; import sys; sys.path.append('.'); fetch_all_data(limit=120, save_dir='data')"

echo ""
echo "=== BƯỚC 2: Tải Dữ liệu đã Lưu và Kiểm tra Nhanh ==="
# Sử dụng Python để tải dữ liệu, in header và kiểm tra tính hợp lệ cơ bản
python -c """
from utils.fetch_checks import load_saved, quick_validate, print_head
import sys
sys.path.append('.') # Đảm bảo import local hoạt động

# Tải DataFrame đã lưu
m, p = load_saved('data')

# In 5 dòng đầu
print('\\n--- Mega 6/45 Head ---')
print_head(m)
print('\\n--- Power 6/55 Head ---')
print_head(p)

# Chạy kiểm tra nhanh (ví dụ: số lượng dòng tối thiểu, kiểu dữ liệu)
quick_validate(m, 'mega')
quick_validate(p, 'power')

print('\\n✅ Thu thập và kiểm tra dữ liệu hoàn tất.')
"""
