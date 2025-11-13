# MegaPowerReal

Pipeline tự động lấy kết quả Mega 6/45 & Power 6/55, huấn luyện mô hình và gợi ý dãy số cho kỳ tiếp theo.

## Setup
1. Tạo repository trên GitHub và push toàn bộ file.
2. Thêm GitHub Secrets:
   - `GMAIL_USER` (gmail gửi)
   - `GMAIL_PASS` (App Password 16 ký tự)
   - `GMAIL_RECEIVER` (nếu khác)
3. Kiểm tra `config.json` (n_periods, window, threshold_retrain_pct).
4. Actions sẽ chạy hàng ngày 10:00 VN (03:00 UTC).

## Lưu ý
- Xổ số là ngẫu nhiên — mô hình chỉ gợi ý dựa trên tần suất lịch sử.
- Kiểm tra logs tại `data/daily_log.txt` và artifacts trong Actions.
