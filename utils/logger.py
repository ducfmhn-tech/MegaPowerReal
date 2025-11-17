import datetime,sys
def log(msg):
    """
    Ghi một thông điệp có timestamp (UTC) ra console và flush buffer ngay lập tức.
    
    Args:
        msg (str): Nội dung thông điệp.
    """
    # Lấy thời gian hiện tại theo múi giờ UTC
    t = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    # In thông điệp
    print(f"[{t}] {msg}")
    
    # Flush buffer để đảm bảo thông điệp xuất hiện ngay lập tức
    sys.stdout.flush()
