import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from utils.logger import log
import config

def send_report(subject, body, attachment_path, receiver_email=None):
    """
    Gửi báo cáo Excel đính kèm qua email sử dụng cấu hình Gmail.
    
    Sử dụng GMAIL_USER và GMAIL_APP_PASSWORD từ config.py (biến môi trường).
    """
    SENDER_EMAIL = config.GMAIL_USER or config.EMAIL_SENDER
    SENDER_PASSWORD = config.GMAIL_APP_PASSWORD or config.EMAIL_PASSWORD
    RECEIVER_EMAIL = receiver_email or config.EMAIL_RECEIVER
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587 # Port tiêu chuẩn cho TLS

    if not all([SENDER_EMAIL, SENDER_PASSWORD, RECEIVER_EMAIL]):
        log("⚠ Bỏ qua gửi email: Thiếu cấu hình EMAIL_SENDER, EMAIL_PASSWORD hoặc EMAIL_RECEIVER.")
        return

    try:
        # Tạo đối tượng message
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = RECEIVER_EMAIL
        msg['Subject'] = subject
        
        # Đính kèm nội dung văn bản
        msg.attach(MIMEText(body, 'plain'))

        # Đính kèm file
        if attachment_path and os.path.exists(attachment_path):
            filename = os.path.basename(attachment_path)
            with open(attachment_path, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {filename}",
            )
            msg.attach(part)
        
        # Thiết lập kết nối SMTP
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.ehlo()
        server.starttls()  # Bắt đầu chế độ bảo mật TLS
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        
        # Gửi email
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
        server.quit()
        
        log(f"✅ Báo cáo đã gửi thành công tới {RECEIVER_EMAIL}!")

    except Exception as e:
        log(f"❌ Lỗi khi gửi email: {e}")
