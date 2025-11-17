import smtplib
import os
from email.message import EmailMessage
from email.utils import formataddr

def send_email(attachment_path, config):
    """
    Gửi email kèm file Excel đính kèm.
    """
    msg = EmailMessage()
    msg['Subject'] = 'Báo cáo dự đoán Mega/Power'
    msg['From'] = formataddr(("MegaPowerReal", config["user"]))
    msg['To'] = config["to"]
    msg.set_content("Đính kèm báo cáo Mega/Power mới nhất. Xin vui lòng kiểm tra.")

    with open(attachment_path, 'rb') as f:
        file_data = f.read()
        file_name = os.path.basename(attachment_path)

    msg.add_attachment(
        file_data,
        maintype='application',
        subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        filename=file_name
    )

    try:
        with smtplib.SMTP(config["host"], config["port"]) as server:
            server.starttls()
            server.login(config["user"], config["password"])
            server.send_message(msg)
        print(f"✅ Email gửi thành công tới {config['to']}")
    except Exception as e:
        print(f"❌ Lỗi gửi email: {e}")
        raise
