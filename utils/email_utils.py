# utils/email_utils.py
import os
from utils.logger import log
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def send_email_with_attachment(subject, body, attachment_path=None):
    sender = os.getenv("EMAIL_SENDER") or os.getenv("GMAIL_USER")
    password = os.getenv("EMAIL_PASSWORD") or os.getenv("GMAIL_APP_PASSWORD")
    receiver = os.getenv("EMAIL_RECEIVER") or os.getenv("RECEIVER_EMAIL")
    if not sender or not password or not receiver:
        log("⚠️ Email sending failed: Missing EMAIL_SENDER / EMAIL_PASSWORD / EMAIL_RECEIVER env.")
        return False
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = receiver
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path,"rb") as f:
            part = MIMEBase("application","octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(attachment_path)}")
        msg.attach(part)
    try:
        # Use SSL port 465 to be robust in GH Actions
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.send_message(msg)
        log(f"📧 Email sent to {receiver} (attachment={bool(attachment_path)})")
        return True
    except Exception as e:
        log(f"❌ Email sending failed: {e}")
        return False
