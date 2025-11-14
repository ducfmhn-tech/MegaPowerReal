"""
email_utils.py — Send email via Gmail SMTP
"""

import os
import smtplib
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
from config import CFG

def load_email_config_from_env():
    """Override config using GitHub Secrets."""
    CFG["email_sender"]   = os.getenv("EMAIL_SENDER")
    CFG["email_receiver"] = os.getenv("EMAIL_RECEIVER")
    CFG["email_password"] = os.getenv("EMAIL_PASSWORD")

def send_email_with_attachment(body_text, attachment_path=None):
    """Send email with optional XLSX attachment."""
    
    load_email_config_from_env()

    sender = CFG["email_sender"]
    receiver = CFG["email_receiver"]
    password = CFG["email_password"]

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = receiver
    msg["Subject"] = CFG["email_subject"]

    msg.attach(MIMEText(body_text, "plain"))

    # Optional file
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(attachment_path)}")
            msg.attach(part)

    # Send via Gmail SMTP
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, password)
        smtp.send_message(msg)

    print(f"📧 Email sent to {receiver} (attachment={bool(attachment_path)})")
