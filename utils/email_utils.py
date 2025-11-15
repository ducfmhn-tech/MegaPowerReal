# utils/email_utils.py
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from utils.logger import log

def send_email_with_attachment(subject, body, attachment_path=None):
    """
    Send email using SMTP (Gmail). Requires EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECEIVER env vars.
    Returns "ok" on success, "missing-config" when env vars absent, "error" on exception.
    """
    sender = os.getenv("EMAIL_SENDER", "").strip()
    password = os.getenv("EMAIL_PASSWORD", "").strip()
    receiver = os.getenv("EMAIL_RECEIVER", "").strip()

    if not sender or not password or not receiver:
        log("⚠ Missing EMAIL_SENDER / EMAIL_PASSWORD / EMAIL_RECEIVER env.")
        return "missing-config"

    try:
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = receiver
        msg["Subject"] = subject or "MegaPower Report"

        msg.attach(MIMEText(body or "", "plain", "utf-8"))

        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition",
                            f"attachment; filename={os.path.basename(attachment_path)}")
            msg.attach(part)

        server = smtplib.SMTP("smtp.gmail.com", 587, timeout=60)
        server.ehlo()
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, receiver, msg.as_string())
        server.quit()

        log(f"📧 Email sent to {receiver} (attachment={bool(attachment_path)})")
        return "ok"
    except Exception as e:
        log(f"⚠️ Email sending failed: {e}")
        return "error"
