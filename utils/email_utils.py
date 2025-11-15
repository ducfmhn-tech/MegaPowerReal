# utils/email_utils.py
import os, smtplib, ssl
from email.message import EmailMessage
from utils.logger import log

sender = os.getenv("EMAIL_SENDER")
password = os.getenv("EMAIL_PASSWORD")
receiver = os.getenv("EMAIL_RECEIVER")
if not sender or not password or not receiver:
    log("⚠ Missing EMAIL_SENDER / EMAIL_PASSWORD / EMAIL_RECEIVER env.")
    return "missing-config"
def send_email_with_attachment(subject, body, to_addrs, attachment_path=None):
    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("EMAIL_PASSWORD")  # app password
    if not sender or not password or not to_addrs:
        log("⚠ Missing EMAIL_SENDER / EMAIL_PASSWORD / EMAIL_RECEIVER env.")
        return False, "missing-config"
    if isinstance(to_addrs, str): to_addrs=[to_addrs]
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(to_addrs)
    msg.set_content(body)
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            data=f.read()
            msg.add_attachment(data, maintype="application", subtype="octet-stream", filename=os.path.basename(attachment_path))
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
        log(f"📧 Email sent to {to_addrs} (attachment={bool(attachment_path)})")
        return True, None
    except Exception as e:
        log(f"⚠ Email sending failed: {e}")
        return False, str(e)
