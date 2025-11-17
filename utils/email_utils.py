# utils/email_utils.py
import os, smtplib, mimetypes
from email.message import EmailMessage
from utils.logger import log

def _get_smtp_config():
    sender = os.getenv("EMAIL_SENDER","").strip()
    password = os.getenv("EMAIL_PASSWORD","").strip()
    receiver = os.getenv("EMAIL_RECEIVER","").strip()
    smtp_host = os.getenv("EMAIL_HOST","smtp.gmail.com")
    smtp_port = int(os.getenv("EMAIL_PORT","587"))
    return sender, password, receiver, smtp_host, smtp_port

def send_email_with_attachment(subject, body, attachment_path=None):
    sender, password, receiver, smtp_host, smtp_port = _get_smtp_config()
    if not sender or not password or not receiver:
        msg = "missing-config"
        log(f"⚠ Email config missing: sender/password/receiver")
        return msg
    try:
        msg = EmailMessage()
        msg["From"] = sender
        msg["To"] = receiver
        msg["Subject"] = subject
        msg.set_content(body)
        if attachment_path and os.path.exists(attachment_path):
            ctype, _ = mimetypes.guess_type(attachment_path)
            if ctype is None:
                ctype = "application/octet-stream"
            maintype, subtype = ctype.split("/",1)
            with open(attachment_path,"rb") as f:
                data = f.read()
            msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=os.path.basename(attachment_path))
        server = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
        server.ehlo()
        if smtp_port == 587:
            server.starttls()
        server.login(sender, password)
        server.send_message(msg)
        server.quit()
        return "ok"
    except Exception as e:
        log(f"⚠ Email sending failed: {e}")
        return str(e)
