# utils/email_utils.py
import os
import ssl
import smtplib
from email.message import EmailMessage
from utils.logger import log

def send_email_with_attachment(subject, body, attachment_path):
    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("EMAIL_PASSWORD")
    receiver = os.getenv("EMAIL_RECEIVER")

    if not sender or not password or not receiver:
        log("⚠ Missing EMAIL_SENDER / EMAIL_PASSWORD / EMAIL_RECEIVER env.")
        return "missing-config"

    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = receiver
        msg.set_content(body)

        if attachment_path and os.path.exists(attachment_path):
            with open(attachment_path, "rb") as f:
                data = f.read()
            msg.add_attachment(data, maintype="application", subtype="octet-stream", filename=os.path.basename(attachment_path))

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender, password)
            server.send_message(msg)

        log(f"📧 Email sent to {receiver} (attachment={'Yes' if attachment_path else 'No'})")
        return "ok"
    except Exception as e:
        log(f"⚠️ Email sending failed: {e}")
        return str(e)
