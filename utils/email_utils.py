import os
import smtplib
import ssl
from email.message import EmailMessage
from utils.logger import log


def send_email_with_attachment(filepath):
    sender = os.getenv("EMAIL_SENDER")
    password = os.getenv("EMAIL_PASSWORD")
    receiver = os.getenv("EMAIL_RECEIVER")

    if not sender or not password or not receiver:
        log("⚠ Missing EMAIL_SENDER / EMAIL_PASSWORD / EMAIL_RECEIVER env.")
        return "missing-config"

    try:
        msg = EmailMessage()
        msg["Subject"] = "MegaPowerReal Report"
        msg["From"] = sender
        msg["To"] = receiver
        msg.set_content("Attached is the latest MegaPowerReal report.")

        with open(filepath, "rb") as f:
            filedata = f.read()

        msg.add_attachment(
            filedata,
            maintype="application",
            subtype="octet-stream",
            filename=os.path.basename(filepath),
        )

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender, password)
            server.send_message(msg)

        log(f"📧 Email sent to {receiver} (attachment=True)")
        return "ok"

    except Exception as e:
        log(f"⚠️ Email sending failed: {e}")
        return "fail"
