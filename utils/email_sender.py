"""
send email via Gmail SMTP. Use GitHub secrets:
GMAIL_USER, GMAIL_PASS, GMAIL_RECEIVER (optional)
"""
import os, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

def send_email_smtp(subject, body, attachment_path=None):
    sender = os.getenv("GMAIL_USER")
    password = os.getenv("GMAIL_PASS")
    receiver = os.getenv("GMAIL_RECEIVER") or os.getenv("GMAIL_USER")
    if not sender or not password:
        print("SMTP credentials not set in env. Skipping email.")
        return False
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = receiver
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
            msg.attach(part)
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender, password)
            server.send_message(msg)
        print("Email sent to", receiver)
        return True
    except Exception as e:
        print("Error sending email:", e)
        return False
