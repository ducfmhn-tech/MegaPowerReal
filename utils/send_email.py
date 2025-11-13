"""
send_email.py
-----------------------------------
- Send email via Gmail SMTP
- Used for MegaPowerReal project
- Supports attachments (.xlsx, .csv, .txt)
"""

import smtplib, os
from email.message import EmailMessage
from datetime import datetime

def send_report_email(sender, password, receiver, subject, body, attachment_path=None):
    """Send report email with optional attachment."""
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = receiver
    msg["Subject"] = subject
    msg.set_content(body)

    # Attach file if exists
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as f:
            file_data = f.read()
            file_name = os.path.basename(attachment_path)
        msg.add_attachment(
            file_data,
            maintype="application",
            subtype="octet-stream",
            filename=file_name
        )
        print(f"📎 Attached: {file_name}")
    else:
        print("⚠️ No attachment found or file missing.")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(sender, password)
            smtp.send_message(msg)
        print(f"📤 Email sent successfully to {receiver}")
        _log_email(sender, receiver, subject, attachment_path)
    except Exception as e:
        print("❌ Failed to send email:", e)


def _log_email(sender, receiver, subject, attachment_path=None):
    """Write a log entry to daily_log.txt after sending email."""
    os.makedirs("logs", exist_ok=True)
    log_file = os.path.join("logs", "daily_log.txt")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(
            f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
            f"From: {sender} -> To: {receiver} | Subject: {subject} | "
            f"Attachment: {os.path.basename(attachment_path) if attachment_path else 'None'}\n"
        )
    print("🗒 Log updated → logs/daily_log.txt")
