# utils/email_utils.py
import smtplib, os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

def send_email_with_report(sender, password, recipient, subject, body, attach_path=None):
    try:
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = recipient
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        if attach_path and os.path.exists(attach_path):
            with open(attach_path, 'rb') as f:
                part = MIMEApplication(f.read(), Name=os.path.basename(attach_path))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attach_path)}"'
            msg.attach(part)

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, password)
        server.send_message(msg)
        server.quit()
        print(f"✅ Email sent successfully to {recipient}")
    except Exception as e:
        print(f"❌ Email sending failed: {e}")
