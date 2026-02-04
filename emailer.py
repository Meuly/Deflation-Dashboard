import smtplib
from email.mime.text import MIMEText
from config import SMTP_SERVER, SMTP_PORT

def send_email(subject, body, username, password, sender, recipient):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(username, password)
        server.send_message(msg)
