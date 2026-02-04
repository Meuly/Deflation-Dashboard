from emailer import send_email
import os
from datetime import datetime

def main():
    now = datetime.now().strftime("%Y-%m-%d %H:%M ET")

    subject = "Deflation Dashboard — TEST RUN"
    body = f"""
System Test Successful

Timestamp: {now}

This confirms:
- GitHub Actions ran
- Python executed
- Email delivery works

Next step: add indicators.
"""

    send_email(
        subject=subject,
        body=body,
        username=os.environ["EMAIL_USERNAME"],
        password=os.environ["EMAIL_PASSWORD"],
        sender=os.environ["EMAIL_FROM"],
        recipient=os.environ["EMAIL_TO"]
    )

if __name__ == "__main__":
    main()
