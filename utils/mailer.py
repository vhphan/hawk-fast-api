import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from loguru import logger

from utils.utils import get_env


def send_mail(title, html_content, to_email, cc_email=None):
    logger.info("Sending email...")
    msg = MIMEMultipart()
    html_part = MIMEText(html_content, 'html')
    msg.attach(html_part)
    from_email = "Network_Hawk@ericsson.com"
    msg['From'] = from_email
    msg['To'] = ", ".join(to_email) if isinstance(to_email, list) else to_email
    msg['Subject'] = title
    cc_email = ", ".join(cc_email) if cc_email and isinstance(cc_email, list) else cc_email
    if cc_email:
        msg['Cc'] = cc_email
    server = smtplib.SMTP(get_env('ERIC_HOST'),
                          int(get_env('ERIC_PORT')))
    server.starttls()
    server.login(get_env('ERIC_EMAIL_SENDER'), get_env('ERIC_PASSWORD'))
    server.sendmail(from_email, to_email, msg.as_string())
    server.quit()
    logger.info("Email sent!")


