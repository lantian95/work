# -*- coding: utf-8 -*-
import sys
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class SendMail(object):
    def send_mail(self, days, cmd):
        sender = '**'
        receiver = '**'
        subject = 'pushing xinlan log error'
        smtpserver = '**'
        username = '**'
        password = '**'
        message = 'pushing xinlan log has error on '+days+" about "+cmd
        msg = MIMEText(message, 'plain', 'utf-8')
        msg['Subject'] = Header(subject, 'utf-8')
        msg['From'] = sender
        smtp = smtplib.SMTP()
        #smtp.set_debuglevel(1)
        smtp.connect(smtpserver)
        smtp.login(username, password)
        smtp.sendmail(sender, receiver, msg.as_string())
        smtp.quit()

if __name__ == '__main__':
    if len(sys.argv) == 2:
        _, days = tuple(sys.argv)
    else:
        print("param error")
    SendMail().send_mail(days)