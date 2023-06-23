import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Header

apikey = os.environ.get('SENDGRID_API_KEY')

sg = SendGridAPIClient(apikey)

from_email = ('your-email@email.com','Your Name')

to_emails = [('person1@email.com','Person1 Name'),
             ('person2@email.com','Person2 Name')]
             
subject = 'The text that will show up in the subject line.'

message_body = '<p>The email message that you would like to send.</p>'

message = Mail(from_email=from_email,
               to_emails=to_emails,
               subject=subject,
               html_content=message_body)

message.header = Header('Accept', 'application/json', p=0)

try:
    response = sg.send(message)
    print(response.status_code)
    print(response.body)
    print(response.headers)
except Exception as e:
    print(e)