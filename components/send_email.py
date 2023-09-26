from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import base64

def send_email(self, subject, body):
    creds = Credentials.from_authorized_user_file('token.json')
    service = build('gmail', 'v1', credentials=creds)

    message = MIMEMultipart()
    message['to'] = 'christian@intellicode.co.za'
    message['subject'] = subject
    message.attach(MIMEText(body, 'plain'))

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    message = service.users().messages().send(userId='me', body={'raw': raw_message})

    try:
        message.execute()
        print('Message sent')
    except HttpError as error:
        print(f'An error occurred: {error}')