import os
from typing import Union, List, Optional
from enum import StrEnum

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as OAuth2Credentials
from google.auth.external_account_authorized_user import Credentials as ExternalAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pydantic import BaseModel

from utils import Utils

type Creds = Union[ExternalAccountCredentials, OAuth2Credentials]

class GmailMessage(BaseModel):
    class CommonHeaders(StrEnum):
        FROM = "From"
        SUBJECT = "Subject"
        TO = "To" # e.g John Doe <john.doe@gmail.com>

    class MimeType(StrEnum):
        TEXT = "text/plain"

    class MessagePart(BaseModel):
        class Header(BaseModel):
            name: str
            value: str

        class MessagePartBody(BaseModel):
            data: Optional[bytes] = None
        
        partId: str
        mimeType: str
        headers: List[Header]
        body: MessagePartBody

    id: str
    internalDate: str
    payload: MessagePart

    
    def is_text_body(self) -> bool:
        return self.payload.mimeType == GmailMessage.MimeType.TEXT

    @property
    def sender(self) -> str:
        return self._get_header_value(GmailMessage.CommonHeaders.FROM)
    
    @property
    def recipient(self) -> str:
        return self._get_header_value(GmailMessage.CommonHeaders.TO)
    
    @property
    def subject(self) -> str:
        return self._get_header_value(GmailMessage.CommonHeaders.SUBJECT)
    
    @property
    def text_body(self) -> str:
        """Returns text body of given Message and empty string if its mimeType is not text."""
        if not self.is_text_body():
            return ""
        if self.payload.body.data is None:
            return ""
        return Utils.decode_b64_into_text(self.payload.body.data)
    
    def _get_header_value(self, header_name: 'GmailMessage.CommonHeaders') -> str:
        res = list(filter(lambda h: h.name == header_name, self.payload.headers))
        if len(res) == 0:
            return ""
        return res[0].value

class GmailService:
    """Service to fetch and update messages from the user's Gmail."""

    def __init__(self):
        pass

    def list_emails(self, max_results: int=10):
        """List maximum of given emails for given user."""
        try:
            # Call the Gmail API
            creds = self._fetch_creds()
            service = build("gmail", "v1", credentials=creds)
            results = service.users().messages().list(userId="me", maxResults=max_results).execute()
            
            messages = results.get("messages", [])

            if not messages:
                print("No messages found.")
                return

            print("Messages:")
            for message in messages:
                msg_dict = (
                    service.users().messages().get(userId="me", id=message["id"]).execute()
                )
                msg = GmailMessage(**msg_dict)
                if not msg.is_text_body():
                    continue
                print(f'Message ID: {message["id"]}')
                print(f'Message Sender: {msg.sender}, recipient: {msg.recipient}, subject: {len(msg.subject)}')
                print(f"Message body: ", msg.text_body)
                print("\n\n")

        except Exception as e:
            print(f"An error occurred when listing emails: {e}")


    def _fetch_creds(self) -> Creds :
        """Fetch OAuth2 credentials for user."""

        # If modifying these scopes, delete the file token.json.
        SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists("token.json"):
            creds = OAuth2Credentials.from_authorized_user_file("token.json", SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open("token.json", "w") as token:
                token.write(creds.to_json())
        
        return creds