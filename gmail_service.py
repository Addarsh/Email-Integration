import os
from typing import Union, List, Optional
from enum import StrEnum

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as OAuth2Credentials
from google.auth.external_account_authorized_user import Credentials as ExternalAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pydantic import BaseModel, Field

from utils import Utils
from email_service import EmailService, ListEmailsRequest

type Creds = Union[ExternalAccountCredentials, OAuth2Credentials]

class GmailMessage(BaseModel):
    class CommonHeaders(StrEnum):
        FROM = "From"
        SUBJECT = "Subject"
        TO = "To" # e.g John Doe <john.doe@gmail.com>

    class MimeType(StrEnum):
        PLAIN_TEXT = "text/plain"
        MULTIPART_ALTERNATIVE = "multipart/alternative"
        MULTIPART_MIXED = "multipart/mixed"

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
        parts: List['GmailMessage.MessagePart'] = Field(default_factory=list)

    id: str
    internalDate: str
    payload: MessagePart

    @property
    def sender(self) -> str:
        return self._get_header_value(GmailMessage.CommonHeaders.FROM)
    
    @property
    def recipient(self) -> str:
        return self._get_header_value(GmailMessage.CommonHeaders.TO)
    
    @property
    def subject(self) -> str:
        return self._get_header_value(GmailMessage.CommonHeaders.SUBJECT)
    
    def get_plain_text_body(self) -> str:
        """Returns plain text body of given Message."""

        def helper(payload: GmailMessage.MessagePart) -> str:
            if payload.mimeType not in set([GmailMessage.MimeType.MULTIPART_ALTERNATIVE, GmailMessage.MimeType.MULTIPART_MIXED, GmailMessage.MimeType.PLAIN_TEXT]):
                return ""
            
            if len(payload.parts) == 0:
                if payload.mimeType != GmailMessage.MimeType.PLAIN_TEXT:
                    return ""
                if  payload.body.data is None:
                    return ""
                
                return Utils.decode_b64_into_text(payload.body.data)

            text_bodies: List[str] = []
            for part in payload.parts:
                part_text = helper(part)
                if len(part_text) > 0:
                    text_bodies.append(part_text)

            return "\n".join(text_bodies)
        
        return helper(self.payload)
    
    def _get_header_value(self, header_name: 'GmailMessage.CommonHeaders') -> str:
        res = list(filter(lambda h: h.name == header_name, self.payload.headers))
        if len(res) == 0:
            return ""
        return res[0].value
    
class GmailListMessagesRequest(BaseModel):
    userId: str = "me"
    q: Optional[str]
    maxResults: int
    pageToken: Optional[str] = None

class GmailListMessagesResponse(BaseModel):
    class IncompleteMessage(BaseModel):
        id: str
   
    messages: List[IncompleteMessage] = Field(default_factory=list)
    nextPageToken: Optional[str] = None


class GmailService(EmailService):
    """Service to fetch and update messages from the user's Gmail."""

    def __init__(self):
        self.LIST_MESSAGES_PAGE_SIZE = 1

    def list(self, list_emails_req: ListEmailsRequest):
        """List up to a maximum number of emails for given user."""
        try:
            # Call the Gmail API
            creds = self._fetch_creds()
            service = build("gmail", "v1", credentials=creds)

            cur_page_token = None
            first_time = True
            count = 0
            while (count < list_emails_req.max_results and cur_page_token is not None) or first_time :                
                req = GmailListMessagesRequest(q=list_emails_req.query, maxResults=self.LIST_MESSAGES_PAGE_SIZE, pageToken=cur_page_token)
                results_dict = service.users().messages().list(**req.model_dump()).execute()
                response = GmailListMessagesResponse(**results_dict)

                messages = response.messages
                if len(messages) == 0:
                    print("No more messages found.")
                    return

                for message in messages:
                    msg_dict = (
                        service.users().messages().get(userId="me", id=message.id).execute()
                    )
                    msg = GmailMessage(**msg_dict)
                    count += 1
                    print(f'Message ID: {message.id}')
                    print(f'Message Sender: {msg.sender}, recipient: {msg.recipient}, subject: {msg.subject}')
                    print(f"Message body: ", msg.get_plain_text_body())
                    print(f"Mime type: ", msg.payload.mimeType)
                    print("\n\n\n\n")

                if first_time:
                    first_time = False

                cur_page_token = response.nextPageToken
                print(f"got results count: {count}, next page token: {cur_page_token}")

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