import os
import logging
from typing import Union, List, Optional
from enum import StrEnum

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as OAuth2Credentials
from google.auth.external_account_authorized_user import (
    Credentials as ExternalAccountCredentials,
)
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pydantic import BaseModel, Field
from datetime import datetime

from common.utils import Utils
from services.email_service import (
    EmailService,
    ListEmailIdsRequest,
    ListEmailIdsResponse,
    BatchUpdateEmailsRequest,
    GetEmailsRequest,
    GetEmailsResponse,
)
from models.email import Email


logger = logging.getLogger(__name__)

type Creds = Union[ExternalAccountCredentials, OAuth2Credentials]


class GmailMessage(BaseModel):
    class CommonHeaders(StrEnum):
        FROM = "From"
        SUBJECT = "Subject"
        TO = "To"  # e.g John Doe <john.doe@gmail.com>

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
        parts: List["GmailMessage.MessagePart"] = Field(default_factory=list)

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

    @property
    def received_at(self) -> datetime:
        timestamp_ms: int = int(self.internalDate)
        return Utils.timestamp_ms_to_datetime(timestamp_ms)

    def get_plain_text_body(self) -> str:
        """Returns plain text body of given Message."""

        def helper(payload: GmailMessage.MessagePart) -> str:
            if payload.mimeType not in set(
                [
                    GmailMessage.MimeType.MULTIPART_ALTERNATIVE,
                    GmailMessage.MimeType.MULTIPART_MIXED,
                    GmailMessage.MimeType.PLAIN_TEXT,
                ]
            ):
                return ""

            if len(payload.parts) == 0:
                if payload.mimeType != GmailMessage.MimeType.PLAIN_TEXT:
                    return ""
                if payload.body.data is None:
                    return ""

                return Utils.decode_b64_into_text(payload.body.data)

            text_bodies: List[str] = []
            for part in payload.parts:
                part_text = helper(part)
                if len(part_text) > 0:
                    text_bodies.append(part_text)

            return "\n".join(text_bodies)

        return helper(self.payload)

    def _get_header_value(self, header_name: "GmailMessage.CommonHeaders") -> str:
        res = list(filter(lambda h: h.name == header_name, self.payload.headers))
        if len(res) == 0:
            return ""
        return res[0].value

    def to_email_message(self) -> Email:
        return Email(
            id=self.id,
            sender=self.sender,
            recipient=self.recipient,
            subject=self.subject,
            plain_text_body=self.get_plain_text_body(),
            received_at=self.received_at,
        )


class GmailListMessagesRequest(BaseModel):
    userId: str = "me"
    q: Optional[str]
    pageToken: Optional[str] = None
    maxResults: int


class GmailListMessagesResponse(BaseModel):
    class IncompleteMessage(BaseModel):
        id: str

    messages: List[IncompleteMessage] = Field(default_factory=list)
    nextPageToken: Optional[str] = None


class GmailBatchUpdateEmailsRequest(BaseModel):
    class Body(BaseModel):
        ids: List[str]
        addLabelIds: List[str]
        removeLabelIds: List[str]

    userId: str = "me"
    body: Body


class GmailAPIError(Exception):
    pass


class GmailService(EmailService):
    """Service to fetch and update messages from the user's Gmail."""

    def list_email_ids(self, req: ListEmailIdsRequest) -> ListEmailIdsResponse:
        """Fetches emails for the given user based on the request."""
        list_email_ids_response = ListEmailIdsResponse(
            email_ids=[], next_page_token=None
        )
        try:
            service = self._build_service()
            gmail_req = GmailListMessagesRequest(
                q=req.query,
                pageToken=req.cur_page_token,
                maxResults=req.page_size,
            )
            results_dict = (
                service.users().messages().list(**gmail_req.model_dump()).execute()
            )
            response = GmailListMessagesResponse(**results_dict)
            for message in response.messages:
                list_email_ids_response.email_ids.append(message.id)

            list_email_ids_response.next_page_token = response.nextPageToken
            return list_email_ids_response

        except Exception as e:
            logger.error(f"List Emails failed for req: {req} with error: {e}")
            raise GmailAPIError("List emails failed") from e

    def get_emails(self, req: GetEmailsRequest) -> GetEmailsResponse:
        """Get emails for given request."""
        response = GetEmailsResponse(emails=[])
        try:
            service = self._build_service()
            for email_id in req.email_ids:
                msg_dict = (
                    service.users().messages().get(userId="me", id=email_id).execute()
                )
                response.emails.append(GmailMessage(**msg_dict).to_email_message())

            return response
        except Exception as e:
            logger.error(f"Get Emails failed for req: {req} with error: {e}")
            raise GmailAPIError("Get Emails failed") from e

    def batch_update_emails(self, req: BatchUpdateEmailsRequest):
        """Batch modify given Email IDs with the following Labels."""
        logger.debug(f"Batch update req: {req}")
        try:
            service = self._build_service()
            gmail_req = GmailBatchUpdateEmailsRequest(
                body=GmailBatchUpdateEmailsRequest.Body(
                    ids=req.ids,
                    addLabelIds=req.add_label_ids,
                    removeLabelIds=req.remove_label_ids,
                )
            )
            service.users().messages().batchModify(**gmail_req.model_dump()).execute()
        except Exception as e:
            logger.error(f"Update Emails failed for req: {req} with error: {e}")
            raise GmailAPIError("Update Emails failed") from e

    def _build_service(self):
        creds = self._fetch_creds()
        return build("gmail", "v1", credentials=creds)

    def _fetch_creds(self) -> Creds:
        """Fetch OAuth2 credentials for user."""
        try:
            SCOPES = [s.strip() for s in os.environ["GMAIL_SCOPES"].split(",")]
            creds = None
            # The file stores the user's access and refresh tokens, and is
            # created automatically when the authorization flow completes for the first
            # time.
            token_path = os.environ["TOKEN_PATH"]
            credentials_path = os.environ["CREDENTIALS_PATH"]
            if os.path.exists(token_path):
                creds = OAuth2Credentials.from_authorized_user_file(token_path, SCOPES)
            # If there are no (valid) credentials available, let the user log in.
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        credentials_path, SCOPES
                    )
                    creds = flow.run_local_server(port=0)
                # Save the credentials for the next run
                with open(token_path, "w") as token:
                    token.write(creds.to_json())

            return creds
        except Exception as e:
            logger.error(f"Failed to fetch credentials withe error: {e}")
            raise GmailAPIError("Failed to fetch Credentials") from e
