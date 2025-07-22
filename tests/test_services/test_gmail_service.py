import logging
import unittest
from datetime import datetime, timezone
from services.gmail_service import (
    GmailService,
    GmailMessage,
    GmailAPIError,
    GetEmailsRequest,
    BatchUpdateEmailsRequest,
)
from services.email_service import ListEmailIdsRequest
from unittest.mock import patch, MagicMock
import base64
from typing import Dict, Any

# Suppress logging during tests for cleaner output
logging.basicConfig(level=logging.ERROR)


# Helper to encode text to Base64 for message bodies
def b64(text: str) -> bytes:
    return base64.urlsafe_b64encode(text.encode("utf-8"))


# A sample message with a simple text/plain body
SIMPLE_MESSAGE: Dict[str, Any] = {
    "id": "1",
    "internalDate": "1672531200000",  # 2023-01-01 00:00:00 UTC
    "payload": {
        "partId": "0",
        "mimeType": "text/plain",
        "headers": [
            {"name": "Subject", "value": "Test Subject 1"},
            {"name": "From", "value": "sender1@example.com"},
            {"name": "To", "value": "recipient1@example.com"},
        ],
        "body": {"data": b64("This is a simple plain text body.")},
        "parts": [],
    },
}

# A sample message with a multipart/alternative body
MULTIPART_ALTERNATIVE_MESSAGE: Dict[str, Any] = {
    "id": "2",
    "internalDate": "1672617600000",  # 2023-01-02 00:00:00 UTC
    "payload": {
        "partId": "0",
        "mimeType": "multipart/alternative",
        "headers": [
            {"name": "Subject", "value": "Test Subject 2"},
            {"name": "From", "value": "sender2@example.com"},
            {"name": "To", "value": "recipient2@example.com"},
        ],
        "body": {},
        "parts": [
            {
                "partId": "1",
                "mimeType": "text/plain",
                "headers": [],
                "body": {"data": b64("This is the plain text part.")},
                "parts": [],
            },
            {
                "partId": "2",
                "mimeType": "text/html",
                "headers": [],
                "body": {"data": b64("<h1>This is the HTML part.</h1>")},
                "parts": [],
            },
        ],
    },
}

# A sample message with a nested multipart body
MULTIPART_MIXED_MESSAGE: Dict[str, Any] = {
    "id": "3",
    "internalDate": "1672704000000",  # 2023-01-03 00:00:00 UTC
    "payload": {
        "partId": "0",
        "mimeType": "multipart/mixed",
        "headers": [
            {"name": "Subject", "value": "Test Subject 3"},
            {"name": "From", "value": "sender3@example.com"},
            {"name": "To", "value": "recipient3@example.com"},
        ],
        "body": {},
        "parts": [
            {
                "partId": "1",
                "mimeType": "multipart/alternative",
                "headers": [],
                "body": {},
                "parts": [
                    {
                        "partId": "1.1",
                        "mimeType": "text/plain",
                        "headers": [],
                        "body": {"data": b64("Nested plain text.")},
                        "parts": [],
                    },
                    {
                        "partId": "1.2",
                        "mimeType": "text/html",
                        "headers": [],
                        "body": {"data": b64("<h1>Nested HTML.</h1>")},
                        "parts": [],
                    },
                ],
            },
            {
                "partId": "2",
                "mimeType": "application/octet-stream",
                "headers": [],
                "body": {"data": b64("some attachment data")},
                "parts": [],
            },
        ],
    },
}

# A sample response from the messages.list endpoint
LIST_MESSAGES_RESPONSE = {
    "messages": [{"id": "1"}, {"id": "2"}],
    "nextPageToken": "nextPageToken123",
}


class TestGmailMessage(unittest.TestCase):
    """Tests for the GmailMessage Pydantic model and its methods."""

    def test_properties(self):
        """Test that properties correctly extract header and date info."""
        msg = GmailMessage(**SIMPLE_MESSAGE)
        self.assertEqual(msg.sender, "sender1@example.com")
        self.assertEqual(msg.recipient, "recipient1@example.com")
        self.assertEqual(msg.subject, "Test Subject 1")
        self.assertEqual(msg.received_at, datetime(2023, 1, 1, tzinfo=timezone.utc))

    def test_get_plain_text_body_simple(self):
        """Test body extraction from a simple text/plain message."""
        msg = GmailMessage(**SIMPLE_MESSAGE)
        self.assertEqual(msg.get_plain_text_body(), "This is a simple plain text body.")

    def test_get_plain_text_body_multipart_alternative(self):
        """Test body extraction from a multipart/alternative message."""
        msg = GmailMessage(**MULTIPART_ALTERNATIVE_MESSAGE)
        self.assertEqual(msg.get_plain_text_body(), "This is the plain text part.")

    def test_get_plain_text_body_multipart_mixed(self):
        """Test body extraction from a nested multipart message."""
        msg = GmailMessage(**MULTIPART_MIXED_MESSAGE)
        self.assertEqual(msg.get_plain_text_body(), "Nested plain text.")

    def test_get_plain_text_body_no_text_part(self):
        """Test that an empty string is returned if no text/plain part exists."""
        no_text_message = MULTIPART_ALTERNATIVE_MESSAGE.copy()
        no_text_message["payload"]["parts"] = [
            p
            for p in no_text_message["payload"]["parts"]
            if p["mimeType"] != "text/plain"
        ]
        msg = GmailMessage(**no_text_message)
        self.assertEqual(msg.get_plain_text_body(), "")

    def test_to_email_message(self):
        """Test the conversion from GmailMessage to the generic Email model."""
        msg = GmailMessage(**SIMPLE_MESSAGE)
        email = msg.to_email_message()
        self.assertEqual(email.id, "1")
        self.assertEqual(email.sender, "sender1@example.com")
        self.assertEqual(email.recipient, "recipient1@example.com")
        self.assertEqual(email.subject, "Test Subject 1")
        self.assertEqual(email.plain_text_body, "This is a simple plain text body.")
        self.assertEqual(email.received_at, datetime(2023, 1, 1, tzinfo=timezone.utc))


class TestGmailService(unittest.TestCase):
    """Tests for the GmailService class, mocking all external API calls."""

    def setUp(self):
        """Instantiate the service for each test."""
        self.service = GmailService()

    @patch("services.gmail_service.GmailService._build_service")
    def test_list_email_ids_success(self, mock_build_service):
        """Test successful listing of email IDs."""
        # Setup mock for the chained API calls
        mock_api = MagicMock()
        # This line accesses the list mock's return_value WITHOUT calling it
        mock_api.users().messages().list.return_value.execute.return_value = (
            LIST_MESSAGES_RESPONSE
        )
        mock_build_service.return_value = mock_api

        # Create request and call the method
        req = ListEmailIdsRequest(
            page_size=10, cur_page_token="oldToken", senders=["test@example.com"]
        )
        response = self.service.list_email_ids(req)

        # Assertions
        self.assertEqual(response.email_ids, ["1", "2"])
        self.assertEqual(response.next_page_token, "nextPageToken123")
        mock_api.users().messages().list.assert_called_once_with(
            userId="me",
            q="from:test@example.com",
            pageToken="oldToken",
            maxResults=10,
        )

    @patch("services.gmail_service.GmailService._build_service")
    def test_list_email_ids_api_error(self, mock_build_service):
        """Test that GmailAPIError is raised on API failure."""
        mock_api = MagicMock()
        mock_api.users().messages().list().execute.side_effect = Exception("API Error")
        mock_build_service.return_value = mock_api

        with self.assertRaises(GmailAPIError):
            req = ListEmailIdsRequest(page_size=10)
            self.service.list_email_ids(req)

    @patch("services.gmail_service.GmailService._build_service")
    def test_get_emails_success(self, mock_build_service):
        """Test successful fetching of full email details."""
        mock_api = MagicMock()

        # CORRECTED MOCK SETUP
        # Configure the return value of the 'get' method without calling it.
        mock_api.users().messages().get.return_value.execute.side_effect = [
            SIMPLE_MESSAGE,
            MULTIPART_ALTERNATIVE_MESSAGE,
        ]
        mock_build_service.return_value = mock_api

        req = GetEmailsRequest(email_ids=["1", "2"])
        response = self.service.get_emails(req)

        # This assertion should now pass
        self.assertEqual(len(response.emails), 2)
        self.assertEqual(response.emails[0].id, "1")
        self.assertEqual(response.emails[1].id, "2")

        # CORRECTED CALL COUNT ASSERTION
        # Check the call_count attribute directly without calling get() again.
        self.assertEqual(mock_api.users().messages().get.call_count, 2)

        # Verify the calls were made with the correct arguments
        mock_api.users().messages().get.assert_any_call(userId="me", id="1")
        mock_api.users().messages().get.assert_any_call(userId="me", id="2")

    @patch("services.gmail_service.GmailService._build_service")
    def test_batch_update_emails_success(self, mock_build_service):
        """Test successful batch modification of emails."""
        mock_api = MagicMock()
        mock_api.users().messages().batchModify.return_value.execute.return_value = {}
        mock_build_service.return_value = mock_api

        req = BatchUpdateEmailsRequest(
            ids=["1", "2"],
            add_label_ids=["UNREAD"],
            remove_label_ids=["INBOX"],
        )
        self.service.batch_update_emails(req)

        # Check that batchModify was called with the correct body
        expected_body = {
            "ids": ["1", "2"],
            "addLabelIds": ["UNREAD"],
            "removeLabelIds": ["INBOX"],
        }
        mock_api.users().messages().batchModify.assert_called_once_with(
            userId="me", body=expected_body
        )
