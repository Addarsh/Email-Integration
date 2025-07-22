from pydantic import BaseModel, field_serializer
from datetime import datetime
from typing import Optional, Any, List
from enum import StrEnum


class Email(BaseModel):
    # Set by database manager automatically for both reads and writes.
    pk: Optional[int] = None

    id: str  # Message ID associated with the email.
    sender: str
    recipient: str
    subject: str
    plain_text_body: str
    received_at: datetime

    @field_serializer("received_at")
    def serialize_received_at(self, dt: datetime) -> int:
        """
        Serializes the datetime object to a Unix timestamp (seconds since epoch).
        """
        # .timestamp() method returns a float, so we cast to int for seconds.
        # If you need milliseconds, multiply by 1000 before casting or keep as float.
        return int(dt.timestamp())


class EmailColumnName(StrEnum):
    """Valid email column names"""

    SENDER = "sender"
    RECIPIENT = "recipient"
    SUBJECT = "subject"
    PLAIN_TEXT_BODY = "plain_text_body"
    RECEIVED_AT = "received_at"


class EmailLabel(StrEnum):
    UNREAD = "UNREAD"
    SPAM = "SPAM"
    INBOX = "INBOX"
    IMPORTANT = "IMPORTANT"


class FilterEmailsRequest(BaseModel):
    """Filter request to search for emails from the database."""

    class Rule(BaseModel):
        class Predicate(StrEnum):
            CONTAINS = "Contains"
            NOT_CONTAINS = "Does Not Contain"
            EQUALS = "Equals"
            NOT_EQUALS = "Does Not Equal"
            LESS_THAN = "Less Than"
            GREATER_THAN = "Greater Than"

        column_name: EmailColumnName
        predicate: Predicate
        value: Any

    class RulesCollection(BaseModel):
        class CollectionPredicate(StrEnum):
            ANY = "Any"
            ALL = "All"

        predicate: CollectionPredicate
        rules: List["FilterEmailsRequest.Rule"]

    filter: RulesCollection

    @property
    def column_names(self) -> List[str]:
        return [rule.column_name for rule in self.filter.rules]
