from abc import ABC, abstractmethod
from pydantic import BaseModel, Field
from typing import List, Optional
from models.email import Email
from enum import StrEnum


class ListEmailsRequest(BaseModel):
    senders: List[str] = Field(default_factory=list)
    max_results: int

    @property
    def query(self) -> Optional[str]:
        if len(self.senders) == 0:
            return None
        return " OR ".join([f"from:{sender}" for sender in self.senders])


class ListEmailsResponse(BaseModel):
    count: int
    emails: List[Email]


class BatchUpdateEmailsRequest(BaseModel):
    """Bulk update Labels for given Email Ids."""

    class Label(StrEnum):
        INBOX = "INBOX"
        SPAM = "SPAM"
        IMPORTANT = "IMPORTANT"
        UNREAD = "UNREAD"

    ids: List[str]
    add_label_ids: List[str] = Field(default_factory=list)
    remove_label_ids: List[str] = Field(default_factory=list)


class EmailService(ABC):
    @abstractmethod
    def list(self, list_emails_req: ListEmailsRequest) -> ListEmailsResponse:
        pass

    @abstractmethod
    def batch_update_emails(self, req: BatchUpdateEmailsRequest):
        pass
