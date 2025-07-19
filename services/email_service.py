from abc import ABC, abstractmethod
from pydantic import BaseModel, Field
from typing import List, Optional
from models.email import Email


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


class EmailService(ABC):
    @abstractmethod
    def list(self, list_emails_req: ListEmailsRequest) -> ListEmailsResponse:
        pass
