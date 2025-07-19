from pydantic import BaseModel
from datetime import datetime


class EmailMessage(BaseModel):
    id: str
    sender: str
    recipient: str
    subject: str
    plain_text_body: str
    received_at: datetime
