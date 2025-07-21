from pydantic import BaseModel, field_serializer
from datetime import datetime
from typing import Optional

class Email(BaseModel):
    # Set by database manager automatically for both reads and writes.
    pk: Optional[int] = None

    id: str # Message ID associated with the email.
    sender: str
    recipient: str
    subject: str
    plain_text_body: str
    received_at: datetime

    @field_serializer('received_at')
    def serialize_received_at(self, dt: datetime) -> int:
        """
        Serializes the datetime object to a Unix timestamp (seconds since epoch).
        """
        # .timestamp() method returns a float, so we cast to int for seconds.
        # If you need milliseconds, multiply by 1000 before casting or keep as float.
        return int(dt.timestamp())
