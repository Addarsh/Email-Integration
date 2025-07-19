import base64
from datetime import datetime, timezone

class Utils:

    @staticmethod
    def decode_b64_into_text(b64_encoded_data: bytes) -> str:
        """Returns string from base64 encoded bytes input."""
        return base64.urlsafe_b64decode(b64_encoded_data).decode('utf-8')
    
    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: int) -> datetime:
        """Converts timestmap in milliseconds since epoch to datetime object."""
        timestamp_seconds: float = timestamp_ms / 1000
        return datetime.fromtimestamp(timestamp=timestamp_seconds, tz=timezone.utc)
    
    @staticmethod
    def timestamp_seconds_to_datetime(timestamp_seconds: int) -> datetime:
        """Converts timestmap in seconds since epoch to datetime object."""
        return datetime.fromtimestamp(timestamp=timestamp_seconds, tz=timezone.utc)