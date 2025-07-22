import base64
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
import re
import uuid


class Utils:

    @staticmethod
    def decode_b64_into_text(b64_encoded_data: bytes) -> str:
        """Returns string from base64 encoded bytes input."""
        return base64.urlsafe_b64decode(b64_encoded_data).decode("utf-8")

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: int) -> datetime:
        """Converts timestmap in milliseconds since epoch to datetime object."""
        timestamp_seconds: float = timestamp_ms / 1000
        return datetime.fromtimestamp(timestamp=timestamp_seconds, tz=timezone.utc)

    @staticmethod
    def timestamp_seconds_to_datetime(timestamp_seconds: int) -> datetime:
        """Converts timestmap in seconds since epoch to datetime object."""
        return datetime.fromtimestamp(timestamp=timestamp_seconds, tz=timezone.utc)

    @staticmethod
    def get_timestamp_ago(duration_str: str) -> int:
        """
        Calculates a Unix timestamp (seconds since epoch) for a time
        'X days/months/years ago' from today.

        Args:
            duration_str: A string in the format 'X days', 'X months', or 'X years'.
                        X must be an integer.

        Returns:
            An integer representing the Unix timestamp (seconds since epoch)
            for the calculated date/time.

        Raises:
            ValueError: If the duration_str format is invalid or X is not a positive integer.
        """
        # Regex to extract the number and the unit (days, months, years)
        # This regex is the same as the one we used for validation, but now we'll use its groups.
        duration_pattern = r"^(\d+)\s+(days|months|years)$"
        match = re.match(duration_pattern, duration_str)

        if not match:
            raise ValueError(
                f"Invalid duration format. Expected 'X days/months/years'. Got: '{duration_str}'"
            )

        number_str = match.group(1)
        unit = match.group(2)

        try:
            number = int(number_str)
        except ValueError:
            raise ValueError(f"Invalid number '{number_str}' in duration string.")

        if number <= 0:
            raise ValueError(
                f"Duration number must be a positive integer. Got: {number}"
            )

        today = datetime.now(timezone.utc)  # Or datetime.today() if you don't need time component

        if unit == "days":
            past_date = today - timedelta(days=number)
        elif unit == "months":
            # relativedelta handles varying month lengths and leap years correctly
            past_date = today - relativedelta(months=number)
        elif unit == "years":
            # relativedelta handles leap years correctly for years
            past_date = today - relativedelta(years=number)
        else:
            # This case should ideally not be reached due to regex, but good for safety
            raise ValueError(f"Unknown unit: {unit}")

        # Convert the datetime object to a Unix timestamp (seconds since epoch)
        # .timestamp() is available from Python 3.3 onwards
        return int(past_date.timestamp())
    
    @staticmethod
    def unique_id() -> int:
        """
        Returns unique ID that can fit within SQLite's 8 byte signed integer type.
        Essentailly that means the max int value returned can be 2^63-1.
        """
        return uuid.uuid4().int & (1<<63)-1
