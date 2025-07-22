from pydantic import BaseModel, field_validator, ValidationInfo
from typing import Any, List
from enum import StrEnum
from utils import Utils


class EmailRulesConfig(BaseModel):
    """Rules configuration object."""

    class Rule(BaseModel):
        _DURATION_PATTERN = r"^(\d+)\s+(days|months|years)$"

        class FieldName(StrEnum):
            FROM = "From"
            TO = "To"
            SUBJECT = "Subject"
            RECEIVED_DATE = "Date Received"
            MESSAGE = "Message"

        class Predicate(StrEnum):
            CONTAINS = "contains"
            NOT_CONTAINS = "does not contain"
            EQUALS = "is equal to"
            NOT_EQUALS = "is not equal to"
            LESS_THAN = "is less than"
            GREATER_THAN = "is greater than"

        field_name: FieldName
        predicate: Predicate
        value: Any

        @field_validator("value")
        @classmethod
        def validate_value(cls, value: Any, validation_info: ValidationInfo):
            field_name: EmailRulesConfig.Rule.FieldName = validation_info.data[
                "field_name"
            ]
            if field_name == EmailRulesConfig.Rule.FieldName.RECEIVED_DATE:
                try:
                    # Validates that timestamp string format.
                    Utils.get_timestamp_ago(str(value))
                except Exception as e:
                    raise ValueError(
                        f"Failed to validate received date with error: {e}"
                    )

            return value

        @staticmethod
        def get_timetamp_seconds(value: Any) -> int:
            """Parses given timestamp string (X days/months/years) and converts to timestamp in seconds."""
            try:
                return Utils.get_timestamp_ago(str(value))
            except Exception as e:
                raise ValueError(
                    f"Failed to get timestamp in seconds from value: {value} with error: {e}"
                )

    class EmailRulesCollection(BaseModel):
        class CollectionPredicate(StrEnum):
            ANY = "Any"
            ALL = "All"

        class Action(BaseModel):
            class Type(StrEnum):
                MARK_MESSAGE = "Mark Message As"
                MOVE_MESSAGE_TO = "Move Message To"

            class Label(StrEnum):
                INBOX = "Inbox"
                SPAM = "Spam"
                IMPORTANT = "Important"
                READ = "Read"
                UNREAD = "Unread"

            type: Type
            value: Label

        description: str
        predicate: CollectionPredicate
        rules: List["EmailRulesConfig.Rule"]
        actions: List[Action]

    collections: List[EmailRulesCollection]
