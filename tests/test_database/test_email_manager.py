import pytest
import os
import sqlite3
import logging
from unittest.mock import patch, MagicMock
from database.email_manager import (
    EmailManager,
    EmailTablesCreationError,
    Email,
    EmailInsertDbError,
    EmailReadDbError,
    EmailFilterDbError,
)
from models.email import FilterEmailsRequest
from datetime import datetime, timezone
import pydantic

# Suppress logging during tests for cleaner output.
logging.basicConfig(level=logging.ERROR)

logger = logging.getLogger(__name__)

# --- Fixtures for common setup ---


@pytest.fixture
def email_manager_instance():
    # Setup temporary emails database file.
    db_name = "emails_test.db"
    manager = EmailManager(db_name)
    yield manager

    # Teardown code.
    if os.path.exists(db_name):
        os.remove(db_name)


@pytest.fixture
def sample_emails():
    """Provides a list of sample Email objects for testing."""
    # Use a fixed timestamp for predictable testing
    fixed_timestamp_s = 1678886400  # March 15, 2023 12:00:00 PM UTC
    dt_object = datetime.fromtimestamp(fixed_timestamp_s, tz=timezone.utc)

    return [
        Email(
            id="email1",
            sender="alice@example.com",
            recipient="bob@example.com",
            subject="Meeting Reminder",
            plain_text_body="Don't forget the meeting at 3 PM.",
            received_at=dt_object,
        ),
        Email(
            id="email2",
            sender="charlie@company.com",
            recipient="alice@example.com",
            subject="Project Update",
            plain_text_body="Here's the latest project update.",
            received_at=dt_object,
        ),
        Email(
            id="email3",
            sender="info@newsletter.com",
            recipient="bob@example.com",
            subject="Newsletter for July",
            plain_text_body="Check out our latest news.",
            received_at=dt_object,
        ),
        Email(
            id="email4",
            sender="marketing@company.com",
            recipient="diana@example.com",
            subject="Special Offer!",
            plain_text_body="Limited time offer just for you.",
            received_at=dt_object,
        ),
    ]


# --- Tests for _create_table ---


def test_create_table_success(email_manager_instance):
    """Test that tables and indices are created successfully."""

    conn = sqlite3.connect(email_manager_instance.db_name)
    cursor = conn.cursor()

    # Check if 'emails' table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='emails';"
    )
    assert cursor.fetchone() is not None

    # Check if FTS table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='fts_idx_emails';"
    )
    assert cursor.fetchone() is not None

    # Check if a secondary indexes exists (e.g., on 'id', 'subject' etc.)
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_message_id';"
    )
    assert cursor.fetchone() is not None
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_sender';"
    )
    assert cursor.fetchone() is not None
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_recipient';"
    )
    assert cursor.fetchone() is not None
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_subject';"
    )
    assert cursor.fetchone() is not None
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_plain_text_body';"
    )
    assert cursor.fetchone() is not None
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_received_at';"
    )
    assert cursor.fetchone() is not None

    conn.close()


def test_create_table_failure():
    """Test that EmailTablesCreationError is raised on database creation failure."""
    # Mock sqlite3.connect to raise an error during table creation
    with patch("sqlite3.connect", side_effect=sqlite3.Error("Test DB Error")):
        with pytest.raises(EmailTablesCreationError):
            EmailManager(":memory:")


# --- Tests for insert method ---


def test_insert_no_emails(email_manager_instance):
    """Test inserting an empty list of emails."""
    manager = email_manager_instance
    # No exception should be raised, and no change to DB
    manager.insert([])
    conn = sqlite3.connect(manager._db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM emails")
    assert cursor.fetchone()[0] == 0
    conn.close()


def test_insert_multiple_emails(email_manager_instance, sample_emails):
    """Test inserting multiple emails."""
    manager = email_manager_instance

    # Mock Utils.unique_id for each email with predictable values
    pks = iter([i for i in range(len(sample_emails))])
    with patch("common.utils.Utils.unique_id", side_effect=lambda: next(pks)):
        manager.insert(sample_emails)

    conn = sqlite3.connect(manager.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM emails")
    assert cursor.fetchone()[0] == len(sample_emails)

    # Verify FTS index entry for one email
    cursor.execute(
        "SELECT COUNT(*) FROM fts_idx_emails WHERE plain_text_body MATCH 'meeting'"
    )
    assert cursor.fetchone()[0] == 1  # email1 contains "meeting"

    conn.close()


def test_insert_duplicate_emails(email_manager_instance, sample_emails):
    """Test inserting duplicate emails (should be ignored due to INSERT OR IGNORE)."""
    manager = email_manager_instance
    email_to_insert = sample_emails[0]

    with patch("common.utils.Utils.unique_id", return_value=1):
        manager.insert([email_to_insert])  # First insert

    with patch("common.utils.Utils.unique_id", return_value=2):
        manager.insert([email_to_insert])  # Second insert (duplicate ID)

    conn = sqlite3.connect(manager.db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM emails WHERE id = ?", (email_to_insert.id,))
    assert cursor.fetchone()[0] == 1  # Should still be only one entry
    conn.close()


def test_insert_db_error(email_manager_instance, sample_emails):
    """Test that EmailInsertDbError is raised on database insert failure."""
    manager = email_manager_instance
    # Mock the executemany method to simulate a DB error
    with patch("sqlite3.connect") as mock_connect:
        # Create a mock connection object
        mock_conn = MagicMock(spec=sqlite3.Connection)
        # Configure the mock_conn to raise an error when executemany is called
        mock_conn.executemany.side_effect = sqlite3.Error("Mock Insert Error")

        # Make sqlite3.connect return our mock_conn
        mock_connect.return_value = mock_conn

        with pytest.raises(EmailInsertDbError):
            manager.insert(sample_emails)


# --- Tests for read method ---


def test_read_multiple_emails(email_manager_instance, sample_emails):
    """Test reading multiple existing emails."""
    manager = email_manager_instance
    with patch(
        "common.utils.Utils.unique_id",
        side_effect=[i for i in range(len(sample_emails))],
    ):
        manager.insert(sample_emails)

    read_ids = [email.id for email in sample_emails[:2]]
    read_emails = manager.read(read_ids)
    assert len(read_emails) == 2
    # Check if the correct IDs were retrieved (order might not be guaranteed by SQL IN)
    retrieved_ids = {em.id for em in read_emails}
    assert retrieved_ids == set(read_ids)


def test_read_non_existent_email(email_manager_instance, sample_emails):
    """Test reading a non-existent email ID."""
    manager = email_manager_instance
    with patch(
        "common.utils.Utils.unique_id",
        side_effect=[i for i in range(len(sample_emails))],
    ):
        manager.insert(sample_emails)

    read_emails = manager.read(["non_existent_id"])
    assert len(read_emails) == 0


def test_read_mixed_existent_and_non_existent_emails(
    email_manager_instance, sample_emails
):
    """Test reading a mix of existing and non-existent email IDs."""
    manager = email_manager_instance
    with patch(
        "common.utils.Utils.unique_id",
        side_effect=[i for i in range(len(sample_emails))],
    ):
        manager.insert(sample_emails)

    read_emails = manager.read(
        [sample_emails[0].id, "non_existent_id", sample_emails[2].id]
    )
    assert len(read_emails) == 2
    retrieved_ids = {em.id for em in read_emails}
    assert retrieved_ids == {sample_emails[0].id, sample_emails[2].id}


def test_read_no_email_ids(email_manager_instance):
    """Test reading with an empty list of email IDs."""
    manager = email_manager_instance
    read_emails = manager.read([])
    assert len(read_emails) == 0


def test_read_db_error(email_manager_instance, sample_emails):
    """Test that EmailReadDbError is raised on database read failure."""
    manager = email_manager_instance
    with patch("sqlite3.connect") as mock_connect:
        # Create a mock connection object
        mock_conn = MagicMock(spec=sqlite3.Connection)
        # Configure the mock_conn to raise an error when execute is called
        mock_conn.execute.side_effect = sqlite3.Error("Mock Read Error")

        # Make sqlite3.connect return our mock_conn
        mock_connect.return_value = mock_conn

        with pytest.raises(EmailReadDbError):
            manager.read([sample_emails[0].id])


# --- Tests for filter method ---


@pytest.fixture
def populated_email_manager(email_manager_instance, sample_emails):
    """Fixture to provide an EmailManager with pre-inserted sample emails."""
    manager = email_manager_instance
    # Ensure unique_id generates consistent Pks for predictable FTS rowids
    with patch(
        "common.utils.Utils.unique_id",
        side_effect=[i for i in range(len(sample_emails))],
    ):
        manager.insert(sample_emails)
    return manager


def test_filter_by_sender_equals(populated_email_manager):
    manager = populated_email_manager
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "sender",
                    "predicate": "Equals",
                    "value": "alice@example.com",
                }
            ],
        }
    }
    filtered_emails = manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
    assert len(filtered_emails) == 1
    assert filtered_emails[0].id == "email1"


def test_filter_by_recipient_equals(populated_email_manager):
    manager = populated_email_manager
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "recipient",
                    "predicate": "Equals",
                    "value": "bob@example.com",
                }
            ],
        }
    }
    filtered_emails = manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
    assert len(filtered_emails) == 2
    retrieved_ids = {em.id for em in filtered_emails}
    assert retrieved_ids == {"email1", "email3"}


def test_filter_by_subject_contains(populated_email_manager):
    manager = populated_email_manager
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "subject",
                    "predicate": "Contains",
                    "value": "Meeting",
                }
            ],
        }
    }
    filtered_emails = manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
    assert len(filtered_emails) == 1
    assert filtered_emails[0].id == "email1"


def test_filter_by_body_contains(populated_email_manager):
    manager = populated_email_manager
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "plain_text_body",
                    "predicate": "Contains",
                    "value": "Project update",
                }
            ],
        }
    }
    filtered_emails = manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
    assert len(filtered_emails) == 1
    assert filtered_emails[0].id == "email2"


def test_filter_by_multiple_contains_all_predicate(populated_email_manager):
    manager = populated_email_manager
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "plain_text_body",
                    "predicate": "Contains",
                    "value": "3 PM",
                },
                {
                    "column_name": "subject",
                    "predicate": "Contains",
                    "value": "Meeting",
                },
            ],
        }
    }
    filtered_emails = manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
    assert len(filtered_emails) == 1
    assert filtered_emails[0].id == "email1"


def test_filter_by_multiple_contains_any_predicate(populated_email_manager):
    manager = populated_email_manager
    req_dict = {
        "filter": {
            "predicate": "OR",
            "rules": [
                {
                    "column_name": "plain_text_body",
                    "predicate": "Contains",
                    "value": "latest news",
                },
                {
                    "column_name": "subject",
                    "predicate": "Contains",
                    "value": "Meeting",
                },
            ],
        }
    }
    filtered_emails = manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
    assert len(filtered_emails) == 2
    retrieved_ids = {em.id for em in filtered_emails}
    assert retrieved_ids == {"email1", "email3"}


def test_filter_by_combined_lookup_and_fts_all_predicate(populated_email_manager):
    manager = populated_email_manager
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "sender",
                    "predicate": "Equals",
                    "value": "charlie@company.com",
                },
                {
                    "column_name": "plain_text_body",
                    "predicate": "Contains",
                    "value": "update",
                },
            ],
        }
    }
    filtered_emails = manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
    assert len(filtered_emails) == 1
    assert filtered_emails[0].id == "email2"


def test_filter_by_combined_lookup_and_fts_any_predicate(populated_email_manager):
    manager = populated_email_manager
    req_dict = {
        "filter": {
            "predicate": "OR",
            "rules": [
                {
                    "column_name": "subject",
                    "predicate": "Contains",
                    "value": "Meeting",
                },
                {
                    "column_name": "recipient",
                    "predicate": "Equals",
                    "value": "diana@example.com",
                },
            ],
        }
    }
    filtered_emails = manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
    assert len(filtered_emails) == 2
    retrieved_ids = {em.id for em in filtered_emails}
    assert retrieved_ids == {"email1", "email4"}


def test_filter_by_not_contains_body(populated_email_manager):
    manager = populated_email_manager
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "plain_text_body",
                    "predicate": "Does Not Contain",
                    "value": "meeting",
                },
            ],
        }
    }
    filtered_emails = manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
    assert len(filtered_emails) == 3  # Should exclude email1 ("meeting")
    retrieved_ids = {em.id for em in filtered_emails}
    assert "email1" not in retrieved_ids


def test_filter_by_received_at_less_than(populated_email_manager):
    manager = populated_email_manager
    # Sample emails received at 1678886400 (March 15, 2023)
    # Test for emails received before a slightly later timestamp
    later_timestamp_s = 1678886500  # Slightly later
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "received_at",
                    "predicate": "Less Than",
                    "value": later_timestamp_s,
                },
            ],
        }
    }
    filtered_emails = manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
    assert (
        len(filtered_emails) == 4
    )  # All emails should be less than this future timestamp


def test_filter_by_received_at_greater_than(populated_email_manager):
    manager = populated_email_manager
    # Sample emails received at 1678886400 (March 15, 2023)
    # Test for emails received after a slightly earlier timestamp
    earlier_timestamp_s = 1678886300  # Slightly earlier
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "received_at",
                    "predicate": "Greater Than",
                    "value": earlier_timestamp_s,
                },
            ],
        }
    }
    filtered_emails = manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
    assert (
        len(filtered_emails) == 4
    )  # All emails should be greater than this past timestamp


def test_filter_no_matching_emails(populated_email_manager):
    manager = populated_email_manager
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "sender",
                    "predicate": "Equals",
                    "value": "non_existent@example.com",
                },
            ],
        }
    }
    filtered_emails = manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
    assert len(filtered_emails) == 0


def test_filter_invalid_column_name(email_manager_instance):
    """Test that ValueError is raised for invalid column names."""
    manager = email_manager_instance
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "invalid_column",
                    "predicate": "Equals",
                    "value": "test",
                },
            ],
        }
    }
    with pytest.raises(pydantic.ValidationError):
        manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore


def test_filter_invalid_predicate_type(email_manager_instance):
    """Test that ValueError is raised for invalid predicate values in rules."""
    manager = email_manager_instance
    # Intentionally create a rule with a bad predicate value (not EQUALS, NOT_EQUALS, etc.)
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "sender",
                    "predicate": "INVALID_PREDICATE",
                    "value": "test",
                },
            ],
        }
    }
    with pytest.raises(pydantic.ValidationError):
        manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore


def test_filter_db_error(populated_email_manager):
    """Test that EmailFilterDbError is raised on database filter failure."""
    manager = populated_email_manager
    req_dict = {
        "filter": {
            "predicate": "AND",
            "rules": [
                {
                    "column_name": "sender",
                    "predicate": "Equals",
                    "value": "alice@example.com",
                },
            ],
        }
    }
    # Mock the execute method to simulate a DB error
    with patch("sqlite3.connect") as mock_connect:
        # Create a mock connection object
        mock_conn = MagicMock(spec=sqlite3.Connection)
        # Configure the mock_conn to raise an error when executemany is called
        mock_conn.execute.side_effect = sqlite3.Error("Mock Filter Error")

        # Make sqlite3.connect return our mock_conn
        mock_connect.return_value = mock_conn

        with pytest.raises(EmailFilterDbError):
            manager.filter(FilterEmailsRequest(**req_dict))  # type: ignore
