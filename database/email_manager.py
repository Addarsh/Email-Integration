import sqlite3
from models.email import Email
from typing import List
from utils import Utils


class EmailManager:

    def __init__(self, db_name: str) -> None:
        self.db_name = db_name
        self._create_table()

    def insert(self, emails: List[Email]):
        """Insert a batch of emails into the database in a single transaction."""
        if len(emails) == 0:
            print("No emails to insert, nothing to do.")
            return

        conn = None
        try:
            conn = sqlite3.connect(self.db_name)
            data = tuple([em.model_dump() for em in emails])
            with conn:
                # Insert or ignore so that any duplicate email IDs are skipped during the write.
                conn.executemany(
                    "INSERT OR IGNORE INTO emails VALUES(:id, :sender, :recipient, :subject, :plain_text_body, :received_at)",
                    data,
                )

            print(f"Inserted {len(emails)} emails into database successfully")
        except Exception as e:
            print(f"Failed insert Email with error: {e}")
        finally:
            if conn:
                conn.close()

    def read(self, email_ids: List[str]) -> List[Email]:
        """Reads emails with given IDs from the database."""
        if len(email_ids) == 0:
            print("No email Ids to reads, nothing to do.")
            return []

        conn = None
        emails = []
        try:
            conn = sqlite3.connect(self.db_name)
            placeholders = ", ".join(["?" for _ in email_ids])
            query = f"SELECT id, sender, recipient, subject, plain_text_body, received_at FROM emails WHERE id IN ({placeholders})"
            for row in conn.execute(query, tuple(email_ids)):
                emails.append(
                    Email(
                        id=row[0],
                        sender=row[1],
                        recipient=row[2],
                        subject=row[3],
                        plain_text_body=row[4],
                        received_at=Utils.timestamp_seconds_to_datetime(row[5]),
                    )
                )
            print(f"Read: {len(emails)} emails from database successfully")
        except Exception as e:
            print(f"Failed read Emails with error: {e}")
        finally:
            if conn:
                conn.close()
            return emails

    def _create_table(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_name)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS emails  (
                    id TEXT PRIMARY KEY,
                    sender TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    plain_text_body TEXT NOT NULL,
                    received_at INTEGER NOT NULL
                );
            """
            )
        except Exception as e:
            print(f"Failed insert Email with error: {e}")
        finally:
            if conn:
                conn.close()
