import sqlite3
from models.email import Email
from typing import List, Any
from utils import Utils
from pydantic import BaseModel, Field
from enum import StrEnum

class FilterEmailsRequest(BaseModel):

    class Rule(BaseModel):
        class Predicate(StrEnum):
            CONTAINS = "Contains"
            NOT_CONTAINS = "Does Not Contain"
            EQUALS = "Equals"
            NOT_EQUALS = "Does Not Equal"
            LESS_THAN = "Less Than"
            GREATER_THAN = "Greater Than"
        
        column_name: str = Field(description="This must exactly match a valid column the table otherwise the filter will fail.")
        predicate: Predicate
        value: Any
    
    class RulesCollection(BaseModel):
        class CollectionPredicate(StrEnum):
            ANY = "Any"
            ALL = "All"

        rules: List['FilterEmailsRequest.Rule']
        predicate: CollectionPredicate

    
    filter: RulesCollection

    @property
    def column_names(self) -> List[str]:
        return [rule.column_name for rule in self.filter.rules]

class EmailManager:
    _VALID_COLUMNS = ['pk', 'id', 'sender', 'recipient', 'subject', 'plain_text_body', 'received_at']

    def __init__(self, db_name: str) -> None:
        self._db_name = db_name
        self._create_table()
        self._pk: int = 1

    def insert(self, emails: List[Email]):
        """Insert a batch of emails into the database in a single transaction."""
        if len(emails) == 0:
            print("No emails to insert, nothing to do.")
            return

        conn = None
        try:
            conn = sqlite3.connect(self._db_name)
            for em in emails:
                em.pk = self._pk
                self._pk += 1
            data = tuple([em.model_dump() for em in emails])
            with conn:
                # Insert or ignore so that any duplicate email IDs are skipped during the write.
                conn.executemany(
                    "INSERT OR IGNORE INTO emails VALUES(:pk, :id, :sender, :recipient, :subject, :plain_text_body, :received_at)",
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
            conn = sqlite3.connect(self._db_name)
            placeholders = ", ".join(["?" for _ in email_ids])
            query = f"SELECT pk, id, sender, recipient, subject, plain_text_body, received_at FROM emails WHERE id IN ({placeholders})"
            for row in conn.execute(query, tuple(email_ids)):
                emails.append(
                    Email(
                        pk=row[0],
                        id=row[1],
                        sender=row[2],
                        recipient=row[3],
                        subject=row[4],
                        plain_text_body=row[5],
                        received_at=Utils.timestamp_seconds_to_datetime(row[6]),
                    )
                )
            print(f"Read: {len(emails)} emails from database successfully")
        except Exception as e:
            print(f"Failed read Emails with error: {e}")
        finally:
            if conn:
                conn.close()
            return emails
        
    def filter(self, req: FilterEmailsRequest) -> List[Email]:
        """
        Filter emails with the given criteria in the request.
        
        It will intelligently query the right index (secondary or Full text search or both) depending on the column name
        and the predicate type.
        """
        conn = None
        emails = []
        try:
            if len(req.column_names) == 0:
                raise ValueError("No column names found in filter req")
            if any([column_name not in set(EmailManager._VALID_COLUMNS) for column_name in req.column_names]):
                raise ValueError("One or more column names in req are invalid")
            
            join_predicate = ""
            if req.filter.predicate == FilterEmailsRequest.RulesCollection.CollectionPredicate.ALL:
                join_predicate = " AND "
            elif req.filter.predicate == FilterEmailsRequest.RulesCollection.CollectionPredicate.ANY:
                join_predicate = " OR "
            else:
                raise ValueError("Invalid predicate value in rules collection")

            # so we need read the filters and create where clauses accoringly.
            # we start from lower predicates and then combine them with OR or AND depending
            # on the outer predicate. Equals,Not Equals, Less Than, Greater Than
            # are converted to secondary index query i.e. where column =, <, >, value
            # Contains is converted to FTS: MATCH 'column: "value"'.          
            search_rules: List[FilterEmailsRequest.Rule] = []
            db_lookup_rules: List[FilterEmailsRequest.Rule] = []
            for rule in req.filter.rules:
                if rule.predicate in set([FilterEmailsRequest.Rule.Predicate.CONTAINS, FilterEmailsRequest.Rule.Predicate.NOT_CONTAINS]):
                    # Full text search predicate.
                    search_rules.append(rule)
                elif rule.predicate in set([FilterEmailsRequest.Rule.Predicate.EQUALS, FilterEmailsRequest.Rule.Predicate.NOT_EQUALS, FilterEmailsRequest.Rule.Predicate.LESS_THAN, FilterEmailsRequest.Rule.Predicate.GREATER_THAN]):
                    # Main Table lookup predicate.
                    db_lookup_rules.append(rule)
                else:
                    raise ValueError(f"Invalid predicate value: {rule.predicate}")

            # Filter request is valid.
            base_sql = """
            SELECT
              e.pk, e.id, e.sender, e.recipient, e.subject, e.plain_text_body, e.received_at
            FROM
              emails AS e
            JOIN
              fts_idx_emails AS fts
            ON
              e.pk = fts.rowid
            """
            where_clauses = []
            params: List[Any] = []
            if len(search_rules) > 0:
                # e.g. MATCH 'col_1 : {val_1} AND -col_2: {val_2}'
                search_clauses: List[str] = []
                for rule in search_rules:
                    clause = ""
                    escaped_value = str(rule.value).replace('"', '""')
                    quoted_value = f'"{escaped_value}"'
                    if rule.predicate == FilterEmailsRequest.Rule.Predicate.CONTAINS:
                        clause = f"{rule.column_name} : {quoted_value}"
                    else:
                        # does not contain.
                        clause = f"-{rule.column_name} : {quoted_value}"
                    search_clauses.append(clause)
                
                # The entire FTS query is one string, which will be passed as ONE parameter.
                search_clause_str = f"{join_predicate.join(search_clauses)}"
                params.append(search_clause_str)
                where_clauses.append("fts.fts_idx_emails MATCH ?")

            if len(db_lookup_rules) > 0:
                # e.g. e.col_1 = 'v1' AND e.col_2 = 'val_2'
                lookup_clauses = []
                for rule in db_lookup_rules:
                    clause = f"e.{rule.column_name}"
                    if rule.predicate == FilterEmailsRequest.Rule.Predicate.EQUALS:
                        clause = f"{clause} = ?"
                    elif rule.predicate == FilterEmailsRequest.Rule.Predicate.NOT_EQUALS:
                        clause = f"{clause} != ?"
                    elif rule.predicate == FilterEmailsRequest.Rule.Predicate.LESS_THAN:
                        clause = f"{clause} < ?"
                    else:
                        clause = f"{clause} > ?"
                    lookup_clauses.append(clause)
                    params.append(rule.value)
                
                final_clause = join_predicate.join(lookup_clauses)
                where_clauses.append(final_clause)

            where_condition = f"WHERE {join_predicate.join(where_clauses)}"
            final_query = base_sql + where_condition

            print("final query: ", final_query)
            print("params: ", params)
            
            conn = sqlite3.connect(self._db_name)
            for row in conn.execute(final_query, tuple(params)):
                emails.append(
                    Email(
                        pk=row[0],
                        id=row[1],
                        sender=row[2],
                        recipient=row[3],
                        subject=row[4],
                        plain_text_body=row[5],
                        received_at=Utils.timestamp_seconds_to_datetime(row[6]),
                    )
                )
                
            print(f"Found {len(emails)} emails using filter.")
        except Exception as e:
            print(f"Failed to filter Emails for req: {req} with error: {e}")
        finally:
            if conn:
                conn.close()
            return emails

    def _create_table(self):
        conn = None
        try:
            conn = sqlite3.connect(self._db_name)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS emails  (
                    pk INTEGER PRIMARY KEY,
                    id TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    recipient TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    plain_text_body TEXT NOT NULL,
                    received_at INTEGER NOT NULL
                );
            """
            )

            # Create secondary index on all columns.
            # Define a single string with multiple SQL commands
            sql_script = """
            CREATE INDEX IF NOT EXISTS idx_message_id ON emails (id);
            CREATE INDEX IF NOT EXISTS idx_sender ON emails (sender);
            CREATE INDEX IF NOT EXISTS idx_recipient ON emails (recipient);
            CREATE INDEX IF NOT EXISTS idx_subject ON emails (subject);
            CREATE INDEX IF NOT EXISTS idx_plain_text_body ON emails (plain_text_body);
            CREATE INDEX IF NOT EXISTS idx_received_at ON emails (received_at);
            """
            conn.executescript(sql_script)

            # Create full text search virtual table with external content table pointing to emails.
            # Index for all columns other than id and received_at.
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS fts_idx_emails USING fts5(
                    sender,
                    recipient,
                    subject,
                    plain_text_body,
                    content='emails',
                    content_rowid='pk'
                );
                """
            )

            # Create triggers to keep FTS table in sync with main emails table.
            triggers_sql = """
            CREATE TRIGGER IF NOT EXISTS emails_ai AFTER INSERT ON emails BEGIN
                INSERT INTO fts_idx_emails(rowid, sender, recipient, subject, plain_text_body) VALUES (new.pk, new.sender, new.recipient, new.subject, new.plain_text_body);
            END;
            CREATE TRIGGER IF NOT EXISTS emails_ad AFTER DELETE ON emails BEGIN
                INSERT INTO fts_idx_emails(fts_idx_emails, rowid, sender, recipient, subject, plain_text_body) VALUES ('delete', old.pk, old.sender, old.recipient, old.subject, old.plain_text_body);
            END;
            CREATE TRIGGER IF NOT EXISTS emails_au AFTER UPDATE ON emails BEGIN
                INSERT INTO fts_idx_emails(fts_idx_emails, rowid, sender, recipient, subject, plain_text_body) VALUES( 'delete', old.pk, old.sender, old.recipient, old.subject, old.plain_text_body);
                INSERT INTO fts_idx_emails(rowid, sender, recipient, subject, plain_text_body) VALUES (new.pk, new.sender, new.recipient, new.subject, new.plain_text_body);
            END;
            """
            conn.executescript(triggers_sql)

            conn.commit()

            print("Created table and indices successfully!")
        except Exception as e:
            print(f"Failed insert Email with error: {e}")
        finally:
            if conn:
                conn.close()
