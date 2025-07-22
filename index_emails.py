import logging
import sys
from services.gmail_service import GmailService
from services.email_service import ListEmailIdsRequest, GetEmailsRequest
from database.email_manager import EmailManager
from models.email import Email
from typing import List, Optional

logger = logging.getLogger(__name__)


def run():
    gmail_service = GmailService()
    email_manager = EmailManager("emails.db")

    max_emails_to_fetch = 22
    batch_size = 10
    n = (
        max_emails_to_fetch // batch_size
        if max_emails_to_fetch % batch_size == 0
        else (max_emails_to_fetch // batch_size + 1)
    )

    cur_page_token: Optional[str] = None
    # emails to index.
    emails: List[Email] = []
    for i in range(n):

        # Fetch email ids.
        req = ListEmailIdsRequest(
            senders=["support@rapidapi.com"],
            cur_page_token=cur_page_token,
            page_size=batch_size,
        )
        response = gmail_service.list_email_ids(req)
        logger.info(f"Got {len(response.email_ids)} emails in iteration: {i+1}")

        if response.next_page_token == None:
            # No more emails left to parse.
            break

        cur_page_token = response.next_page_token

        # Only index new emails that are not already in the database.
        existing_email_ids: List[str] = [
            em.id for em in email_manager.read(response.email_ids)
        ]
        new_email_ids: List[str] = list(
            set(response.email_ids).difference(set(existing_email_ids))
        )
        if len(new_email_ids) == 0:
            continue

        response = gmail_service.get_emails(
            req=GetEmailsRequest(email_ids=new_email_ids)
        )
        emails.extend(response.emails)

    if len(emails) > 0:
        logger.info(f"Indexing {len(emails)} emails")
        email_manager.insert(emails)
    else:
        logger.info("No new emails to index")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("indexer.log"),  # Log to a file
            logging.StreamHandler(),  # Also log to the console
        ],
    )
    try:
        run()
    except Exception as e:
        logging.exception("Email indexing failed")
        sys.exit(1)
