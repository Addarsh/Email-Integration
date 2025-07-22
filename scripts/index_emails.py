import logging
import sys
import argparse
from services.gmail_service import GmailService
from services.email_service import ListEmailIdsRequest, GetEmailsRequest
from database.email_manager import EmailManager
from models.email import Email
from typing import List, Optional

logger = logging.getLogger(__name__)


def run(max_emails_count: int, batch_size: int, email_senders: List[str]):
    gmail_service = GmailService()
    email_manager = EmailManager("emails.db")

    n = (
        max_emails_count // batch_size
        if max_emails_count % batch_size == 0
        else (max_emails_count // batch_size + 1)
    )

    cur_page_token: Optional[str] = None
    # emails to index.
    emails: List[Email] = []
    for i in range(n):

        # Fetch email ids.
        req = ListEmailIdsRequest(
            senders=email_senders,
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
        email_manager.insert(emails)
        logger.info(f"Indexed {len(emails)} emails successfully")
    else:
        logger.info("No new emails to index")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default is INFO."
    )
    parser.add_argument(
        "--email_senders",
        type=str,
        nargs='*',
        default=[],
        help="Filter emails by list of sender emails. Default is empty list."
    )
    parser.add_argument(
        "--max_count",
        type=int,
        default=100,
        help="Max number of emails to index. Default is 100."
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=10,
        help="Batch size when fetching emails. Default is 10."
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("indexer.log"),  # Log to a file
            logging.StreamHandler(),  # Also log to the console
        ],
    )
    try:
        run(max_emails_count=args.max_count, batch_size=args.batch_size, email_senders=args.email_senders)
    except Exception as e:
        logging.exception("Email indexing failed")
        sys.exit(1)
