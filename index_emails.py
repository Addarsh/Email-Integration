from services.gmail_service import GmailService
from services.email_service import ListEmailsRequest
from database.email_manager import EmailManager
from models.email import Email
from typing import List, Optional

if __name__ == "__main__":
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
    emails: List[Email] = []
    for i in range(n):
        req = ListEmailsRequest(
            senders=["support@rapidapi.com"],
            cur_page_token=cur_page_token,
            page_size=batch_size,
        )
        response = gmail_service.list_emails(req)
        print(f"Got {len(response.emails)} emails in iteration: {i+1}")
        emails.extend(response.emails)

        if response.next_page_token == None:
            # No more results.
            break
        cur_page_token = response.next_page_token

    email_manager.insert(emails)
    email_manager.read(email_ids=[email.id for email in emails])
