from services.gmail_service import GmailService
from services.email_service import ListEmailsRequest
from database.email_manager import EmailManager

if __name__ == "__main__":
    gs = GmailService()

    response = gs.list(list_emails_req=ListEmailsRequest(senders=["sanaa.p@thoughtspot.com>"], max_results=2))
    print(f"Got email results count: {response.count}")
    for i, email in enumerate(response.emails):
        print(f"\n\nEmail {i}:\n{email}")

    email_mgr = EmailManager("emails.db")
    email_mgr.insert(response.emails)

    email_mgr.read(email_ids=[email.id for email in response.emails])