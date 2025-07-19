from gmail_service import GmailService
from email_service import ListEmailsRequest

if __name__ == "__main__":
    gs = GmailService()

    response = gs.list(list_emails_req=ListEmailsRequest(senders=["sanaa.p@thoughtspot.com>"], max_results=5))
    print(f"Got email results count: {response.count}")
    for i, email in enumerate(response.emails):
        print(f"\n\nEmail {i}:\n{email}")