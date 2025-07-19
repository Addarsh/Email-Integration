from gmail_service import GmailService
from email_service import ListEmailsRequest

if __name__ == "__main__":
    gs = GmailService()

    gs.list(list_emails_req=ListEmailsRequest(senders=["sanaa.p@thoughtspot.com>"], max_results=5))