from services.gmail_service import GmailService
from services.email_service import ListEmailsRequest
from database.email_manager import EmailManager

if __name__ == "__main__":
    gmail_service = GmailService()
    email_manager = EmailManager("emails.db")
    response = gmail_service.list(list_emails_req=ListEmailsRequest(senders=["support@rapidapi.com"], max_results=20))
    print(f"Got email results count: {response.count}")
    id_map = {}
    for i, email in enumerate(response.emails):
        print(f"\n\nEmail {i}:\n{email.model_dump_json(indent=2)}")
        if email.id not in id_map:
            id_map[email.id] = 0
        id_map[email.id] += 1

    email_manager.insert(response.emails)
    email_manager.read(email_ids=[email.id for email in response.emails])
