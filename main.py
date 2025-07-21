from services.gmail_service import GmailService
from services.email_service import ListEmailsRequest
from database.email_manager import EmailManager, FilterEmailsRequest

if __name__ == "__main__":
    # gs = GmailService()
    # response = gs.list(list_emails_req=ListEmailsRequest(senders=["sanaa.p@thoughtspot.com>"], max_results=2))
    # print(f"Got email results count: {response.count}")
    # for i, email in enumerate(response.emails):
    #     print(f"\n\nEmail {i}:\n{email}")

    email_mgr = EmailManager("emails.db")
    # email_mgr.insert(response.emails)
    # email_mgr.read(email_ids=[email.id for email in response.emails])

    filter_req = FilterEmailsRequest(
        filter=FilterEmailsRequest.RulesCollection(
            rules=[
                # FilterEmailsRequest.Rule(
                #     column_name='sender',
                #     predicate=FilterEmailsRequest.Rule.Predicate.CONTAINS,
                #     value="thoughtspot"
                # ),
                FilterEmailsRequest.Rule(
                    column_name='recipient',
                    predicate=FilterEmailsRequest.Rule.Predicate.CONTAINS,
                    value="Chandrasekar"
                ),
                # FilterEmailsRequest.Rule(
                #     column_name='received_at',
                #     predicate=FilterEmailsRequest.Rule.Predicate.GREATER_THAN,
                #     value=1752665984,
                # ),
                FilterEmailsRequest.Rule(
                    column_name='subject',
                    predicate=FilterEmailsRequest.Rule.Predicate.EQUALS,
                    value="ThoughtSpot | Interview confirmation - Addarsh Chandrasekar",
                )
            ],
            predicate=FilterEmailsRequest.RulesCollection.CollectionPredicate.ANY
        )
    )
    email_mgr.filter(filter_req)