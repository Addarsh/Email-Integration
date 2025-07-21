from database.email_manager import EmailManager, FilterEmailsRequest

if __name__ == "__main__":
    email_mgr = EmailManager("emails.db")
    filter_req = FilterEmailsRequest(
        filter=FilterEmailsRequest.RulesCollection(
            rules=[
                FilterEmailsRequest.Rule(
                    column_name='sender',
                    predicate=FilterEmailsRequest.Rule.Predicate.CONTAINS,
                    value="thoughtspot"
                ),
                FilterEmailsRequest.Rule(
                    column_name='recipient',
                    predicate=FilterEmailsRequest.Rule.Predicate.NOT_CONTAINS,
                    value="chandrasekar"
                ),
                FilterEmailsRequest.Rule(
                    column_name='received_at',
                    predicate=FilterEmailsRequest.Rule.Predicate.LESS_THAN,
                    value=1752748114,
                ),
                # FilterEmailsRequest.Rule(
                #     column_name='subject',
                #     predicate=FilterEmailsRequest.Rule.Predicate.EQUALS,
                #     value="ThoughtSpot | Interview confirmation - Addarsh Chandrasekar",
                # )
            ],
            predicate=FilterEmailsRequest.RulesCollection.CollectionPredicate.ALL
        )
    )
    email_mgr.filter(filter_req)