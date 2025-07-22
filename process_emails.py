import sys
import logging
import json
from database.email_manager import EmailManager
from models.rule import EmailRulesConfig
from services.gmail_service import GmailService
from services.email_service import BatchUpdateEmailsRequest
from models.email import FilterEmailsRequest, EmailColumnName, Email, EmailLabel
from typing import List

logger = logging.getLogger(__name__)


class RulesProcessorError(Exception):
    pass


class RulesProcessor:

    def __init__(self, source_path: str) -> None:
        self._path = source_path
        data = None
        try:
            with open(source_path, "r") as f:
                data = f.read()
            self._rule_config = EmailRulesConfig(**json.loads(data))
        except Exception as e:
            raise RulesProcessorError("Failed to initialize RulesProcessor") from e

    @property
    def collections(self) -> List[EmailRulesConfig.EmailRulesCollection]:
        return self._rule_config.collections

    def transform_to_db_request(
        self, rule_collection: EmailRulesConfig.EmailRulesCollection
    ) -> FilterEmailsRequest:
        """Transforms given collection of rules to a database filter request for Emails."""
        collection_predicate_map = {
            EmailRulesConfig.EmailRulesCollection.CollectionPredicate.ALL: FilterEmailsRequest.RulesCollection.CollectionPredicate.ALL,
            EmailRulesConfig.EmailRulesCollection.CollectionPredicate.ANY: FilterEmailsRequest.RulesCollection.CollectionPredicate.ANY,
        }

        db_collection_predicate = collection_predicate_map.get(
            rule_collection.predicate
        )
        if db_collection_predicate == None:
            logger.error(
                f"Invalid Collection Rule Predicate value in collection: {rule_collection}"
            )
            raise RulesProcessorError("Invalid Collection Rule Predicate value")

        rule_column_map = {
            EmailRulesConfig.Rule.FieldName.FROM: EmailColumnName.SENDER,
            EmailRulesConfig.Rule.FieldName.TO: EmailColumnName.RECIPIENT,
            EmailRulesConfig.Rule.FieldName.SUBJECT: EmailColumnName.SUBJECT,
            EmailRulesConfig.Rule.FieldName.MESSAGE: EmailColumnName.PLAIN_TEXT_BODY,
            EmailRulesConfig.Rule.FieldName.RECEIVED_DATE: EmailColumnName.RECEIVED_AT,
        }
        rule_predicate_map = {
            EmailRulesConfig.Rule.Predicate.CONTAINS: FilterEmailsRequest.Rule.Predicate.CONTAINS,
            EmailRulesConfig.Rule.Predicate.NOT_CONTAINS: FilterEmailsRequest.Rule.Predicate.NOT_CONTAINS,
            EmailRulesConfig.Rule.Predicate.EQUALS: FilterEmailsRequest.Rule.Predicate.EQUALS,
            EmailRulesConfig.Rule.Predicate.NOT_EQUALS: FilterEmailsRequest.Rule.Predicate.NOT_EQUALS,
            EmailRulesConfig.Rule.Predicate.LESS_THAN: FilterEmailsRequest.Rule.Predicate.LESS_THAN,
            EmailRulesConfig.Rule.Predicate.GREATER_THAN: FilterEmailsRequest.Rule.Predicate.GREATER_THAN,
        }

        db_rules: List[FilterEmailsRequest.Rule] = []
        for rule in rule_collection.rules:
            column_name = rule_column_map.get(rule.field_name)
            if column_name == None:
                logger.error(
                    f"Invalid Rule Field name in collection: {rule_collection}"
                )
                raise RulesProcessorError("Invalid Rule Field name value")

            predicate = rule_predicate_map.get(rule.predicate)
            if predicate == None:
                logger.error(
                    f"Invalid Rule Predicate value in collection: {rule_collection}"
                )
                raise RulesProcessorError("Invalid Rule Predicate value")

            value = rule.value
            if rule.field_name == EmailRulesConfig.Rule.FieldName.RECEIVED_DATE:
                # convert to timestamp in seconds.
                value = EmailRulesConfig.Rule.get_timetamp_seconds(value)

                # Date field has reverse logic in some cases. So 'less than 2 years old' means timestamp > 2 year timestamp and vice versa.
                if predicate == FilterEmailsRequest.Rule.Predicate.LESS_THAN:
                    predicate = FilterEmailsRequest.Rule.Predicate.GREATER_THAN
                elif predicate == FilterEmailsRequest.Rule.Predicate.GREATER_THAN:
                    predicate = FilterEmailsRequest.Rule.Predicate.LESS_THAN

            db_rule = FilterEmailsRequest.Rule(
                column_name=column_name, predicate=predicate, value=value
            )
            db_rules.append(db_rule)

        return FilterEmailsRequest(
            filter=FilterEmailsRequest.RulesCollection(
                predicate=db_collection_predicate, rules=db_rules
            )
        )

    def create_batch_update_emails_request(
        self,
        emails: List[Email],
        rule_collection: EmailRulesConfig.EmailRulesCollection,
    ) -> BatchUpdateEmailsRequest:
        """Creates a batch update request for given emails based on given rules collection."""
        actions: List[EmailRulesConfig.EmailRulesCollection.Action] = (
            rule_collection.actions
        )
        req = BatchUpdateEmailsRequest(
            ids=[em.id for em in emails], add_label_ids=[], remove_label_ids=[]
        )
        for action in actions:
            if (
                action.type
                == EmailRulesConfig.EmailRulesCollection.Action.Type.MARK_MESSAGE
            ):
                if (
                    action.value
                    == EmailRulesConfig.EmailRulesCollection.Action.Label.READ
                ):
                    # remove Unread label.
                    req.remove_label_ids.append(EmailLabel.UNREAD)
                else:
                    req.add_label_ids.append(EmailLabel.UNREAD)
            else:
                # Move message.
                if (
                    action.value
                    == EmailRulesConfig.EmailRulesCollection.Action.Label.INBOX
                ):
                    # add inbox label.
                    req.add_label_ids.append(EmailLabel.INBOX)
                else:
                    # remove the inbox label and add the given label.
                    mapping = {
                        EmailRulesConfig.EmailRulesCollection.Action.Label.IMPORTANT: EmailLabel.IMPORTANT,
                        EmailRulesConfig.EmailRulesCollection.Action.Label.INBOX: EmailLabel.INBOX,
                        EmailRulesConfig.EmailRulesCollection.Action.Label.SPAM: EmailLabel.SPAM,
                    }
                    if mapping.get(action.value) == None:
                        logger.error(
                            f"Invalid Action label in collection: {rule_collection}"
                        )
                        raise RulesProcessorError("Invalid Action label")
                    req.add_label_ids.append(mapping[action.value])
                    req.remove_label_ids.append(EmailLabel.INBOX)

        return req


def run():
    """
    Reads rules from rules JSON file and returns rules object.

    Based on rules, will lookup emails.

    Actions are then taken on those emails according to the rules.
    """
    rules_processor = RulesProcessor("rules_config.json")
    email_manager = EmailManager("emails.db")
    email_service = GmailService()

    for rule_collection in rules_processor.collections:
        filter_req = rules_processor.transform_to_db_request(rule_collection)
        emails: List[Email] = email_manager.filter(filter_req)

        update_req = rules_processor.create_batch_update_emails_request(
            emails, rule_collection
        )
        email_service.batch_update_emails(update_req)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("processor.log"),  # Log to a file
            logging.StreamHandler(),  # Also log to the console
        ],
    )
    try:
        run()
    except Exception as e:
        logging.exception("Email indexing failed")
        sys.exit(1)
