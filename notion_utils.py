from datetime import datetime

from notion.client import NotionClient
from notion.collection import NotionDate


def get_last_table_update(url: str, date_field: str, token: str):
    client = NotionClient(token_v2=token)
    cv = client.get_collection_view(url)
    res = cv.build_query(aggregate=[{"aggregator": "latest_date", "property": date_field, }]).execute()
    return int(NotionDate.from_notion(res.aggregates[0]['value']).start.timestamp())


def insert_transactions_to_notion(url: str, transactions: list, token: str):
    client = NotionClient(token_v2=token)
    cv = client.get_collection_view(url)

    for transaction in transactions:
        row = cv.collection.add_row()
        row.description = transaction['description']
        row.bank = transaction['bank']
        row.terminal = transaction['terminal']
        row.amount = transaction['amount'] / 100
        row.rest = transaction['rest'] / 100
        row.time = NotionDate(start=datetime.fromtimestamp(transaction['time']), timezone="Europe/Kiev")

    return cv.default_query().execute()


def get_transactions_after(url: str, date_field: str, after_ts: int, token: str):
    client = NotionClient(token_v2=token)
    cv = client.get_collection_view(url)
    res = cv.build_query(
        filter=[
            {
                "property": date_field,
                "filter": {
                    "operator": "date_is_on_or_after",
                    "value": {
                        "type": "exact",
                        "value": {
                            "type": "date",
                            "start_date": f"{datetime.fromtimestamp(after_ts):%Y-%m-%d}"
                        }
                    }
                }
            }
        ]
    ).execute()

    return [row.get_all_properties() for row in res]
