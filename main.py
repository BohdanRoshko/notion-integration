import json

import privatbank
import monobank

from notion_utils import insert_transactions_to_notion, get_last_table_update, get_transactions_after

if __name__ == '__main__':
    with open("config.json") as f:
        config = json.load(f)
    forward_window = config["forward_window"]
    notion_token = config["notion"]["token"]
    notion_url = config["notion"]["table_url"]
    cards = config["cards"]

    last_update = get_last_table_update(notion_url, "time", notion_token)
    get_transaction_to = last_update + forward_window

    transactions = []
    for card in cards:
        if card["type"] == "privat":
            transactions.extend(
                privatbank.get_transactions(
                    last_update,
                    get_transaction_to,
                    card["card_num"],
                    card["merchant_id"],
                    card["merchant_pass"],
                )
            )
        if card["type"] == "mono":
            transactions.extend(
                monobank.get_transactions(
                    last_update,
                    get_transaction_to,
                    card["card_id"],
                    card["limits_history"],
                    card["token"],
                )
            )

    transactions.sort(key=lambda x: x['time'])

    last_transactions = get_transactions_after(notion_url, "time", last_update, notion_token)
    last_transactions_hash = {
        f"{t['amount'] * 100:.0f}{t['rest'] * 100:.0f}{t['time'].start.timestamp():.0f}"
        for t in last_transactions
    }

    transactions = [
        t
        for t in transactions
        if f"{t['amount']}{t['rest']}{t['time']}" not in last_transactions_hash
    ]

    insert_transactions_to_notion(notion_url, transactions, notion_token)
