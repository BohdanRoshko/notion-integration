import json
import time

import requests

API_ENDPOINT = "https://api.monobank.ua/"
CLIENT_INFO_PATH = "/personal/client-info"
TRANSACTION_PATH = "/personal/statement/{account}/{frm}/{to}"

FIELDS = {
    'time': 'time',
    'description': 'description',
    'amount': 'amount',
    'balance': 'rest',
    'mcc': 'terminal',
}

BANK_NAME = "monobank"


def get_transactions(frm: int, to: int, card_id: str, limits_history: dict, token: str):
    url = API_ENDPOINT + TRANSACTION_PATH.format(
        account=card_id,
        frm=frm,
        to=to,
    )
    resp = requests.get(url, headers={"X-Token": token})
    resp.raise_for_status()

    with open("mcc_codes.json") as f:
        mcc_db = json.load(f)
        mcc_map = {int(mcc['mcc']): mcc['edited_description'] for mcc in mcc_db}

    card_info = get_card_info(card_id, token)
    limits_history = {int(k): v for k, v in limits_history.items()}
    limits_history[int(time.time())] = card_info['creditLimit']
    limits_history_ts = sorted(limits_history.keys(), reverse=True)

    transactions = []
    for transaction in resp.json():
        transaction_data = {name: transaction[f] for f, name in FIELDS.items()}

        transaction_data['bank'] = BANK_NAME
        transaction_data['terminal'] = mcc_map.get(transaction_data['terminal'], str(transaction_data['terminal']))
        for i, ts in enumerate(limits_history_ts):
            if ts <= transaction_data['time']:
                transaction_data['rest'] -= limits_history[limits_history_ts[i - 1]]

        transactions.append(transaction_data)
    return transactions


def get_card_info(card_id: str, token: str):
    url = API_ENDPOINT + CLIENT_INFO_PATH
    resp = requests.get(url, headers={"X-Token": token})
    resp.raise_for_status()

    for account in resp.json()['accounts']:
        if account['id'] == card_id:
            return account
    raise BaseException(f"Not found card with id {card_id}")


def transactions_generator(to_ts, card_id: str, token: str):
    chunk_size = 2682000
    cur_ts = int(time.time())
    for ts in range(cur_ts, to_ts, -chunk_size):
        for transaction in get_transactions(ts - chunk_size, ts, card_id, token):
            yield transaction
        time.sleep(60)


def get_credit_limits(card_id: str, token: str):
    limits_change = {}

    tr_generator = transactions_generator(1543622400, card_id, token)  # 01/12/2018
    last_transaction = next(tr_generator)
    for transaction in tr_generator:
        expected_rest = last_transaction['rest'] - last_transaction['amount']
        rest_diff = expected_rest - transaction['rest']
        if rest_diff:
            limits_change[transaction['time']] = rest_diff
        last_transaction = transaction

    card_info = get_card_info(card_id, token)
    cur_limit = card_info['creditLimit']

    credit_limits = {}
    for ts, limit in sorted(limits_change.items(), key=lambda v: -v[0]):
        cur_limit -= limit
        credit_limits[ts] = cur_limit
    return credit_limits
