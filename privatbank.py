import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime

import requests

DATA_TEMP = '''
<oper>cmt</oper>
<wait>0</wait>
<test>0</test>
<payment id="">
<prop name="sd" value="{frm}" />
<prop name="ed" value="{to}" />
<prop name="card" value="{card}" />
</payment>
'''.replace('\n', '')

BODY_TEMP = '''
<?xml version="1.0" encoding="UTF-8"?>
<request version="1.0">
<merchant>
<id>{m_id}</id>
<signature>{signature}</signature>
</merchant>
<data>{data}</data>
</request>
'''.replace('\n', '')

TRANSACTION_URL = "https://api.privatbank.ua/p24api/rest_fiz"

FIELDS = {
    'description': 'description',
    'cardamount': 'amount',
    'rest': 'rest',
    'terminal': 'terminal',
}

BANK_NAME = "privatbank"


def get_transactions(frm: int, to: int, card: str, merchant: str, pswd: str):
    data = DATA_TEMP.format(
        frm=f"{datetime.fromtimestamp(frm):%d.%m.%Y}",
        to=f"{datetime.fromtimestamp(to):%d.%m.%Y}",
        card=card,
    )

    signature = hashlib.sha1(
        hashlib.md5(
            (data + pswd).encode()
        ).hexdigest().encode()
    ).hexdigest()

    body = BODY_TEMP.format(
        m_id=merchant,
        signature=signature,
        data=data,
    )

    resp = requests.post(TRANSACTION_URL, data=body)
    resp.raise_for_status()

    response_xml = ET.fromstring(resp.content)
    statements = response_xml.findall('.//statement')

    transactions = []
    for statement in statements:
        transaction = {name: statement.attrib[f] for f, name in FIELDS.items()}

        transaction['time'] = int(
            datetime.strptime(
                f"{statement.attrib['trandate']} {statement.attrib['trantime']}",
                "%Y-%m-%d %H:%M:%S"
            ).timestamp()
        )

        amount, _ = transaction['amount'].split()
        transaction['amount'] = int(float(amount) * 100)

        rest, _ = transaction['rest'].split()
        transaction['rest'] = int(float(rest) * 100)

        transaction['bank'] = BANK_NAME

        transactions.append(transaction)
    return transactions
