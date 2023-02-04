import re
from datetime import datetime
from beancount.ingest import importer
from beancount.core.data import Posting, Transaction, Balance, EMPTY_SET, new_metadata
from beancount.core.amount import Amount
from beancount.core.number import D
from beancount.core.flags import FLAG_OKAY
from subprocess import PIPE, Popen

def pdftotext(filename):
    """Convert a PDF file to a text equivalent.
    Args:
    filename: A string path, the filename to convert.
    Returns:
    A string, the text contents of the filename.
    """
    pipe = Popen(['pdftotext', '-enc', 'UTF-8', '-layout', filename, '-'], stdout=PIPE, stderr=PIPE)
    stdout, _stderr = pipe.communicate()
    return stdout.decode()


class Importer(importer.ImporterProtocol):

    def __init__(self, account='Assets:BJBank'):
        self.account = account

    def identify(self, file):
        if file.mimetype() != 'application/pdf':
            return False
        text = file.convert(pdftotext)
        return '北京银行个人客户交易流水清单' in text

    def file_name(self, file):
        return "bjbank.pdf"

    def file_account(self, file):
        return self.account

    def file_date(self, file):
        text = file.convert(pdftotext)
        pattern = re.compile(r'日期范围：\d{4}-\d{2}-\d{2}—(?P<date>\d{4}-\d{2}-\d{2})', re.MULTILINE)
        match = re.search(pattern, text)
        return datetime.strptime(match['date'], "%Y-%m-%d").date()


    def extract(self, file):
        acct = self.file_account(file)
        text = file.convert(pdftotext)
        entries = []
        pattern = re.compile(r'(?P<date>\d{4}-\d{2}-\d{2})\s+人民币\s+钞\s+(?P<narration>[^\s]+)\s+(?P<amount>[+\-\.\d]+)\s+(?P<balance>[\d,\.]+)(?P<payee>\s+[^\s]+)?(?P<payee_no>\s+[^\s]+)?$', re.MULTILINE)
        matches = re.findall(pattern, text)
        for (raw_date, narration, amount, balance, payee, payee_no) in matches:
            date = datetime.strptime(raw_date, "%Y-%m-%d").date()
            payee = payee.strip()
            payee_no = payee_no.strip()
            amount = amount.lstrip('+')
            amount = Amount(D(amount), "CNY")
            other_account = 'Expenses:TODO'

            entries.append(Transaction(
                date = date,
                payee = payee,
                narration = narration,
                meta = new_metadata(file.name, int(1)),
                flag = FLAG_OKAY,
                tags = EMPTY_SET,
                links = EMPTY_SET,
                postings = [
                    Posting(account = acct, units = amount, cost=None, price=None, flag=None, meta=None),
                    Posting(account = other_account, units = None, cost=None, price=None, flag=None, meta=None)
                ]
            ))
        return entries
