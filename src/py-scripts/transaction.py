from typing import Union
import hashlib
from datetime import datetime
import json

class Transaction:

    def __init__(self, from_account, to_account, amount: Union[int|float]):

        self.from_account = from_account
        self.to_account = to_account
        self.amount = amount

        self.transaction_id = self._generate_transaction_id()


    def _generate_transaction_id(self):

        hash256_hex = hashlib.sha256(f'{self.from_account}{self.to_account}{self.amount}{datetime.now()}').hexdigest()[:18]

        return hash256_hex.upper()
    
    def to_string(self):

        return {'transaction_id': self.transaction_id, 'from_account': self.from_account, 'to_account': self.to_account, 'amount': self.amount} 

