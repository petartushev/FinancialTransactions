import psycopg2
from psycopg2 import sql
import numpy as np
import pandas as pd
import hashlib

from faker import Faker

N = 10
SEED = 0
CURRENCY = 'USD'

Faker.seed(SEED)

generator = Faker()

connection = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="localhost", port=5432)
curr = connection.cursor()

select_query = sql.SQL('SELECT * FROM postgres.public.Client')

try:

    curr.execute(select_query)

    rows = curr.fetchall()

    df = pd.DataFrame(rows, columns=['client_embg', 'client_name', 'date_of_birth'])


except Exception as e:

    print(e)

balance = np.random.normal(loc=200000, scale=30000, size=df.shape[0])

df['balance'] = balance

def generate_account_id(input: str):

    sha256_hash = hashlib.sha256(input.encode())

    sha256_hash_hex = sha256_hash.hexdigest()

    account_id = int(int(sha256_hash_hex, base=16) // 1e+60)

    account_id = str(account_id)

    if len(account_id) != 17:

        account_id += f'{np.random.randint(0, 9)}'

    return account_id


insert_query = sql.SQL('INSERT INTO postgres.public.Account (account_id, client_embg, currency_type, balance) VALUES (%s, %s, %s, %s)')

all_accounts = set()
i = 0

for row in df.iterrows():

    input_string = ''.join(str(val) for val in list(row[1][:-1]))
 
    account_id = generate_account_id(input_string)

    while account_id in all_accounts:

        print(f'Account id confilct occured.')
        print(f'Newly generated account id: {account_id} is already taken.')

        account_id = generate_account_id(f'{input_string}{i}')

        i += 1



    try:

        all_accounts.add(account_id)

        curr.execute(insert_query, (int(account_id), row[1]['client_embg'], CURRENCY, row[1]['balance']))

        i = 0

    except Exception as e:

        print(e)



connection.commit()

connection.close()


