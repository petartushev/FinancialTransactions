import psycopg2
from psycopg2 import sql
from kafka import KafkaProducer
import numpy as np
import time
import hashlib

from transaction import Transaction
from utils import parse_db_return

def generate_transaction_id(input: str):

    sha256_hash = hashlib.sha256(input.encode())

    sha256_hash_hex = sha256_hash.hexdigest()

    account_id = int(int(sha256_hash_hex, base=16) // 1e+60)

    account_id = str(account_id)

    if len(account_id) != 18:

        account_id += f'{np.random.randint(0, 9)}'

    return account_id

kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: x.encode('utf-8'))

connection = psycopg2.connect(database="postgres", user="postgres", password="postgres", host="localhost", port=5432)

cursor = connection.cursor()

select_query = sql.SQL('SELECT a.account_id FROM postgres.public.account a')

try:

    cursor.execute(select_query)

    account_ids = cursor.fetchall()


except Exception as e:

    print(e)

# finally:

#     connection.close()

account_ids = parse_db_return(account_ids)

try:

    while True:

        from_account = str(np.random.choice(account_ids))
        to_account = str(np.random.choice(account_ids))

        if from_account == to_account:

            continue

        query = '''
        SELECT a.balance 
        FROM postgres.public.account a
        WHERE a.account_id = %s
        '''

        select_query = sql.SQL(query)

        cursor.execute(select_query, (from_account,))

        from_account_balance = cursor.fetchall()

        from_account_balance = parse_db_return(from_account_balance)

        # amount = np.random.chisquare(1)
        amount = np.random.normal(200, 200)

        transaction_time = time.time()

        transaction_id = generate_transaction_id(f'{transaction_time}{from_account}{to_account}{amount}')

        value = str({'transactionId': transaction_id, 'timestamp': int(transaction_time), 'sendingClientAccountNumber': from_account, 'receivingClientAccountNumber': to_account, 'amount': abs(amount)})
        
        kafka_producer.send(topic='transaction', value=value)

        # print(value)

        time.sleep(np.random.random()*11.5)

except KeyboardInterrupt as e:

    print('\nShutting down simulation gracefully.')

finally:

    cursor.close()
    connection.close()
