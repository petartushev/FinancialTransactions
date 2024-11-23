import json
from pathlib import Path

import random
import psycopg2
from psycopg2 import sql
from faker import Faker

path = Path(__file__).parent.parent.parent

with open(path / '.db_credentials.json', 'r') as f:
    credentials = json.load(f)

N = 10
SEED = 0
Faker.seed(SEED)

gender = False

generator = Faker()

connection = psycopg2.connect(
    database=credentials['DB'], 
    user=credentials['DB_USER'], 
    password=credentials['DB_PASS'], 
    host=credentials['DB_HOST'], 
    port=credentials['DB_PORT']
)





cursor = connection.cursor()

for _ in range(N):
    
    if gender:

        full_name = generator.name_male()

        gender = False
    
    else:

        full_name = generator.name_female()

        gender = True

    date_of_birth = generator.date_of_birth(minimum_age=18, maximum_age=70)

    day = date_of_birth.day
    month = date_of_birth.month
    year = date_of_birth.year

    embg_client = f'{day:02d}{month:02d}{year%1000:03d}{generator.random_number(fix_len=True, digits=6)}'

    try:

        insert_query = sql.SQL(
            f'INSERT INTO {credentials["DB"]}.{credentials["DB_SCHEMA"]}.client (client_embg, client_name, date_of_birth) VALUES (%s, %s, %s)'
        )

        cursor.execute(insert_query, (embg_client, full_name, date_of_birth))


    except Exception as e:

        print(e)

    connection.commit()

connection.close()




