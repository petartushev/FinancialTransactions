import random
import psycopg2
from psycopg2 import sql
from faker import Faker
from variables import DB, DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_SCHEMA

N = 10
SEED = 0
Faker.seed(SEED)

gender = False

generator = Faker()

connection = psycopg2.connect(
    database=DB, 
    user=DB_USER, 
    password=DB_PASS, 
    host=DB_HOST, 
    port=DB_PORT
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
            f'INSERT INTO {DB}.{DB_SCHEMA}.client (client_embg, client_name, date_of_birth) VALUES (%s, %s, %s)'
        )

        cursor.execute(insert_query, (embg_client, full_name, date_of_birth))


    except Exception as e:

        print(e)

    connection.commit()

connection.close()




