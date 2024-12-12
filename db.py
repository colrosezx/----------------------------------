import asyncio
import asyncpg
from config import user, host, password, db_name

async def create_db():
    # Подключение к существующей базе данных (например, postgres)
    conn = await asyncpg.connect(
        user=user,
        password=password,
        database='postgres',
        host=host,
    )

    try:
        await conn.execute(f'CREATE DATABASE "{db_name}"')
        print(f'Database {db_name} created successfully.')
    except asyncpg.exceptions.DuplicateDatabaseError:
        print(f'Database {db_name} already exists.')
    finally:
        await conn.close()

async def create_table():
    conn = None
    try:
        print('Connecting to the database...')
        conn = await asyncpg.connect(
            user=user,
            password=password,
            database=db_name,
            host=host
        )
        print('Connection ok')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS wb_cards (
                Id SERIAL PRIMARY KEY,
                Article INT,
                Marketplace VARCHAR(50),
                CardName VARCHAR(500),
                Url VARCHAR(1000),
                Card_Price_With_Discount FLOAT,
                Card_Price_Without_Discount FLOAT,
                Quantity_Of_Goods INT,
                BrandName VARCHAR(100),
                Rating FLOAT,
                Count_Feedbacks INT
            )
        ''')
        print('Table created successfully.')
    except Exception as e:
        print(f'Error creating table: {e}')
    finally:
        if conn:
            await conn.close()


async def insert_into_database(marketplace, product_url, product_article,
                               product_name, card_price_without_discount,
                               card_price_with_discount, quantity_of_goods,
                               brand_name, rating, count_feedbacks):
    try:
        connection = await asyncpg.connect(
            host=host,
            user=user,
            password=password,
            database=db_name
        )

        await connection.execute(
            """
            INSERT INTO wb_cards (
                Article,
                Marketplace,
                CardName,
                Url,
                Card_Price_With_Discount,
                Card_Price_Without_Discount,
                Quantity_Of_Goods,
                BrandName,
                Rating,
                Count_Feedbacks
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
            )
            """,
            product_article, marketplace, product_name, product_url,
            card_price_with_discount, card_price_without_discount, quantity_of_goods,
            brand_name, rating, count_feedbacks
        )

        print('Object already downloaded to table')

    except Exception as _ex:
        print(_ex)

    finally:
        if connection:
            await connection.close()

## 1 шаг Сначала создаем базу данных
# asyncio.run(create_db())

## 2 шаг создаем таблицу
# asyncio.run(create_table())