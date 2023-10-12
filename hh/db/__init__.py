import os
import psycopg
from dotenv import load_dotenv

load_dotenv('.env')


URI = os.environ.get('PG_URI')


async def write_log(log_message, status):
    async with await psycopg.AsyncConnection.connect(URI) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute(
                "INSERT INTO logs (log_message, status) VALUES (%s, %s)",
                (log_message, status))
