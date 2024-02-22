from mysql.connector import connect, Error
from os import environ


username = environ.get("MYSQL_USER")
pwd = environ.get("MYSQL_PWD")
db = environ.get("MYSQL_DB")

if username is None or pwd is None or db is None:
# if username is None or pwd is None:
    raise Exception("Missing $MYSQL_USER or $MYSQL_PWD")

try:
    with connect(host="localhost",user=username,password=pwd, database=db) as conn:
        cursor = conn.cursor()
        # query = ("INSERT INTO cache_eutils (base64_data, cached_value) VALUES ('a1', 'b1')")
        query = ("select * from cache_eutils")
        cursor.execute(query)
        for i in cursor.fetchall():
            print(i)
        # conn.commit()
        cursor.close()
        conn.close()

except Error as e:
    print(e)