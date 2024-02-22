import contextlib
from mysql.connector import connect, Error
from os import environ

@contextlib.contextmanager
def connect_to_db():
    """
    Context manager for connecting to database.

    Yields the connection object for use within the context.
    """
    username = environ.get("MYSQL_USER")
    pwd = environ.get("MYSQL_PWD")
    db = environ.get("MYSQL_DB")
    host = environ.get("MYSQL_HOST")

    if username is None or pwd is None or db is None:
        # if username is None or pwd is None:
        raise Exception("Missing environment variable")

    conn = connect(host="localhost",user=username,password=pwd, database=db)
    try:
        yield conn
    finally:
        conn.close()



def get_cache_value(cache_key, tbl_name) -> str | None:
    """
    Check the cache, if exist, return the result
    """


    query_templates = {
        "cache_eutils": ("SELECT cached_value FROM cache_eutils " 
        "WHERE base64_data = %s"),
    }

    with connect_to_db() as conn:
        cursor = conn.cursor()
        cursor.execute(query_templates[tbl_name], (cache_key, ))
        result = cursor.fetchone()
    
    if result is not None:
        return result[0]
    else:
        return None 


def save_cache(cache_key, cache_value, tbl_name):
    """
    Save the crawled value to cache
    """

    query_templates = {
        "cache_eutils": "INSERT INTO cache_eutils (base64_data, cached_value) VALUES (%s, %s)",
    }

    with connect_to_db() as conn:
        c = conn.cursor()
        c.execute(query_templates[tbl_name], (cache_key, cache_value))
        conn.commit()

def save_pmid_err(pmid):
    with connect_to_db() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO pmid_errors (pmid, num_retry) VALUES (%s, %s)", (pmid, 0))
        conn.commit()