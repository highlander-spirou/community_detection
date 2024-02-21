import contextlib
import sqlite3


@contextlib.contextmanager
def connect_to_db():
    """
    Context manager for connecting to an SQLite3 database.

    Yields the connection object for use within the context.
    """
    conn = sqlite3.connect("kw_cache.db", timeout=10000)
    try:
        yield conn
    finally:
        conn.close()


def ensure_tbl(tbl_name):
    conn = sqlite3.connect("kw_cache.db", timeout=10000)
    cursor = conn.cursor()   
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {tbl_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, base64_data TEXT, cached_value TEXT);")
    conn.commit()
    conn.close()


def check_cache(cache_str, tbl_name) -> str | None:
    """
    Check the cache, if exist, return the result
    """
    with connect_to_db() as conn:
        c = conn.cursor()
        c.execute("SELECT cached_value FROM cache_tbl WHERE base64_data = ?", (cache_str,))
        result = c.fetchone()
    
    if result:
        return result[0]
    return None