import sqlite3

conn = sqlite3.connect("../kw_cache.db", timeout=10000)
cursor = conn.cursor()   
cursor.execute("DELETE FROM cache_tbl;")
conn.commit()
conn.close()


