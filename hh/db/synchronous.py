import sqlite3

def write_data_sync(data):
    with sqlite3.connect('./results.db') as db:
        db.executemany("INSERT INTO results (pmid, json_content) VALUES (?,?)", data)
        db.commit()