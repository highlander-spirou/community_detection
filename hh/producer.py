import json
import base64
import sqlite3
import httpx
from celery import Celery
from lib.db import connect_to_db, ensure_tbl


producer = Celery('tasks', broker='redis://localhost:6379/0')

# def ensure_tbl():
#     conn = sqlite3.connect("kw_cache.db", timeout=10000)
#     cursor = conn.cursor()   
#     cursor.execute("CREATE TABLE IF NOT EXISTS cache_tbl (id INTEGER PRIMARY KEY AUTOINCREMENT, base64_data TEXT, cached_value TEXT);")
#     conn.commit()
#     conn.close()


def check_cache(cache_str) -> str | None:
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

def save_cache_data(base_64_data, cached_value):
    with connect_to_db() as conn:
        c = conn.cursor()
        c.execute("INSERT INTO cache_tbl (base64_data, cached_value) VALUES (?, ?)", (base_64_data, cached_value))
        conn.commit()
    

def fetch_json(url, params):
    with httpx.Client() as client:
        response = client.get(url, params=params)
        return response.json()

def get_pmid_list(**kwargs):
    
    kwargs_str = json.dumps(kwargs)
    kwargs_base64 = base64.b64encode(kwargs_str.encode('utf-8')).decode('utf-8') 

    prev_result = check_cache(kwargs_base64)
    if prev_result is None:
        # Crawl the data
        
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        default_keys = {
            "sort": "pub_date",
            'retmode': 'json',
        }
        params = kwargs | default_keys
        results = fetch_json(url, params)
        result_str = json.dumps(results)
        save_cache_data(kwargs_base64, result_str)
        return results

    else:
        return json.loads(prev_result)


def task_publisher(**kwargs):
    """
    This function enqueue ids crawled by `get_pmid_list` to the celery broker
    """
    # Crawl the data
    crawl_json = get_pmid_list(**kwargs)
    pmids = crawl_json['esearchresult']['idlist']

    # Publish the task via Celery
    for i in pmids:
        producer.send_task('get_pmid_info', kwargs={"pmid": i})
    return


if __name__ == "__main__":
    ensure_tbl("cache_tbl")
    params = {
        "db": "pubmed",
        "term": "biomedical data science",
        "mindate": "1995/11",
        "maxdate": "1996/01",
        "retmax": 10,
        'retstart': 0,
    }
    task_publisher(**params)