import json
import base64
import sqlite3
import httpx
from celery import Celery
# from lib.sql import connect_to_db, ensure_tbl, save_cache_table, check_cache
from lib.sql import get_cache_value, save_cache

producer = Celery('tasks', broker='redis://localhost:6379/0')

def fetch_json(url, params):
    with httpx.Client() as client:
        response = client.get(url, params=params)
        return response.json()

def get_pmid_list(**kwargs):
    
    kwargs_str = json.dumps(kwargs)
    kwargs_base64 = base64.b64encode(kwargs_str.encode('utf-8')).decode('utf-8') 
    prev_result = get_cache_value(kwargs_base64, "cache_eutils")

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
        save_cache(kwargs_base64, result_str, "cache_eutils")
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
    params = {
        "db": "pubmed",
        "term": "biomedical data science",
        "mindate": "1995/11",
        "maxdate": "1996/01",
        "retmax": 10,
        'retstart': 0,
    }
    task_publisher(**params)