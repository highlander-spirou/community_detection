import json
from celery import Celery
from crawler.lib import *
from typing import TypedDict
from singleton import redis_0, redis_1
from scheduler import TaskError, RedisDecode
from datetime import datetime, timedelta
from os import environ

RETMAX = 200


class EutilParams(TypedDict):
    term: str
    mindate: str
    maxdate: str
    retstart: int


redis_host = environ.get('REDIS_HOST')

producer = Celery('tasks', broker=f'redis://{redis_host}:6379/0')

celery_store = redis_0
crawl_store = redis_1

def increase_date_str(original_date):
    year, month = original_date.split('/')
    year = str(int(year) + 1)
    new_date = year + '/' + month
    return new_date

def check_redis_queue():
    if celery_store.llen('celery') != 0:
        raise TaskError()


def get_params() -> EutilParams:
    json_b = crawl_store.hgetall("eutil_params")
    eutil_params = {key.decode('utf-8'): val.decode('utf-8') for key, val in json_b.items()}
    eutil_params['retstart'] = int(eutil_params['retstart'])
    return eutil_params


def get_params_daily() -> EutilParams:
    today = f"{datetime.now():%Y/%m/%d}"
    # Calculate yesterday's date by subtracting one day
    yesterday = datetime.now() - timedelta(days = 1)

    # Format yesterday's date as string
    yesterday_str = yesterday.strftime("%Y/%m/%d")


    return {"term": "biomedical data science",
         "mindate": yesterday_str,
         "maxdate": today,
         "retstart": 0
        }


def get_pmid_list(kwargs_str:str) -> str:
    """
    Save the Crawl result to MySQL, return the hash `kwargs`
    """

    kwargs_base64 = hash_dict(kwargs_str)

    prev_result = get_cache_value(kwargs_base64, "cache_eutils")

    if prev_result is None:
        print("Cache not found, retrieving new data")

        kwargs:EutilParams = json.loads(kwargs_str)

        # Crawl the data
        url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        default_keys = {
            "sort": "pub_date",
            'retmode': 'json',
            'db': 'pubmed',
            'retmax': RETMAX
        }
        params = kwargs | default_keys
        results = fetch_json(url, params)
        ret_count = results['esearchresult']['retmax']
        ids = results['esearchresult']['idlist']
        result_str = json.dumps({'count': int(ret_count), 'ids': ids})
        save_cache(kwargs_base64, result_str, "cache_eutils")
    else:
        print('Retrieve list from cache')
    return kwargs_base64


@RedisDecode
def check_existed_data(hashed_params:str):
    prev_result = get_cache_value(hashed_params, "cache_eutils")
    json_decoded = json.loads(prev_result)

    pmids = json_decoded['ids']
    if len(pmids) == 0:
        print('No new pmid for today')
        return []
    
    existed_ids = set()

    format_strings = ','.join(['%s'] * len(pmids))
    with connect_to_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT pmid FROM cache_pmids WHERE pmid IN (%s)" % format_strings,
                        tuple(pmids))

        results = cursor.fetchall()
        for i in results:
            existed_ids.add(str(i[0]))

    non_existed = list(filter(lambda x: x not in existed_ids, pmids))

    return non_existed


@RedisDecode
def task_publisher(pmids:list):
    """
    This function enqueue ids crawled by `get_pmid_list` to the celery broker
    """
    # Publish the task via Celery
    if len(pmids) == 0:
        print('Empty pmid list, skipping ...')
        return
    for i in pmids:
        producer.send_task('get_pmid_info', kwargs={"pmid": i})
    


@RedisDecode
def update_params(hashed_params:str):
    prev_result = get_cache_value(hashed_params, "cache_eutils")
    json_decoded = json.loads(prev_result)

    ret_count = json_decoded['count']
    
    if ret_count < RETMAX: # if ret_count < RETMAX means we reach to the end of the batch
        old_mindate = crawl_store.hget("eutil_params", "mindate").decode('utf-8')
        old_maxdate = crawl_store.hget("eutil_params", "maxdate").decode('utf-8')

        new_mindate = increase_date_str(old_mindate)
        new_maxdate = increase_date_str(old_maxdate)
        to_update = {"mindate": new_mindate, "maxdate": new_maxdate, "retstart": 0}

    else: # We have not reached to the end of the batch
        old_retstart = crawl_store.hget("eutil_params", "retstart").decode('utf-8')
        to_update = {"retstart": int(old_retstart) + RETMAX}


    crawl_store.hset("eutil_params", mapping=to_update)