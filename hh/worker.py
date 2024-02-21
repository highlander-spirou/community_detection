# This script must be run before spawn worker (create database if not exist)

from celery import Celery
import httpx
import sqlite3
from lib.db import connect_to_db, ensure_tbl

worker = Celery('tasks', broker='redis://localhost:6379/0')

# def ensure_tbl():
#     conn = sqlite3.connect("kw_cache.db", timeout=10000)
#     cursor = conn.cursor()   
#     cursor.execute("CREATE TABLE IF NOT EXISTS cache_pmid (id INTEGER PRIMARY KEY AUTOINCREMENT, pmid INT, cached_value TEXT);")
#     conn.commit()
#     conn.close()


class CrawlError(Exception):
    def __init__():
        pass


def fetch_html(url):
    """
    This function use to parse HTML or XML response to bs4 src.
    """
    with httpx.Client() as client:
        response = client.get(url)

    return bs4.BeautifulSoup(response.text, features="xml")


def get_pmid_info(pmid: int):
    """
    Crawl the pmid information
    """
    try:
        root = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&retmode=xml"
    except Exception:
        raise CrawlError()


@worker.task(name="get_pmid_info")
def task_executor(pmid: int):
    """
    Pop the pmid from the queue and process it
    """
    print("Processing pmid:", pmid)
    # try:
    #     result = get_pmid_info(pmid)
    #     save_to_mongo(result)
    #     ping_graph_scheduler(pmid)
    # except CrawlError:
    #     save_failure(pmid)


def save_to_mongo(pmid_info):
    """
    Save the pmid_info `json` to MongoDB records
    """
    return

def ping_graph_scheduler(pmid):
    """
    Call the graph scheduler for successfull query 
    """
    return

def save_failure(pmid):
    """
    Save the fail ids to redis to proceed re-crawl in the future
    """


if __name__ == "__main__":
    ensure_tbl("cache_pmid")