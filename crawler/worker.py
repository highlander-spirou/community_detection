from celery import Celery
import bs4
from crawler.lib.sql import save_cache, save_pmid_err
from crawler.lib.crawl import fetch_html, get_text_or_none
from os import environ


redis_host = environ.get("REDIS_HOST")
if redis_host is None:
    raise Exception("Environment variable REDIS_HOST not found")

worker = Celery('tasks', broker=f'redis://{redis_host}:6379/0')

class CrawlError(Exception):
    def __init__(self):
        pass


def get_pmid_info(pmid: int) -> str:
    """
    Crawl the pmid information

    Validate the `Title` field, if None, raise error
    """
    try:
        root = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&retmode=xml"
        src = fetch_html(root)
        src_bs4 = bs4.BeautifulSoup(src, features="xml")
        title = get_text_or_none(src_bs4.select_one("ArticleTitle"))
        if title is None:
            raise CrawlError()
        else:
            save_cache(pmid, src, "cache_pmids")
    except Exception:
        raise CrawlError()


@worker.task(name="get_pmid_info", rate_limit='3/s')
def task_executor(pmid: int):
    """
    Pop the pmid from the queue and process it
    """
    print("Processing pmid:", pmid)
    try:
        get_pmid_info(pmid)
    except CrawlError:
        save_pmid_err(pmid)