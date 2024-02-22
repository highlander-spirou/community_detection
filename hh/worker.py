# This script must be run before spawn worker (create database if not exist)

from celery import Celery
import httpx
import bs4
from lib.sql import get_cache_value, save_cache, save_pmid_err

worker = Celery('tasks', broker='redis://localhost:6379/0')

class CrawlError(Exception):
    def __init__(self):
        pass


def fetch_html(url):
    """
    This function use to parse HTML or XML response to bs4 src.
    """
    with httpx.Client() as client:
        response = client.get(url)
    return response.text


def get_text_or_none(element):
    """
    Helper function to return None instead of get_text if the element is None
    """
    if element is not None:
        return element.get_text()
    else:
        return None


def get_pmid_info(pmid: int) -> str:
    """
    Crawl the pmid information

    Validate the `Title` field, if None, raise error
    """
    prev_result = get_cache_value(pmid, "cache_pmids")
    if prev_result is not None:
        print(f"Get pmid {pmid} from cache")
        return prev_result

    try:
        root = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&retmode=xml"
        src = fetch_html(root)
        src_bs4 = bs4.BeautifulSoup(src, features="xml")
        title = get_text_or_none(src_bs4.select_one("ArticleTitle"))
        if title is None:
            raise CrawlError()
        else:
            save_cache(pmid, src, "cache_pmids")
            return src
    except Exception:
        raise CrawlError()


@worker.task(name="get_pmid_info")
def task_executor(pmid: int):
    """
    Pop the pmid from the queue and process it
    """
    print("Processing pmid:", pmid)
    try:
        get_pmid_info(pmid)
    except CrawlError:
        save_pmid_err(pmid)

    # try:
    #     result = get_pmid_info(pmid)
    #     save_to_mongo(result)
    #     ping_graph_scheduler(pmid)
    # except CrawlError:
    #     save_failure(pmid)


def ping_graph_scheduler(pmid):
    """
    Call the graph scheduler for successfull query 
    """
    return



if __name__ == "__main__":
    get_pmid_info(1016300000)