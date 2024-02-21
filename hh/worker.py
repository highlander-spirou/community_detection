class CrawlError(Exception):
    def __init__():
        pass

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

def get_pmid_info(pmid: int):
    """
    Crawl the pmid information
    """
    try:
        return
    except Exception:
        raise CrawlError()

def task_executor(pmid: int):
    """
    Pop the pmid from the queue and process it
    """
    try:
        result = get_pmid_info(pmid)
        save_to_mongo(result)
        ping_graph_scheduler(pmid)
    except CrawlError:
        save_failure(pmid)



        