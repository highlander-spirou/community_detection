def get_pmid_list(**kwargs):
    return

def task_publisher(**kwargs):
    """
    This function enqueue ids crawled by `get_pmid_list` to the celery broker
    """
    pmids = get_pmid_list(**kwargs)
    
    # Publish the task via Celery
    ...

    return