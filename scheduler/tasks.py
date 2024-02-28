from redis import Redis
# from hh.
def check_redis_queue():
    print('hello 1')


def get_pmid_list():
    r = Redis(db=1)
    params = r.hgetall("crawl_arguments")
    print(params)
    pass

def task_publisher():
    print('hello 4')
    pass

def update_params():
    print('hello 5')
    pass