from apscheduler.schedulers.background import BackgroundScheduler
from scheduler.workflow import Manager, RedisLog, Task
from scheduler.tasks import *
from redis import Redis


def setup():
  rd = Redis(db=1)
  if not bool(rd.exists("crawl_arguments")):
    params = {
      "db": "pubmed",
      "term": "biomedical data science",
      "mindate": "1995/01",
      "maxdate": "1995/12",
      "retmax": 10,
      'retstart': 0,
    }
    rd.hset("crawl_arguments", mapping=params)


if __name__ == "__main__":
  from time import sleep

  setup()

  task_dependencies = """
    check_redis_queue > get_pmid_list,
    get_pmid_list > task_publisher,
    get_pmid_list > update_params
  """
  log_cls = RedisLog("APScheduler")

  initial_crawl_args = {''}

  man = Manager(tasks=[check_redis_queue, get_pmid_list, task_publisher, update_params], graph=task_dependencies, task_cls=Task, log_cls=log_cls)

  scheduler = BackgroundScheduler()

  def runner():
    man.run_tasks()

    
  scheduler.add_job(runner, 'interval', seconds=2, id='1')  # Run every 10 seconds
  print("Scheduler started...")
  scheduler.start()
  i = 0
  while i <= 7:
    sleep(4)
    print("Starting scheduler at main thread")
    i += 1
  else:
    scheduler.shutdown()

