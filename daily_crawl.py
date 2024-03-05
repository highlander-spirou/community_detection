from time import sleep
print('Mannual sleeping, waiting for database setup ....')
sleep(30)

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_ERROR

from singleton import redis_1
from crawler.producer import get_params_daily, get_pmid_list, task_publisher, check_existed_data
from scheduler.workflow import TaskManager


if __name__ == "__main__":
    from datetime import datetime


    r = redis_1

    BATCH_CRAWL_INTERVAL = 5 * 60 # in seconds 
    RETRY_INTERVAL = 8 * 60 * 60

    g = """
        get_params_daily >> get_pmid_list,
        get_pmid_list >> check_existed_data,
        check_existed_data >> task_publisher
    """

    man = TaskManager(graph=g, tasks=[get_params_daily, get_pmid_list, task_publisher, check_existed_data], db=2)
    def runner():
        man.run_pipeline()

    schedule = BlockingScheduler()
    schedule.add_job(runner, 'cron', id='daily_crawl', hour=6, minute=0, next_run_time=datetime.now())
    
    def error_hook(event):
        """
        The error hook temporary stop the daemon, retry the job for a shorter interval,
        then resume the job 
        """
        schedule.pause_job('daily_crawl')
        try:
            print('Run daily crawler failed, waiting for retry ...')
            sleep(RETRY_INTERVAL) # sleep for 8 hours to avoid high traffic
            runner()
            print('Retry success')
        except Exception:
            print('Run daily crawler failed, writing to error log!')
            r.lpush('fail_daily_crawl', f"{datetime.now():%Y/%m/%d %H:%M:%S}")
        finally:
            schedule.resume_job('daily_crawl')

    schedule.add_listener(error_hook, EVENT_JOB_ERROR)
    
    
    print('Starting scheduler ....')

    schedule.start()