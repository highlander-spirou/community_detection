from time import sleep
print('Mannual sleeping, waiting for database setup ....')
sleep(30)


from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR

import json
from singleton import redis_1
from crawler.producer import check_redis_queue, get_params, get_pmid_list, update_params, task_publisher, check_existed_data
from scheduler.workflow import TaskManager

r = redis_1

def seed():
    if r.exists('eutil_params'):
        print('Eutil parameter exists, skip seeding ...')
    else:
        seed = {
            "term": "biomedical data science",
            "mindate": "1995/01",
            "maxdate": "1995/12",
            'retstart': 0,
        }
        r.hset('eutil_params', mapping=seed)


if __name__ == "__main__":
    from time import sleep
    from datetime import datetime


    BATCH_CRAWL_INTERVAL = 5 * 60 # in seconds 
    RETRY_INTERVAL = 1 * 60 # in seconds


    # Initialize the default argument
    seed()

    g = """
        check_redis_queue > get_params,
        get_params >> get_pmid_list,
        get_pmid_list >> check_existed_data,
        get_pmid_list >> update_params,
        check_existed_data >> task_publisher
    """

    man = TaskManager(graph=g, tasks=[check_redis_queue, get_params, get_pmid_list, update_params, task_publisher, check_existed_data])
    def runner():
        man.run_pipeline()

    schedule = BackgroundScheduler()
    schedule.add_job(runner, 'interval', seconds=BATCH_CRAWL_INTERVAL, id='run_pipeline', next_run_time=datetime.now())
    
    def error_hook(event):
        """
        The error hook temporary stop the daemon, retry the job for a shorter interval,
        then resume the job 
        """
        schedule.pause_job('run_pipeline')
        i = 0
        is_job_still_fail = True
        while i < 3 and is_job_still_fail:
            # Manual run the runner
            try:
                sleep(RETRY_INTERVAL)
                # sleep(5)
                print(f'Re run the job for {i+1} time')
                runner()
                is_job_still_fail = False
            except Exception:
                print("Job still fails, pending for rerun")
                i += 1
        else:
            if is_job_still_fail != False:
                print(f'Job failed, registering to error log ...')
                err_params = man.get_task_results('get_params')
                err_json = json.dumps(err_params)
                r.lpush('fail_arguments', err_json)
            else:
                print(f'Job successful running after {i} time(s)')
            schedule.resume_job('run_pipeline')


    schedule.add_listener(error_hook, EVENT_JOB_ERROR)
    schedule.start()

    i = 0
    while i < 10_000:
        sleep(60)
        print('60 seconds')
        i += 1

    else:
        schedule.shutdown()
    