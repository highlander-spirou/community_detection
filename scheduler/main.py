from apscheduler.schedulers.background import BackgroundScheduler
# import time
# from random import randint

def my_task():
  print("Task executed!")

scheduler = BackgroundScheduler()

def run_scheduler():
  scheduler.add_job(my_task, 'interval', seconds=2, id='1')  # Run every 10 seconds
  print("Scheduler started...")
  scheduler.start()

if __name__ == "__main__":
  from time import sleep

  graph = """
check_redis_queue > get_params,
get_params > get_pmid_list,
get_pmid_list > task_publisher,
get_pmid_list > update_params
"""

  run_scheduler()
  i = 0
  while i <= 7:
    sleep(4)
    print("Starting scheduler at main thread")
    i += 1
  else:
    scheduler.remove_job('1')

