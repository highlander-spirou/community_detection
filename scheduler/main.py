from apscheduler.schedulers.background import BackgroundScheduler
import time

def my_task():
  print("Task executed!")

def main():
  scheduler = BackgroundScheduler()
  scheduler.add_job(my_task, 'interval', seconds=2)  # Run every 10 seconds
  print("Scheduler started...")
  scheduler.start()

if __name__ == "__main__":
  main()
