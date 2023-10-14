from apscheduler.schedulers.asyncio import AsyncIOScheduler
from crawl_scripts.daily_crawl import main as daily_crawl 


if __name__ == '__main__':
    import asyncio
    scheduler = AsyncIOScheduler()
    scheduler.add_job(daily_crawl, "interval", seconds=10)
    scheduler.start()
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        exit()
