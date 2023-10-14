import aiosqlite
import re
import json
from dask.distributed import Client
from hh.parser import get_pmid_info
from hh.db import write_data

async def crawl_url(pmid):
    print(f'Đang xử lí {pmid}')
    result = await get_pmid_info(pmid, False)
    json_result = json.dumps(result)
    return (pmid, json_result)


async def main():
    async with aiosqlite.connect('./results.db') as db:
        cursor = await db.execute('select log_message from logs')
        result = await cursor.fetchall()

    def extract_numbers(input_string): return re.findall(
        r'\d+', input_string)[0]
    error_ids = [extract_numbers(i[0]) for i in result]

    client = await Client(asynchronous=True)
    futures = []
    for i in set(error_ids):
        future = client.submit(crawl_url, i)
        futures.append(future)
    results = await client.gather(futures, errors='skip')
    await write_data(results)
    await client.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
