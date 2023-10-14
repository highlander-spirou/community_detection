import json
import pickle
from dask.distributed import Client
from hh.parser import get_pmid_info
from hh.db import create_table, write_data


async def crawl_url(pmid):
    print(f'Đang xử lí {pmid}')
    result = await get_pmid_info(pmid)
    json_result = json.dumps(result)
    return (pmid, json_result)


async def main():
    await create_table()
    with open('./pmids/batch_10.pkl', 'rb') as f:
        li = pickle.load(f)

    client = await Client(asynchronous=True)
    futures = []
    for i in set(li):
        future = client.submit(crawl_url, i)
        futures.append(future)
    results = await client.gather(futures, errors='skip')
    await write_data(results)
    await client.close()

    

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())