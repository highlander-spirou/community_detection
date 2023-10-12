import asyncio
import sys
import psycopg
from hh.parser import get_pmid_info, get_pmid_list
from hh.resume import get_meta_data, update_meta_data
from hh.db import URI, write_log, pickle_utils


async def main():
    LIMIT = 10
    metadata = await get_meta_data()
    OFFSET = metadata['CHECKPOINT']
    current_page = 0
    async with await psycopg.AsyncConnection.connect(URI) as aconn:
        while current_page < 351:  # có 70k data, mỗi page lấy 200 data => có 350 page
            current_page += 1
            pmid_list = await get_pmid_list(LIMIT, OFFSET)
            results = []
            for i in pmid_list:
                try:
                    print(f'Đang xử lí {i}')
                    result = await get_pmid_info(i)
                    results.append((i, result))
                    await write_log(f'Fetch {i} success', 'success', aconn)
                    OFFSET += 1
                    await update_meta_data(OFFSET)
                except Exception:
                    await write_log(f'Fail to {i}', 'fail', aconn)
            pickle_utils.save_pickle(results)

if __name__ == '__main__':
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
