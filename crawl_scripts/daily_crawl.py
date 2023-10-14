from hh.parser import get_pmid_list, get_pmid_info
from hh.db import find_pmid, write_data
import json

LIMIT = 5
OFFSET = 0


async def main():
    pmids = await get_pmid_list(LIMIT, OFFSET)
    data = await find_pmid(pmids)
    if data[0] - LIMIT > 0:
        # Do PUBMED API xếp theo data publish, ta chỉ cần lấy từ 0 -> difference
        li = []
        for i in pmids[0:(data[0] - LIMIT)]:
            result = await get_pmid_info(i)
            jons_result = json.dumps(result)
            li.append((i, jons_result))

        await write_data(li)
        print(f'New data fetch: {pmids[0:(data[0] - LIMIT)].join(", ")}')
    else:
        print('No new data')



if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
