import os
import aiofiles
import json

async def get_meta_data():
    metadata = './metadata.json'
    if os.path.exists(metadata) is False:
        return {'CHECKPOINT': 0}
    else:
        async with aiofiles.open(metadata, mode='r') as f:
            contents = await f.read()
    return json.loads(contents)


async def update_meta_data(pts):
    metadata = './metadata.json'
    async with aiofiles.open(metadata, 'w') as outfile:
        await outfile.write(json.dumps({"CHECKPOINT": pts}))