import multiprocessing
import sqlite3
import pandas as pd
import re
import json
from hh.parser.synchronous import get_pmid_info_sync
from hh.db.synchronous import write_data_sync


def get_errors():
    with sqlite3.connect('./results.db') as db:
        logs = pd.read_sql("select log_message from logs", db)
        results = pd.read_sql("select pmid from results", db)

    # Xử lí logs
    logs.drop_duplicates(['log_message'], inplace=True)
    logs['error_id'] = logs['log_message'].apply(
        lambda input_string: re.findall(r'\d+', input_string)[0])

    merged = results.merge(logs, left_on='pmid',
                           right_on='error_id', how='outer', indicator=True)
    anti_join = merged[(merged._merge == 'right_only')].drop('_merge', axis=1)
    error_ids = anti_join['error_id'].to_list()
    return error_ids


def main(task_id):
    print(f'Đang xử lí {task_id}')
    try:
        result = get_pmid_info_sync(task_id)
        json_content = json.dumps(result)
        return (task_id, json_content)
    except Exception as e:
        return None


if __name__ == "__main__":
    tasks = get_errors()[130:]

    success = multiprocessing.Manager().list()
    failure = multiprocessing.Manager().list()
    num_cores = multiprocessing.cpu_count()
    with multiprocessing.Pool(processes=num_cores) as pool:
        results = pool.map(main, tasks)

    for task_id, result in zip(tasks, results):
        if result is not None:
            success.append(result)
        else:
            failure.append(task_id)

    write_data_sync(list(success))
    with open('./logs/error_log_1.txt', 'w+') as f:
        for i in list(failure):
            f.write("%s\n" % i)
