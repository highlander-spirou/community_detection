import pickle
import os


def save_pickle(new_result):
    if os.path.exists('./results.pkl'):
        with open('./results.pkl', 'rb') as f:
            li = pickle.load(f)
            li.extend(new_result)
    else:
        li = new_result

    with open('./results.pkl', 'wb') as f1:
        pickle.dump(li, f1)
