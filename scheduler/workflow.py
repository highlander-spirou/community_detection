from networkx import DiGraph, topological_sort
from abc import ABC
from redis import Redis
import json
import functools
from os import environ


def RedisDecode(func):
    """
    This function decode the dependency argument fetch from redis to python string

    When json string is stored inside of Redis, it preserve the `""` sign, makes the string non-native

    This function removes the need to call json.loads() on every function in a TaskManager node
    """
    @functools.wraps(func)
    def wrapper(json_dependency):
        dependency = json.loads(json_dependency)
        return func(dependency)
    return wrapper



class TaskError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def get_node_dependent_node(graph, node_name):
    preds_depdends = []
    predecessors = list(graph.predecessors(node_name))
    for predecessor in predecessors:
        edge_data = graph.get_edge_data(predecessor, node_name)
        if edge_data['pass_kw']:
            preds_depdends.append(predecessor)
    return preds_depdends


class BaseTask(ABC):
    def __init__(self, func, func_name:str|None=None, *args) -> None:
        self.func = func
        self.func_name = func_name
        self.defaults = args


class RedisLog:
    def __init__(self, host='localhost', port=6379, db=1) -> None:
        self.redis = Redis(host=host, port=port, db=db)

    def add_string(self, store_name, key:str, value:str):
        self.redis.hset(store_name, key, value)

    def add_dict(self, store_name, mapping):
        self.redis.hset(store_name, mapping=mapping)

    def get_value(self, store_name, key):
        value = self.redis.hget(store_name, key)
        if value is not None:
            return value.decode('utf-8')
        
    


class TaskManager:
    def __init__(self, graph:str, tasks, db=1) -> None:
        self.graph = graph
        self.tasks = tasks
        redis_host = environ.get('REDIS_HOST')
        if redis_host is None:
            raise Exception("Environment variable REDIS_HOST not found")
        self.store = RedisLog(host=redis_host, db=db)
    
        # Post init
        self.construct_graph()
        self.register_tasks()


    def construct_graph(self):
        graph = DiGraph()

        rows = self.graph.strip().split(',')
        rows = [i.strip() for i in rows]

        for row in rows:
            elem = row.split(' ')
            prev_node = elem[0]
            next_node = elem[2]
            graph.add_node(prev_node)
            graph.add_node(next_node)

            if elem[1] == '>>':
                graph.add_edge(prev_node, next_node, pass_kw=True)
            else:
                graph.add_edge(prev_node, next_node, pass_kw=False)

        self.digraph = graph
        self.order = [i for i in topological_sort(graph)]
        self.create_log()

    def create_log(self):
        init_dict = {i: "PENDING" for i in self.order}
        self.store.add_dict('status', mapping=init_dict)



    def register_tasks(self):
        self.task_registered = {}
        for i in self.tasks:
            self.task_registered[i.__name__] = i

    
    def get_task_results(self, task_name):
        return self.store.get_value("graph_results", task_name)


    def run_pipeline(self):
        for i in self.order:
            task_status = self.store.get_value('status', i)
            if task_status in ('PENDING', 'FAIL'):
                self.store.add_dict('status', {i: 'RUNNING'})
                dep = get_node_dependent_node(self.digraph, i)
                if len(dep) > 0:
                    for j in dep:
                        arg = self.store.get_value("graph_results", j)
                        if arg == 'invalid' or arg == None:
                            raise Exception("The predcessor function is not JSON serializable")
                        try:
                            result = self.task_registered[i](arg)
                            self.store.add_dict('status', {i: 'SUCCESS'})
                        except:
                            self.store.add_dict('status', {i: 'FAIL'})
                            raise TaskError()
                else:
                    try:
                        result = self.task_registered[i]()
                        self.store.add_dict('status', {i: 'SUCCESS'})
                    except:
                        self.store.add_dict('status', {i: 'FAIL'})
                        raise TaskError()

                try:
                    json_result = json.dumps(result, ensure_ascii=False)
                    self.store.add_dict("graph_results", mapping={str(i): json_result})
                except:
                    self.store.add_dict("graph_results", mapping={str(i): 'invalid'})

            else:
                print(f'Task {i} already ran, pass ...')
                pass

        self.create_log()