"""
Define the workflow
"""
from functools import partial
from networkx import topological_sort, DiGraph
from random import randint
from redis import Redis
from abc import ABC, abstractmethod
from typing import Any, Literal



# r = Redis(host='localhost', port=6379, db=1)

class AbstractTask(ABC):
    """
    Abstract task, receive a task name, task func, task default parameters 
    
    Compile to partial function
    """
    def __init__(self, task_func, task_name: str | None, **default_params):
        self.task_name = task_name
        if len(default_params.keys()) == 0:
            self.partial_fn = partial(task_func)
        else:
            self.partial_fn = partial(task_func, **default_params)

    @abstractmethod
    def __call__(self):
        """
        Run the `Task` by make it callable
        """
        pass

class AbstractLog(ABC):

    @abstractmethod
    def init_store(self, *args, **kwargs):
        """
        Register task with default state ("PENDING")
        """
        pass

    @abstractmethod
    def check_task_status(self, key) -> Literal["PENDING", "RUNNING", "SUCCESS", "FAIL"]:
        """
        Check for the status of a task
        """
        pass

    @abstractmethod
    def update_task_status(self, key, value:Literal["PENDING", "RUNNING", "SUCCESS", "FAIL"]):
        """
        Update task with one of 4 statuses:

        - PENDING: Default state, task has been registered but has not been ran
        - RUNNING: Task is running
        - SUCCESS: Task executed
        - FAIL: Task fail to execute
        """
        pass

    @abstractmethod
    def reset_log(self):
        """
        Restore all tasks to `PENDING` if all tasks success
        """
        pass

class RedisLog(AbstractLog):
    def __init__(self, store_name, host='localhost', port=6379, db=1):
        self.store_name = store_name
        self.redis = Redis(host=host, port=port, db=db)

    def init_store(self, init_dict):
        if not bool(self.redis.exists(self.store_name)):
            self.redis.hset(
                self.store_name,
                mapping=init_dict
            )
        
    def update_task_status(self, key, value):
        self.redis.hset(self.store_name, key, value)

    def check_task_status(self, key):
        status = self.redis.hget(self.store_name, key)
        return status.decode('utf-8')
    
    def reset_log(self):
        pass

def graph_str_process(g):
    dependencies = g.strip().split(",")
    dep_nodes = []
    for dep in dependencies:
        dep_stripped = dep.strip()
        nodes = [i.strip() for i in dep_stripped.split(">")]
        dep_nodes.append(nodes)

    graph = DiGraph()
    for i in dep_nodes:
        graph.add_node(i[0])
        graph.add_node(i[1])
        graph.add_edge(i[0], i[1])
    return graph

class Manager:
    def __init__(self, tasks:list, graph:str, task_cls:AbstractTask, log_cls:AbstractLog):
        """
        @tasks: list of functions or tuple of func with default kwargs
            - [func1, (func2, {'a': 1, 'b', 2})]

        @graph: Inspired by mermaid.js task flow
            ```
            task_1 > task_2
            task_2 > task_3
            ```
        @task_cls: Dependency injection of AbstractTask, a constructor to create `Task`
        @log_cls: Dependency injection of AbstractLog
        """
        self.graph = graph
        self.log_cls = log_cls
        self.Task_cls = task_cls

        # Post init
        self.register_task(tasks)
        self.construct_execution()
        self.create_log()


    def register_task(self, tasks):
        self.tasks: dict[str, callable] = {}
        for task in tasks:
            if type(task) == tuple:
                self.tasks[task[0].__name__] = self.Task_cls(task[0], task[0].__name__, task[1])
            else:
                self.tasks[task.__name__] = self.Task_cls(task, task.__name__)

    def construct_execution(self):
        g = graph_str_process(self.graph)
        self.order = [i for i in topological_sort(g)]

    def create_log(self):
        init_dict = {i: "PENDING" for i in self.order} 
        self.log_cls.init_store(init_dict)

    def run_tasks(self):
        for task in self.order:
            task_status = self.log_cls.check_task_status(task)
            if task_status in ('PENDING', 'FAIL'):
                try:
                    self.log_cls.update_task_status(task, 'RUNNING')
                    self.tasks[task]()
                    self.log_cls.update_task_status(task, 'SUCCESS')
                except Exception:
                    self.log_cls.update_task_status(task, 'FAIL')
                    raise Exception(f"Fail running task {task}")
            else:
                print(f"Task {task} has run, skipping ...")
            

    #     self.create_log()
    #     self.construct_execution()
    #     self.register_task(tasks)


    

    # def register_task(self, tasks):
    #     self.tasks = {}
    #     for task in tasks:
    #         if type(task) == tuple:
    #             self.tasks[task[0].__name__] = Task(task[0], task[0].__name__, self.log_location, task[1])
    #         else:
    #             self.tasks[task.__name__] = Task(task, task.__name__, self.log_location)

    # def __call__(self):
    #     for i in self.order:
    #         # if is_task_success(self.log_location, i):
    #         #     print(f'Task {i} triggered, skipping to next task ...')
    #         # else:
    #         #     print(f'Task {i} fail')
    #         print(i, is_task_success(self.log_location, i))
    #         self.tasks[i]()
    #     self.create_log()


# res1 = r.hset(
#     "bike:1",
#     mapping={
#         "model": "Deimos",
#         "brand": "Ergonom",
#         "type": "Enduro bikes",
#         "price": 4972,
#     },
# )
# print(res1)
# # >>> 4

# res2 = r.hget("bike:1", "model")
# print(res2)
# # >>> 'Deimos'

# res3 = r.hget("bike:1", "price")
# print(res3)
# # >>> '4972'

# res4 = r.hgetall("bike:1")
# print(res4)
# # >>> {'model': 'Deimos', 'brand': 'Ergonom', 'type': 'Enduro bikes', 'price': '4972'}

# r.hset("bike:1:stats", "type", 'con cặc')


# res5 = r.hmget("bike:1", ["model", "price"])
# print(res5)
# # >>> ['Deimos', '4972']


# print(res15)
# # >>> 1
# res16 = r.hget("bike:1:stats", "rides")
# print(res16)
# # >>> 3
# res17 = r.hmget("bike:1:stats", ["crashes", "owners"])
# print(res17)
# # >>> ['1', '1']



# def write_json(log_location, task, state):
#     with open(log_location, 'r') as f:
#         prev_state = json.load(f)

#     prev_state[task] = state

#     with open(log_location, 'w', encoding='utf8') as f:
#         json.dump(prev_state, f, ensure_ascii=False)

# def is_task_success(log_location, task):
#     with open(log_location, 'r') as f:
#         prev_state = json.load(f)

#     state = prev_state.get(task)
#     # print(f"Tracing task {task}")
#     # print('prev_state', prev_state)
#     # print("state là", state)
#     # print("1st cond", state is not None)
#     # print("2nd cond", state == 'SUCCESS')
#     if state is not None and state == 'SUCCESS':
#         return True 
#     return False

    

#     graph = DiGraph()
#     for i in dep_nodes:
#         graph.add_node(i[0])
#         graph.add_node(i[1])
#         graph.add_edge(i[0], i[1])

#     return graph

if __name__ == "__main__":
    from time import sleep

    class Task(AbstractTask):
        """
        Extend function of AbstractTask

        This `Task` class run the `self.partial_fn` using `callable` method
        """
        def __init__(self, task_func, task_name: str | None, **default_params):
            super().__init__(task_func, task_name, **default_params)

        def __call__(self, *args, **kwargs):
            self.partial_fn(*args, **kwargs)



    def tinh_di():
        print("1 + 1 bằng mấy")
        sleep(10)

    def chui():
        rand = randint(0, 1)
        if rand != 0:
            print("Đụ mẹ mày tao hỏi lại, 1 + 1 bằng mấy")
            sleep(2)
        else:
            raise Exception()

    def kq():
        rand = randint(0, 1)
        if rand != 0:
            sleep(5)
            print("Dạ 2")
        else:
            raise Exception()

    def chui_nua():
        sleep(5)

        print("Đụ mẹ mày sao tao hỏi 2 lần mày mới trả lời")


    graph = """
        tinh_di > chui,
        chui > kq,
        kq > chui_nua
"""

    log_cls = RedisLog("hello_world")

    man = Manager(tasks=[tinh_di, chui_nua, chui, kq], graph=graph, task_cls=Task, log_cls=log_cls)
    man.run_tasks()

# if __name__ == "__main__":
#     cc = Manager(tasks=[tinh_di, chui, kq, chui_nua], graph=graph, log_location="dumemay.json")
#     try:
#         cc()
#     except Exception:
#         print("\n\n\n-------------------\n\n\n")
#         sleep(2)
#         cc()