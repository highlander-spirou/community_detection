"""
Define the workflow
"""
from functools import partial
from networkx import topological_sort, DiGraph
from redis import Redis
from abc import ABC, abstractmethod
from typing import Literal

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
            
class Task(AbstractTask):
        """
        Extend function of AbstractTask

        This `Task` class run the `self.partial_fn` using `callable` method
        """
        def __init__(self, task_func, task_name: str | None, **default_params):
            super().__init__(task_func, task_name, **default_params)

        def __call__(self, *args, **kwargs):
            self.partial_fn(*args, **kwargs)


#     graph = """
#         tinh_di > chui,
#         chui > kq,
#         kq > chui_nua
# """

#     log_cls = RedisLog("hello_world")

#     man = Manager(tasks=[tinh_di, chui_nua, chui, kq], graph=graph, task_cls=Task, log_cls=log_cls)
#     man.run_tasks()