from main import task_publisher

def check_redis_state():
    """
    Check the state of the queue if it is Empty
    """
    return False

class Scheduler:
    def __init__(self, interval):
        self.interval = interval

    def check_time():
        if check_redis_state():
            # Proceed
            task_publisher(**kwargs)
        else:
            # Re-schedule
            ...

