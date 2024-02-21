import queue 
 
# Initialize LIFO Queue
LIFOq = queue.LifoQueue(maxsize=10_000)

class CommunityDetection:
    def __init__(self, last_pmid):
        self.last_pmid = last_pmid

    def convert_mongo_to_neo4j(self):
        ...

    def run_community_detection(self):
        ...

    def __call__(self):
        self.convert_mongo_to_neo4j()
        self.run_community_detection()


def put_pmid(pmid):
    """
    Put the pmid to queue

    If full, trigger the community detection class
    """
    if LIFOq.full():
        last_pmid = LIFOq.get()
        community_detection_cls = CommunityDetection(last_pmid)
        community_detection_cls()

        LIFOq = queue.LifoQueue(maxsize=10_000)
    
    LIFOq.put(pmid)
