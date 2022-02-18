from threading import Thread, Lock
import lstore.bufferpool as bufferpool
from queue import Queue
 
class MergeWorkerThread(Thread):

    def __init__(self, lock, queue):
        Thread.__init__(self)
        self.lock = lock
        self.queue = queue

    """
    Start the merging process
    """

    def run(self):
        while True:
            base_page, arr_of_tail_pages = self.queue.get() # block and wait
            print(base_page)
            print(arr_of_tail_pages)
            self.queue.task_done()

class MergeWorker:
    def __init__(self):
        self.queue = Queue()
        self.lock = Lock()
        self.thread = MergeWorkerThread(self.lock, self.queue)
        self.thread.daemon = True
        self.thread.start()
