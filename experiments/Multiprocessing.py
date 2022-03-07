##!/usr/bin/env python3
#
#from multiprocessing import Process, Queue
#import time
#
#class MergeWorkerProcess(Process):
#   def __init__(self, completed_queue):
#       Process.__init__(self)
#       self.pending_queue = Queue()
#       self.completed_queue = completed_queue
#       
#   def put(self, value):
#      self.pending_queue.put(value)
#       
#   def run(self):
#       while True:
#           value = self.pending_queue.get()
#           if value is None:
#               self.completed_queue.put(None)
#               break
#
#           process_result = value * 2
#           self.completed_queue.put((value, process_result))
#           print('Processed', value)
#
#if __name__ == '__main__': 
#   completed_queue = Queue()
#   process = MergeWorkerProcess(completed_queue)
#   process.start()
#
#   for i in range(5):
#       time.sleep(1)
#       process.put(i)
#       try:
#           result = completed_queue.get_nowait()
#           print('Parent received', result)
#       except:
#           pass
#
#   process.put(None)
#   process.join()
#   
#   while True:
#       result = completed_queue.get()
#       if result == None:
#           break
#       else:
#           print('Parent received', result)


from multiprocessing import Process, Pipe
import time

class MergeWorkerProcess(Process):
    def __init__(self, child_connection):
        Process.__init__(self)
        self.child_connection = child_connection
        
    def run(self):
        while True:
            value = self.child_connection.recv()
            if value is None:
                self.child_connection.send(None)
                self.child_connection.close()
                break
            
            process_result = value * 2
            self.child_connection.send((value, process_result))
            print('Processed', value)
            
if __name__ == '__main__': 
    parent_connection, child_connection = Pipe()
    process = MergeWorkerProcess(child_connection)
    process.start()
    
    for i in range(5):
        time.sleep(1)
        parent_connection.send(i)


        if parent_connection.poll():
            result = parent_connection.recv()
            print('Parent received', result)
            
    parent_connection.send(None)
    
    while parent_connection.poll():
        result = parent_connection.recv()
        if result == None:
            break
        else:
            print('Parent received', result)
            