from lstore.table import Table
from lstore.record import Record
from lstore.index import Index
from threading import Thread
from copy import deepcopy

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions=[]):
        self.stats = []

        if transactions == []:
            self.transactions = []
        else:
            self.transactions = transactions

        self.result = 0
        self.thread = None

    def __str__(self):
        string = 'Transaction Worker: {}\n'
        string += '=' * 25 + '\n'
        if len(self.transactions) == 0:
            string += 'Worker has no transaction'
        for transaction in self.transactions:
            string += str(transaction) + '\n'
        return string
    
    def __repr__(self):
        return self.__str__()

    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        self.transactions.append(t)


    """
    Runs all transaction as a thread
    """
    def run(self):
        self.thread = Thread(target=self.__run, args=())
        self.thread.start()
    

    """
    Waits for the worker to finish
    """
    def join(self):
        if self.thread is not None:
            self.thread.join()


    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
        