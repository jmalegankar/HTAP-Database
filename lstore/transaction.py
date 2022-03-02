from lstore.table import Table
from lstore.record import Record
from lstore.index import Index

import uuid

class Transaction:

    __slots__ = 'queries', 'tid', 'success_rids', 'locks'

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.tid = uuid.uuid1()
        # allow us to rollback
        self.success_rids = []
        self.locks = []

    def __str__(self):
        string = 'Transaction {}\n'.format(self.tid)
        string += '=' * 15 + '\n'
        if len(self.queries) == 0:
            string += 'Transaction has no query'
        for query, args in self.queries:
            string += '{}: {}\n'.format(query.__name__, args)
        return string

    def __repr__(self):
        return self.__str__()

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))

    def run(self):
        for query, args in self.queries:
            if len(args) == 1:
                result = query(*args[0])
            else:
                result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        # Undo all indexing
        self.unlock_all_locks()
        return False

    def commit(self):
        # TODO: commit to database
        self.unlock_all_locks()
        return True

    """
    Unlock all holding locks
    """
    def unlock_all_locks(self):
        pass
        