from lstore.table import Table
from lstore.record import Record
from lstore.index import Index
import lstore.lock_manager as lock_manager

class Transaction:

    __slots__ = 'queries', 'tid', 'success_rids', 'locks'

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []

        self.tid = lock_manager.shared.tid
        lock_manager.shared.tid += 1

        # allow us to rollback
        self.success_rids = []
        # for unlock_all_locks
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
            query_name = query.__name__

            if query_name == 'select':
                result = query.__self__.select_transaction(*args, transaction_id = self.tid)
            elif query_name == 'insert':
                result = query.__self__.insert_transaction(*args, transaction_id = self.tid)
            elif query_name == 'update':
                result = query.__self__.update_transaction(*args, transaction_id = self.tid)
            elif query_name == 'delete':
                result = query.__self__.delete_transaction(*args, transaction_id = self.tid)
            else:
                result = query.__self__.sum_transaction(*args, transaction_id = self.tid)

            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        # Delete success_rids and undo index
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
        