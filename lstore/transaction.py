from lstore.table import Table
from lstore.record import Record
from lstore.index import Index
from lstore.parser import *
import lstore.transaction_id as transaction_id
import time

class Transaction:

    __slots__ = 'queries', 'tid', 'success_rids', 'locks', 'update_rid'

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []

        self.tid = transaction_id.next_id
        transaction_id.next_id += 1

        # allow us to rollback
        self.success_rids = []
        # for unlock_all_locks
        self.locks = []
        self.update_rid = {} # map rid 

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
        self.queries.append((query, table, args))

    def run(self):
        for query, table, args in self.queries:
            query_name = query.__name__

            if query_name == 'select':
                result, holding_locks, success_rids = query.__self__.select_transaction(
                    *args, transaction_id = self.tid
                )
            elif query_name == 'insert':
                result, holding_locks, success_rids = query.__self__.insert_transaction(
                    *args, transaction_id = self.tid
                )
                self.success_rids += [[query_name, table, success_rids]]
            elif query_name == 'update':
                result, holding_locks, success_rids = query.__self__.update_transaction(
                    *args, transaction_id = self.tid, prevTailDict = self.update_rid
                )
                self.update_rid[success_rids[0]] = success_rids[1]
                self.success_rids += [[query_name, table, success_rids]]
            elif query_name == 'delete':
                result, holding_locks, success_rids = query.__self__.delete_transaction(
                    *args, transaction_id = self.tid
                )
                self.success_rids += [(query_name, table, success_rids)]
            else:
                result, holding_locks, success_rids = query.__self__.sum_transaction(
                    *args, transaction_id = self.tid
                )

            self.locks += [(table, holding_locks)]
            # If the query has failed the transaction should abort
            if result == False:
                print(self.tid, query_name, args, 'failed!')
                return self.abort()
        return self.commit()

    def abort(self):
        # Delete success_rids and undo index
        for query_table_rids in self.success_rids:
            query_name = query_table_rids[0]
            table = query_table_rids[1]
            rids = query_table_rids[2]
            if query_name == 'insert':
                # set invalid
                rid = rids[0]
                base_page_range = get_page_range_number(rid)
                base_page_number, base_offset = get_page_number_and_offset(rid)
                table.page_ranges[base_page_range].arr_of_base_pages[base_page_number].set(
                    base_offset, 200000000, 0
                )

        self.unlock_all_locks()
        return False

    def commit(self):
        # TODO: commit to database
        for query_table_rids in self.success_rids:
            query_name = query_table_rids[0]
            table = query_table_rids[1]
            rids = query_table_rids[2]

            if query_name == 'insert':
                # set timestamp
                rid = rids[0]
                base_page_range = get_page_range_number(rid)
                base_page_number, base_offset = get_page_number_and_offset(rid)
                table.page_ranges[base_page_range].arr_of_base_pages[base_page_number].set(
                    base_offset, int(time.time()), 2
                )
            elif query_name == 'update':
                # update indirection column
                # find base page and set indirection to tail
                # also change page_range update to not update base tail
                rid = rids[0]
                base_page_range = get_page_range_number(rid)
                base_page_number, base_offset = get_page_number_and_offset(rid)

                table.page_ranges[base_page_range].arr_of_base_pages[base_page_number].set(
                    base_offset, rids[1], 0
                )
            elif query_name == 'delete':
                # update indirection column to 'deleted'
                rid = rids[0]
                base_page_range = get_page_range_number(rid)
                base_page_number, base_offset = get_page_number_and_offset(rid)
                table.page_ranges[base_page_range].arr_of_base_pages[base_page_number].set(
                    base_offset, 200000000, 0
                )
        self.unlock_all_locks()
        return True

    """
    Unlock all holding locks
    """
    def unlock_all_locks(self):
        for table_lock in self.locks:
            for lock in table_lock[1]:
                table_lock[0].lock_manager.unlock(self.tid, lock)
        pass
