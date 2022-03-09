from lstore.table import Table
from lstore.record import Record
from lstore.index import Index
from lstore.parser import *
import lstore.transaction_id as transaction_id
import time
import lstore.bufferpool as bufferpool

class Transaction:

    __slots__ = 'queries', 'tid', 'success_rids', 'locks', 'update_rid', 'query_index', 'tables'

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

        self.query_index = {} # used to record original record

        self.tables = {}


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
    def add_query(self, query, table: Table, *args):
        if table.name not in self.tables:
            self.tables[table.name] = table

        self.queries.append((query, table.name, args))


    def run(self):
        for query, table, args in self.queries:
            query_name = query.__name__

            if query_name == 'select':
                result, holding_locks = query.__self__.select_transaction(
                    *args, transaction_id = self.tid
                )
            elif query_name == 'insert':
                result, holding_locks, success_rids, old_index = query.__self__.insert_transaction(
                    *args, transaction_id = self.tid
                )

                if result is not False:
                    self.success_rids += [[query_name, table, success_rids]]
                    self.add_to_index(success_rids[0], table, old_index)
                # Add old index here
            elif query_name == 'update':
                result, holding_locks, success_rids, old_index = query.__self__.update_transaction(
                    *args, transaction_id = self.tid, prevTailDict = self.update_rid
                )

                if result is not False:
                    self.update_rid[success_rids[0]] = success_rids[1]
                    self.success_rids += [[query_name, table, success_rids]]
                    self.add_to_index(success_rids[0], table, old_index)
            elif query_name == 'delete':
                result, holding_locks, success_rids, old_index = query.__self__.delete_transaction(
                    *args, transaction_id = self.tid
                )

                if result is not False:
                    self.success_rids += [(query_name, table, success_rids)]
                    self.add_to_index(success_rids[0], table, old_index)
            elif query_name == 'sum':
                result, holding_locks = query.__self__.sum_transaction(
                    *args, transaction_id = self.tid
                )
            else:
                result, holding_locks = query.__self__.increment_transaction(
                    *args, transaction_id = self.tid
                )

            self.locks += [(table, holding_locks)]

            if result == False:
                # If the query has failed the transaction should abort
                print(self.tid, query_name, args, 'failed!')
                return self.abort()
        # No issue after all transactions, commit!
        return self.commit()


    def add_to_index(self, table_name, base_rid,old_index):
        record = self.query_index.get((table_name, base_rid))
        if record is None:
            self.query_index[(table_name, base_rid)] = old_index
        else:
            for idx, column_info in enumerate(old_index):
                if column_info:
                    if not record[idx]:
                        record[idx] = column_info
                    else:
                        record[idx][1] = column_info[1]
            self.query_index[(table_name, base_rid)] = record
                       

    def abort(self):
        # Delete success_rids and undo index
        for query_table_rids in self.success_rids:
            query_name, table_name, rids = query_table_rids[0], query_table_rids[1], query_table_rids[2]
            table = self.tables[table_name]

            if query_name == 'insert':
                # set invalid
                rid = rids[0]
                base_page_range = get_page_range_number(rid)
                base_page_number, base_offset = get_page_number_and_offset(rid)
                table.page_ranges[base_page_range].arr_of_base_pages[base_page_number].set(
                    base_offset, 200000000, 0
                )

        self.undo_index()
        self.unlock_all_locks()
        return False


    def undo_index(self):
        # abort, need to undo the index
        try:
            for table_rid, record in self.query_index.items():
                table_name, rid = table_rid
                table = self.tables[table_name]

                table.index_latch.acquire()
                for idx, column_info in enumerate(record):
                    if column_info:
                        # need to update back
                        old_value, new_value = column_info[0], column_info[1]

                        if new_value is not None:
                            # remove new value from index
                            table.index.remove(idx, new_value, rid)
            
                        if old_value is not None:
                            table.index.set(idx, old_value, rid)

                table.index_latch.release()
        except:
            print('Undo_index failed')


    def commit(self):
        # TODO: commit to database
        for query_table_rids in self.success_rids:
            query_name, table_name, rids = query_table_rids[0], query_table_rids[1], query_table_rids[2]
            table = self.tables[table_name]

            if query_name == 'insert':
                # set timestamp
                rid = rids[0]
                base_page_range = get_page_range_number(rid)
                base_page_number, base_offset = get_page_number_and_offset(rid)
                table.page_ranges[base_page_range].arr_of_base_pages[base_page_number].set_and_save(
                    base_offset, int(time.time()), 2
                )

                # TODO: save base page to disk
                table.page_ranges[base_page_range].close()
            elif query_name == 'update':
                # update indirection column
                # find base page and set indirection to tail
                # also change page_range update to not update base tail
                rid = rids[0]
                tail_rid = rids[1]
                base_page_range = get_page_range_number(rid)
                tail_page_range = get_page_range_number(tail_rid)
                base_page_number, base_offset = get_page_number_and_offset(rid)

                table.page_ranges[base_page_range].arr_of_base_pages[base_page_number].set_and_save(
                    base_offset, rids[1], 0
                )

                # TODO: save base and tail page to disk
                table.page_ranges[base_page_range].close()
                table.page_ranges[tail_page_range].close()
            elif query_name == 'delete':
                # update indirection column to 'deleted'
                rid = rids[0]
                base_page_range = get_page_range_number(rid)
                base_page_number, base_offset = get_page_number_and_offset(rid)
                table.page_ranges[base_page_range].arr_of_base_pages[base_page_number].set_and_save(
                    base_offset, 200000000, 0
                )

                # TODO: save base page to disk
                table.page_ranges[base_page_range].close()
        self.unlock_all_locks()
        return True


    """
    Unlock all holding locks
    """
    def unlock_all_locks(self):
        try:
            for tablename_lock in self.locks:
                for lock in tablename_lock[1]:
                    self.tables[tablename_lock[0]].lock_manager.unlock(self.tid, lock)
        except:
            print('Unlock all locks failed')
