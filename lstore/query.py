from lstore.record import Record
from lstore.parser import *
from lstore.table import Table
import re
from threading import Lock

class Query:

    __slots__ = 'table'

    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table: Table):
        self.table = table

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        try:
            self.table.index_latch.acquire()
            rid = self.table.index.locate(self.table.key, primary_key)
            self.table.index_latch.release()

            if rid is None:
                return False

            page_range_number = get_page_range_number(rid)

            data = self.table.page_ranges[page_range_number].get_withRID(
                rid,
                self.table.index.indexed_columns
            )

            self.table.page_ranges[page_range_number].delete_withRID(rid)

            self.table.index_latch.acquire()
            for col, value in enumerate(data):
                if value is not None:
                    self.table.index.remove(col, value, rid)
            self.table.index_latch.release()

        except ValueError as e:
            return False
        else:
            return True

    def delete_transaction(self, primary_key, transaction_id: int):
        lock_manager = self.table.lock_manager
        holding_locks, success_rids, key_locks = [], [], []

        try:
            self.table.index_latch.acquire()
            rid = self.table.index.locate(self.table.key, primary_key)
            self.table.index_latch.release()

            if rid is None:
                print('rid is none')
                return False, [], [], []

            if not lock_manager.lock_key(transaction_id, primary_key):
                print('failed to lock key')
                return False, [], [], []
            else:
                key_locks = [primary_key]

            page_range_number = get_page_range_number(rid)

            # X Lock for RID
            if not lock_manager.upgrade(transaction_id, rid):
                print('failed to lock rid')
                return False, holding_locks, success_rids, key_locks
            else:
                holding_locks = [rid]
    
            data = self.table.page_ranges[page_range_number].get_withRID(
                rid,
                self.table.index.indexed_columns
            )
            
            # Need a way to indicate we deleted the value
            # self.table.page_ranges[page_range_number].delete_withRID(rid)

            success_rids = [rid]

            self.table.index_latch.acquire()
            for col, value in enumerate(data):
                if value is not None:
                    self.table.index.remove(col, value, rid)
            self.table.index_latch.release()
            
        except ValueError as e:
            return False, holding_locks, success_rids, key_locks
        else:
            return True, holding_locks, success_rids, key_locks

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        if len(columns) != self.table.num_columns:
            return False

        try:
            self.table.index_latch.acquire()
            rid = self.table.index.locate(self.table.key, columns[self.table.key])
            self.table.index_latch.release()
            if rid is not None:
                # Primary key must be unique
                return False

            page_range_number = self.table.get_next_page_range_number()

            rid = self.table.page_ranges[page_range_number].write(*columns)

            # Set indices for each column
            self.table.index_latch.acquire()
            for column_index in range(self.table.num_columns):
                if self.table.index.indexed_columns[column_index] == 1:
                    self.table.index.set(column_index, columns[column_index], rid)
            self.table.index_latch.release()

        except ValueError as e:
            return False
        else:
            return True

    def insert_transaction(self, *columns, transaction_id: int):
        holding_locks, success_rids, key_locks = [], [], []

        if len(columns) != self.table.num_columns:
            return False, [], [], []

        try:
            self.table.index_latch.acquire()
            rid = self.table.index.locate(self.table.key, columns[self.table.key])
            # Need to S lock columns[self.table.key]
            self.table.index_latch.release()

            if rid is not None:
                # Primary key must be unique
                return False, holding_locks, success_rids, key_locks

            if not self.table.lock_manager.lock_key(transaction_id, columns[self.table.key]):
                return False, holding_locks, success_rids, key_locks
            else:
                key_locks = [columns[self.table.key]]

            page_range_number = self.table.get_next_page_range_number()

            # X LOCK for RID inside the write function, should not abort
            rid = self.table.page_ranges[page_range_number].write(*columns, transaction_id=transaction_id)
            holding_locks = [rid]
            success_rids = [rid]

            # Set indices for each column
            self.table.index_latch.acquire()
            for column_index in range(self.table.num_columns):
                if self.table.index.indexed_columns[column_index] == 1:
                    self.table.index.set(column_index, columns[column_index], rid)
            self.table.index_latch.release()

        except ValueError as e:
            return False, holding_locks, success_rids, key_locks
        else:
            # print(columns[self.table.key], 'inserted')
            return True, holding_locks, success_rids, key_locks

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, index_value, index_column, query_columns):
        try:
            results = []
            if self.table.index.indexed_columns[index_column] == 1:

                self.table.index_latch.acquire()
                if index_column == self.table.key:
                    rid = self.table.index.locate(index_column, index_value)
                    if rid is None:
                        self.table.index_latch.release()
                        return []
                    else:
                        rids = [rid]
                else:
                    rids = self.table.index.locate(index_column, index_value)
                self.table.index_latch.release()

                for rid in rids:
                    page_range_number = get_page_range_number(rid)
                    
                    results.append(
                        Record(
                            rid,
                            self.table.key,
                            self.table.page_ranges[page_range_number].get_withRID(rid, query_columns)
                        )
                    )
            else:
                temp_query_columns = query_columns
                temp_query_columns[index_column] = 1

                self.table.index_latch.acquire()
                rids = self.table.index.indices[self.table.key].values()
                self.table.index_latch.release()

                for rid in rids:
                    page_range_number = get_page_range_number(rid)

                    data = self.table.page_ranges[page_range_number].get_withRID(
                        rid, temp_query_columns
                    )

                    if data[index_column] == index_value:
                        results.append(Record(rid, self.table.key, data))

            return results
        except ValueError as e:
            return False

    def select_transaction(self, index_value, index_column, query_columns, transaction_id: int):
        lock_manager = self.table.lock_manager
        holding_locks = []

        try:
            results = []
            if self.table.index.indexed_columns[index_column] == 1:
                
                self.table.index_latch.acquire()
                if index_column == self.table.key:
                    rid = self.table.index.locate(index_column, index_value)
                    if rid is None:
                        self.table.index_latch.release()
                        return [], [], [], []
                    else:
                        rids = [rid]
                else:
                    rids = self.table.index.locate(index_column, index_value)
                self.table.index_latch.release()

                # S LOCK for RIDS
                for rid in rids:
                    if not lock_manager.lock(transaction_id, rid):
                        print(transaction_id, 'select rid not locked!')
                        return False, holding_locks, [], []
                    else:
                        holding_locks.append(rid)
                    page_range_number = get_page_range_number(rid)
                    
                    results.append(
                        Record(
                            rid,
                            self.table.key,
                            self.table.page_ranges[page_range_number].get_withRID(rid, query_columns)
                        )
                    )
            else:
                temp_query_columns = query_columns
                temp_query_columns[index_column] = 1
                
                self.table.index_latch.acquire()
                rids = self.table.index.indices[self.table.key].values()
                self.table.index_latch.release()
                
                # S LOCK for RIDS
                for rid in rids:
                    if not lock_manager.lock(transaction_id, rid):
                        print(transaction_id, 'select rid not locked!')
                        return False, holding_locks, [], []
                    else:
                        holding_locks.append(rid)

                    page_range_number = get_page_range_number(rid)
                    
                    data = self.table.page_ranges[page_range_number].get_withRID(
                        rid, temp_query_columns
                    )
                    
                    if data[index_column] == index_value:
                        results.append(Record(rid, self.table.key, data))
                        
            return results, holding_locks, [], []
        except ValueError as e:
            return False, holding_locks, [], []

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        try:
            self.table.index_latch.acquire()
            rid = self.table.index.locate(self.table.key, primary_key)
            self.table.index_latch.release()

            if rid is None:
                return False
            
            if columns[self.table.key] is not None:
                self.table.index_latch.acquire()
                if self.table.index.locate(self.table.key, columns[self.table.key]) is not None:
                    self.index_latch.release()
                    return False
                self.table.index_latch.release()

            page_range_number = get_page_range_number(rid)

            data = self.table.page_ranges[page_range_number].get_withRID(
                rid, [0 if column is None else 1 for column in columns]
            )

            self.table.page_ranges[page_range_number].update(rid, *columns)

            self.table.index_latch.acquire()
            for col, value in enumerate(columns):
                if value is not None and self.table.index.indexed_columns[col] == 1:
                    self.table.index.replace(col, data[col], value, rid)
            self.table.index_latch.release()

        except ValueError as e:
            return False
        else:
            return True

    def update_transaction(self, primary_key, *columns, transaction_id: int, prevTailDict):
        lock_manager = self.table.lock_manager
        holding_locks, success_rids, key_locks = [], [], []

        try:
            self.table.index_latch.acquire()
            rid = self.table.index.locate(self.table.key, primary_key)
            self.table.index_latch.release()
            
            if rid is None:
                print('rid is None')
                return False, [], [], []
            
            if not lock_manager.lock_key(transaction_id, primary_key):
                print(transaction_id, 'old lock not locked!')
                return False, [], [], []
            else:
                key_locks.append(primary_key)
            
            if columns[self.table.key] is not None:
                self.table.index_latch.acquire()
                if self.table.index.locate(self.table.key, columns[self.table.key]) is not None:
                    self.index_latch.release()
                    print(transaction_id, 'dup key')
                    return False, holding_locks, success_rids, key_locks
                self.table.index_latch.release()

                if not lock_manager.lock_key(transaction_id, columns[self.table.key]):
                    print(transaction_id, 'new key not locked')
                    return False, holding_locks, success_rids, key_locks
                else:
                    key_locks.append(columns[self.table.key])

            # X LOCK for RID
            if not lock_manager.upgrade(transaction_id, rid):
                print('rid not locked')
                return False, holding_locks, success_rids, key_locks
            else:
                holding_locks = [rid]

            page_range_number = get_page_range_number(rid)

            data = self.table.page_ranges[page_range_number].get_withRID(
                rid, [0 if column is None else 1 for column in columns]
            )

            prevTail = prevTailDict.get(rid, -1)
            new_tail_rid = self.table.page_ranges[page_range_number].update(rid, *columns, prevTail=prevTail, from_transaction=True)

            success_rids = [rid, new_tail_rid]

            self.table.index_latch.acquire()
            for col, value in enumerate(columns):
                if value is not None and self.table.index.indexed_columns[col] == 1:
                    self.table.index.replace(col, data[col], value, rid)
            self.table.index_latch.release()

        except ValueError as e:
            return False, holding_locks, success_rids, key_locks
        else:
            return True, holding_locks, success_rids, key_locks

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            self.table.index_latch.acquire()
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            self.table.index_latch.release()
            total = 0

            query_columns = [0] * self.table.num_columns
            query_columns[aggregate_column_index] = 1
            for rid in rids:
                page_range_number = get_page_range_number(rid)
                total += self.table.page_ranges[page_range_number].get_withRID(rid, query_columns)[aggregate_column_index]

            return total
        except ValueError as e:
            return False

    def sum_transaction(self, start_range, end_range, aggregate_column_index, transaction_id: int):
        lock_manager = self.table.lock_manager
        holding_locks = []

        try:
            self.table.index_latch.acquire()
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            self.table.index_latch.release()
            total = 0

            query_columns = [0] * self.table.num_columns
            query_columns[aggregate_column_index] = 1

            # S LOCK for RIDS
            for rid in rids:
                if not lock_manager.lock(transaction_id, rid):
                    return False, holding_locks, [], []
                else:
                    holding_locks.append(rid)

                page_range_number = get_page_range_number(rid)
                total += self.table.page_ranges[page_range_number].get_withRID(rid, query_columns)[aggregate_column_index]

            return total, holding_locks, [], []
        except ValueError as e:
            return False, [], [], []

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        result = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if result is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = result[column] + 1
            updated = self.update(key, *updated_columns)
            return updated
        return False

    def increment_transaction(self, key, column, transaction_id: int):
        result, holding_locks, success_rids, key_locks = self.select_transaction(key, self.table.key, [1] * self.table.num_columns)[0]
        if result is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = result[column] + 1
            updated = self.update(key, *updated_columns, transaction_id=transaction_id)
            return updated
        return False, holding_locks, success_rids, key_locks

    """
    Simple SQL for debug

    Query.select(index_value, index_column, query_columns) ->
        SELECT query_columns WHERE index_column = index_value
    Ex:
        'SELECT * WHERE 0 = 100'    (select all columns, records where the first column is 100)
        'SELECT 0, 2 WHERE 1 = 50'  (select columns 0 & 2, records where the second column is 50)

    Query.update(primary_key, *columns) ->
        UPDATE column index = new value WHERE primary_key
    Ex:
        'UPDATE 1 = 10, 2 = 20 WHERE 1' (find primary key is 1, set second column 10 and third 20)

    Query.insert(*columns) ->
        INSERT VALUES (columns)
    Ex:
        'INSERT VALUES (1, 2, 3)    (insert record with columns = [1, 2, 3]

    Query.delete(primary_key) ->
        DELETE primary_key
    Ex:
        'DELETE 1'  (delete record with primary key = 1
    """

    def sql(self, command):
        try:
            command = command.lower()
            if command[:6] == 'select':
                selection, _, condition = command[7:].partition(' where ')
                if selection[0] == '*':
                    select_columns = [1] * self.table.num_columns
                else:
                    select_columns = [0] * self.table.num_columns
                    for selected_column in re.split('\s*,\s*', selection):
                        select_columns[int(selected_column)] = 1

                condition_result = re.match('(\d)\s*=\s*(-?\d)', condition)

                if condition_result == None:
                    return False
                else:
                    return self.select(int(condition_result.group(2)), int(condition_result.group(1)), select_columns)
            elif command[:6] == 'update':
                update_data = re.findall('(\d+)\s*=\s*(-?\d+)', command[7:])
                primary_key = re.search('where\s*(-?\d+)', command[7:]).group(1)
                
                update_columns = [None] * self.table.num_columns
                for data in update_data:
                    update_columns[int(data[0])] = int(data[1])

                return self.update(int(primary_key), *update_columns)
            elif command[:6] == 'insert':
                values = re.match('.*\((.+)\)', command[7:]).group(1)
                columns = [int(value) for value in re.split('\s*,\s*', values)]
                return self.insert(*columns)
            elif command[:6] == 'delete':
                primary_key = re.match('\s*(-?\d+)\s*', command[7:]).group(1)
                return self.delete(int(primary_key))
            else:
                return False
        except:

            return False
        