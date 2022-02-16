from lstore.record import Record
from lstore.parser import *
import re

class Query:

    __slots__ = ('table',)

    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        try:
            rid = self.table.index.locate(self.table.key, primary_key)
            if rid is None:
                return False

            page_range_number = get_page_range_number(rid)

            data = self.table.page_ranges[page_range_number].get_withRID(
                rid,
                self.table.index.indexed_columns
            )

            self.table.page_ranges[page_range_number].delete_withRID(rid)

            for col, value in enumerate(data):
                if value is not None:
                    self.table.index.remove(col, value, rid)
        except ValueError:
            return False
        else:
            return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        if len(columns) != self.table.num_columns:
            return False

        try:
            rid = self.table.index.locate(self.table.key, columns[self.table.key])
            if rid is not None:
                # Primary key must be unique
                return False
            
            page_range_number = self.table.get_next_page_range_number()
            rid = self.table.page_ranges[page_range_number].write(*columns)
            
            # Set indices for each column
            for column_index in range(self.table.num_columns):
                if self.table.index.indexed_columns[column_index] == 1:
                    self.table.index.set(column_index, columns[column_index], rid)
        except ValueError:
            return False
        else:
            return True

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
                if index_column == self.table.key:
                    rid = self.table.index.locate(index_column, index_value)
                    if rid is None:
                        return []
                    else:
                        rids = [rid]
                else:
                    rids = self.table.index.locate(index_column, index_value)

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
                
                key_index = self.table.index.indices[self.table.key]
                for primary_key in key_index:
                    rid = key_index[primary_key]
                    
                    page_range_number = get_page_range_number(rid)
                    
                    data = self.table.page_ranges[page_range_number].get_withRID(
                        rid, temp_query_columns
                    )
                    
                    if data[index_column] == index_value:
                        results.append(Record(rid, self.table.key, data))
    
            return results
        except ValueError:
            return False

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        try:
            rid = self.table.index.locate(self.table.key, primary_key)
            if rid is None:
                return False
            
            if columns[self.table.key] is not None:
                if self.table.index.locate(self.table.key, columns[self.table.key]) is not None:
                    return False

            page_range_number = get_page_range_number(rid)

            data = self.table.page_ranges[page_range_number].get_withRID(
                rid, [0 if column is None else 1 for column in columns]
            )

            self.table.page_ranges[page_range_number].update(rid, *columns)

            for col, value in enumerate(columns):
                if value is not None and self.table.index.indexed_columns[col] == 1:
                    self.table.index.replace(col, data[col], value, rid)
        except ValueError:
            return False
        else:
            return True

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
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            total = 0

            query_columns = [0] * self.table.num_columns
            query_columns[aggregate_column_index] = 1
            for rid in rids:
                page_range_number = get_page_range_number(rid)
                total += self.table.page_ranges[page_range_number].get_withRID(rid, query_columns)[aggregate_column_index]

            return total
        except:
            return False

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
        