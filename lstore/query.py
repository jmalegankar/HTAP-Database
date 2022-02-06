from lstore.table import Table
from lstore.record import Record
from lstore.index import Index
from lstore.parser import *


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        try:
            rid = self.table.get_key(primary_key)
            self.table.delete_key(primary_key)
            page_range_number = get_page_range_number(rid)
            self.table.page_ranges[page_range_number].delete_withRID(rid)

        except Exception as e:
            return False
        else:
            return True
        pass

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        if len(columns) != self.table.num_columns:
            return False
    
        page_range_number = self.table.get_next_page_range_number()
        rid = self.table.page_ranges[page_range_number].write(*columns)
        self.table.set_key(columns[self.table.key], rid)
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
    
    """
    A B C
    1 2 3
    2 2 4
    """

    def select(self, index_value, index_column, query_columns):
        rid=self.table.get_key(index_value)
        #rid=self.table.index.locate(index_column,index_value)
        page_range_number = get_page_range_number(rid)
        return [Record(rid, self.table.key, self.table.page_ranges[page_range_number].get_withRID(rid,query_columns))]

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        try:
            rid = self.table.get_key(primary_key)
            if rid == None:
                return False
            page_range_number = get_page_range_number(rid)
            self.table.page_ranges[page_range_number].update(rid, *columns)
        except Exception as e:
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
        pass

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
    