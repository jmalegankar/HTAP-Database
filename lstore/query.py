from lstore.record import Record
from lstore.parser import *

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
        rid = self.table.index.locate(self.table.key, primary_key)
        if len(rid) != 1:
            return False
        rid = rid[0]

        try:
            page_range_number = get_page_range_number(rid)

            data = self.table.page_ranges[page_range_number].get_withRID(
                rid,
                [1] * self.table.num_columns
            )

            self.table.page_ranges[page_range_number].delete_withRID(rid)

            for col, value in enumerate(data):
                self.table.index.remove(col, value, rid)
        except:
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

        if len(self.table.index.locate(self.table.key, columns[self.table.key])) > 0:
            # Primary key must be unique
            return False

        page_range_number = self.table.get_next_page_range_number()
        rid = self.table.page_ranges[page_range_number].write(*columns)

        # Set indices for each column
        for col, value in enumerate(columns):
            self.table.index.set(col, value, rid)

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
            rids = self.table.index.locate(index_column, index_value)

            results = []
            for rid in rids:
                page_range_number = get_page_range_number(rid)

                results.append(
                    Record(
                        rid,
                        self.table.key,
                        self.table.page_ranges[page_range_number].get_withRID(rid, query_columns)
                    )
                )

            return results
        except:
            return False

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        rid = self.table.index.locate(self.table.key, primary_key)
        if len(rid) != 1:
            return False
        rid = rid[0]

        try:
            if columns[self.table.key] is not None:
                new_rid = self.table.index.locate(self.table.key, columns[self.table.key])
                if len(new_rid) != 0:
                    return False

            page_range_number = get_page_range_number(rid)
            query_columns = []

            for column in columns:
                if column is None:
                    query_columns.append(0)
                else:
                    query_columns.append(1)

            data = self.table.page_ranges[page_range_number].get_withRID(rid, query_columns)
            self.table.page_ranges[page_range_number].update(rid, *columns)

            for col, value in enumerate(columns):
                if value is not None:
                    self.table.index.replace(col, data[col], value, rid)
        except:
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
                total += self.select(rid, self.table.key, query_columns)[0][aggregate_column_index]
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
