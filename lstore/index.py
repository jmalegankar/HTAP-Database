"""
A data strucutre holding indices for various columns of a table.
Key column should be indexd by default,
other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""
from btree.b_tree import BPlusTree
from collections import defaultdict
from lstore.parser import get_page_range_number

class Index:

    __slots__ = 'indices', 'indexed_columns', 'key', 'num_columns', 'table'

    def __init__(self, table, data=None):
        self.key = table.key
        self.indices = [None] *  table.num_columns
        self.indexed_columns = [0] * table.num_columns

        if data is not None:
            self.indexed_columns[self.key] = 1

            """
            data = [(key1, rid1), (key2, rid2), (key3, rid3), ...]
            """

            index = BPlusTree()
            for key_value in data:
                index[key_value[0]] = key_value[1]
            self.indices[self.key] = index

        self.num_columns = table.num_columns
        self.table = table

    def close(self):
        return (self.key, [self.indices[i] if i != self.key else self.indices[i].list_all() for i in range(self.num_columns)], self.indexed_columns)

    """
    Set index given column #, value (key of the dict/tree), and rid (the value)
    """

    def set(self, column, value, rid):
        if column == self.key:
            self.indices[column][value] = rid
        else:
            # Use dict
            self.indices[column][value].add(rid)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if column == self.key:
            return self.indices[column].query(value)
        else:
            # Use dict
            return list(self.indices[column][value])

    """
    Remove index given column #, value (key of the dict/tree), rid is None will remove all, or just one
    """

    def remove(self, column, value, rid = None):
        try:
            if column == self.key:
                self.indices[column].delete(value)
            else:
                # Use dict
                if rid is None:
                    self.indices[column][value] = set()
                else:
                    self.indices[column][value].remove(rid)
        except:
            pass


    def replace(self, column, old_value, new_value, rid):
        self.remove(column, old_value, rid)
        self.set(column, new_value, rid)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        return self.indices[self.key].range_query(begin, end)[1]

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        try:
            self.table.index_latch.acquire()
            if column_number < 0 or column_number >= self.num_columns:
                # out of index
                self.table.index_latch.release()
                return False

            if self.indexed_columns[column_number] == 1:
                self.table.index_latch.release()
                return False

            if column_number == self.key:
                self.indices[column_number] = BPlusTree()
            else:
                # Use dict
                self.indices[column_number] = defaultdict(set)
                if self.indexed_columns[self.key] == 1:
                    # Building index
                    temp_query_columns = [0] * self.num_columns
                    temp_query_columns[column_number] = 1

                    for rid in self.indices[self.key].values():
                        page_range_number = get_page_range_number(rid)
                        
                        value = self.table.page_ranges[page_range_number].get_withRID(
                            rid, temp_query_columns
                        )[column_number]

                        self.indices[column_number][value].add(rid)
            self.indexed_columns[column_number] = 1
            self.table.index_latch.release()
            return True
        except:
            self.table.index_latch.release()
            return False

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.table.index_latch.acquire()
        if column_number < 0 or column_number >= self.num_columns:
            # out of index
            self.table.index_latch.release()
            return False

        if self.indexed_columns[column_number] == 0:
            self.table.index_latch.release()
            return False

        self.indices[column_number] = None
        self.indexed_columns[column_number] = 0
        self.table.index_latch.release()
        return True
