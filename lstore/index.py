"""
A data strucutre holding indices for various columns of a table.
Key column should be indexd by default,
other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""
from collections import defaultdict

class Index:

    __slots__ = 'indices', 'indexed_columns', 'key'

    def __init__(self, table):
        self.indices = [None] *  table.num_columns
        self.indexed_columns = [0] * table.num_columns
        self.key = table.key

    """
    Set index given column #, value (key of the dict/tree), and rid (the value)
    """

    def set(self, column, value, rid):
        if column == self.key:
            # TODO: Use B tree
            self.indices[column][value] = rid
        else:
            # Use dict
            self.indices[column][value].add(rid)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if column == self.key:
            # TODO: Use B tree
            return self.indices[column].get(value, None)
        else:
            # Use dict
            return list(self.indices[column][value])

    """
    Remove index given column #, value (key of the dict/tree), rid is None will remove all, or just one
    """

    def remove(self, column, value, rid = None):
        try:
            if column == self.key:
                # TODO: Use B tree
                del self.indices[column][value]
            else:
                # Use dict
                if rid is None:
                    self.indices[column][value] = set()
                else:
                    self.indices[column][value].remove(rid)
        except KeyError:
            pass


    def replace(self, column, old_value, new_value, rid):
        self.remove(column, old_value, rid)
        self.set(column, new_value, rid)

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # TODO: Use B tree range given begin/end
        rids = []
        for key, rid in self.indices[self.key].items():
            if begin <= key <= end:
                rids.append(rid)
        return rids

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        if self.indexed_columns[column_number] == 1:
            return False

        if column_number == self.key:
            # TODO: Use B tree
            self.indices[column_number] = dict()
        else:
            # Use dict
            self.indices[column_number] = defaultdict(set)

        self.indexed_columns[column_number] = 1
        return True

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if self.indexed_columns[column_number] == 0:
            return False

        self.indices[column_number] = None
        self.indexed_columns[column_number] = 0
        return True
    