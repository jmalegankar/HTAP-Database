"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
from collections import defaultdict

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns

    """
    Set index given column #, value (key of the dict/tree), and rid (the value)
    """

    def set(self, column, value, rid):
        self.indices[column][value].append(rid)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        # TODO: Change it to B tree search given value
        return self.indices[column][value]

    """
    Remove index given column #, value (key of the dict/tree), rid is None will remove all, or just one
    """

    def remove(self, column, value, rid = None):
        try:
            if rid == None:
                self.indices[column][value] = []
#               del self.indices[column][value]
            else:
                self.indices[column][value].remove(rid)
#               if len(self.indices[column][value]) == 0:
#                   del self.indices[column][value]
        except:
            pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # TODO: Use B tree range given begin/end
        return filter(lambda x: begin <= x <= end and len(self.indices[column][x]) > 0, self.indices[column])

    """
    Create index for all columns
    """
    
    def create_all_index(self):
        for i in range(len(self.indices)):
            self.create_index(i)
        return True

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        # TODO: Change defaultdict to B tree
        self.indices[column_number] = defaultdict(list)
        return True

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        self.indices[column_number] = None
        return True
    