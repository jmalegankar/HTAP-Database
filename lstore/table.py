from lstore.index import Index
from time import time
from lstore.pageRange import PageRange

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.page_ranges = []
        self.page_range_number = -1
        self.index = Index(self)
        self.key_index = {} # map key to RID
        pass
        
    
    def __str__(self):
        string = 'num_columns: {}, page_range_number: {}\n'.format(self.num_columns, self.page_range_number)
        string += '=' * 35 + '\n'
        if self.page_range_number == -1:
            string += 'Table is Empty'
        for page_range in self.page_ranges:
            string += str(page_range) + '\n'
        return string


    def set_key(self, key, rid):
        self.key_index[key] = rid


    def get_key(self, key):
        return self.key_index[key] if key in self.key_index else None


    def delete_key(self, key):
        del self.key_index[key]


    def create_a_new_page_range(self):
        # WIP
        self.page_range_number += 1
        self.page_ranges.append(PageRange(self.num_columns, self.page_range_number))


    def is_page_range_full(self):
        if self.page_range_number == -1:
            return True
        else:
            # 8 base pages, each 512 records is the max
            return self.page_ranges[self.page_range_number].num_records >= 4096


    def get_next_page_range_number(self):
        if self.is_page_range_full():
            self.create_a_new_page_range()
        return self.page_range_number


    def __merge(self):
        print("merge is happening")
        pass
 
