from lstore.index import Index
from lstore.pageRange import PageRange
from lstore.config import PAGE_RANGE_SIZE
import lstore.bufferpool as bufferpool

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Table:
    
    __slots__ = 'page_ranges', 'name', 'records_per_range', 'num_columns', 'key', 'index', 'page_range_number'

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key, open_from_db=False):
        self.page_ranges = []
        self.name = name

        # PAGE_RANGE_SIZE base pages, each 511 records is the max
        self.records_per_range = PAGE_RANGE_SIZE * 511

        if open_from_db and self.open():
#           print('Open table ' + name + ', status=OK!')
            self.index.create_index(self.key)
        else:
            self.key = key
            self.num_columns = num_columns
            self.page_range_number = -1
            self.index = Index(self)
            self.index.create_index(key)

    def __str__(self):
        string = 'num_columns: {}, page_range_number: {}\n'.format(
            self.num_columns,
            self.page_range_number
        )

        string += '=' * 35 + '\n'
        if self.page_range_number == -1:
            string += 'Table is Empty'
        for page_range in self.page_ranges:
            string += str(page_range) + '\n'
        return string

    def __repr__(self):
        return self.__str__()

    def create_a_new_page_range(self):
        assert self.page_range_number < 99
        self.page_range_number += 1
        self.page_ranges.append(PageRange(self.name, self.num_columns, self.page_range_number))

    def is_page_range_full(self):
        if self.page_range_number == -1:
            return True
        return self.page_ranges[self.page_range_number].num_records >= self.records_per_range

    def get_next_page_range_number(self):
        if self.is_page_range_full():
            self.create_a_new_page_range()
        return self.page_range_number

    def open(self):
        data = bufferpool.shared.read_metadata(self.name + '.metadata')
        data_index = bufferpool.shared.read_metadata(self.name + '.index')
        if data is not None and data_index is not None:
            self.key = data[0]
            self.num_columns = data[1]
            self.page_range_number = data[2]

            for page_range in range(self.page_range_number + 1):
                self.page_ranges.append(PageRange(self.name, self.num_columns, page_range, True))

            self.index = data_index
            return True

#       print('Open table ' + self.name, ', status=ERROR')
        return False

    def close(self):
        bufferpool.shared.write_metadata(self.name + '.metadata', (
            self.key,
            self.num_columns,
            self.page_range_number,
        ))

        bufferpool.shared.write_metadata(self.name + '.index', self.index)

        for page_range in self.page_ranges:
            page_range.close()

        bufferpool.shared.close()

    def __merge(self):
        print("merge is happening")
        