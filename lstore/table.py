from lstore.index import Index
from lstore.pageRange import PageRange
from lstore.config import PAGE_RANGE_SIZE
import lstore.bufferpool as bufferpool
from threading import Lock

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Table:
    
    __slots__ = ('page_ranges', 'name', 'records_per_range', 'num_columns', 'key',
        'index', 'page_range_number', 'merge_worker', 'latch', 'index_latch')

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key, open_from_db=False, merge_worker=None):
        self.page_ranges = []
        self.name = name
        self.latch = Lock()
        self.index_latch = Lock()

        # PAGE_RANGE_SIZE base pages, each 511 records is the max
        self.records_per_range = PAGE_RANGE_SIZE * 511
        
        self.merge_worker = merge_worker
        if open_from_db and self.open():
            pass
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
        self.page_ranges.append(PageRange(
            self.name,
            self.num_columns,
            self.page_range_number,
            merge_worker=self.merge_worker
        ))

    def is_page_range_full(self):
        if self.page_range_number == -1:
            return True

        return self.page_ranges[self.page_range_number].num_records >= self.records_per_range

    def get_next_page_range_number(self):
        self.latch.acquire()
        if self.is_page_range_full():
            self.create_a_new_page_range()
        prn = self.page_range_number
        self.page_ranges[prn].num_records += 1
        self.latch.release()
        return prn

    def open(self):
        data = bufferpool.shared.read_metadata(self.name + '.metadata')
        data_index = bufferpool.shared.read_metadata(self.name + '.index')
        if data is not None and data_index is not None:
            self.key = data[0]
            self.num_columns = data[1]
            self.page_range_number = data[2]

            for page_range in range(self.page_range_number + 1):
                self.page_ranges.append(PageRange(
                    self.name,
                    self.num_columns,
                    page_range,
                    True,
                    merge_worker=self.merge_worker
                ))

            self.index = Index(self, data_index)
            return True

        return False

    def close(self):
        bufferpool.shared.write_metadata(self.name + '.metadata', (
            self.key,
            self.num_columns,
            self.page_range_number,
        ))

        bufferpool.shared.write_metadata(self.name + '.index', (self.index.close()))

        for page_range in self.page_ranges:
            page_range.close()

        bufferpool.shared.close()

    def __merge(self):
        # merge all base page with num_records == 511
        print("merge is happening")
        for page_range in self.page_ranges:
            for page_number in range(page_range.base_page_number + 1):
                if page_range.arr_of_base_pages[page_number].num_records >= 511:
                    page_range.merge(page_number)
