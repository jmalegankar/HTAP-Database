from lstore.index import Index
from lstore.pageRange import PageRange
from lstore.config import PAGE_RANGE_SIZE
import lstore.bufferpool as bufferpool
from threading import Lock
from lstore.lock_manager import LockManager
from lstore.parser import get_page_number_and_offset

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Table:
    
    __slots__ = ('page_ranges', 'name', 'records_per_range', 'num_columns', 'key',
        'index', 'page_range_number', 'merge_worker', 'latch', 'index_latch', 'lock_manager'
        )

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
        self.lock_manager = LockManager()

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
            merge_worker=self.merge_worker,
            lock_manager=self.lock_manager
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

    def rebuild_index(self):
        """
        data_index = [(key1, rid1), (key2, rid2), (key3, rid3), ...]
        """
        data_index = []
        key = self.key

        append = data_index.append
        for page_range_number, page_range in enumerate(self.page_ranges):
            num_of_columns = page_range.num_of_columns
            for page_number, base_page in enumerate(page_range.arr_of_base_pages):
                tps = base_page.tps
                for offset in range(base_page.num_records):
                    indirection, rid, time, schema = base_page.get_metadata_cols(offset)
                    primary_key = base_page.get(offset, key + 4)
                    
                    if time == 0:
                        # need to perform delete
                        base_page.set(offset, 200000000, 0)
                        continue

                    if indirection == 200000000:
                        continue

                    if indirection == 0 or indirection <= tps:
                        append((primary_key, rid))
                    else:
                        if schema & (1 << num_of_columns - key - 1):
                            tail_page_number, tail_offset = get_page_number_and_offset(indirection)
                            new_primary_key = page_range.arr_of_tail_pages[tail_page_number].get(tail_offset, key + 5)
                            append((new_primary_key, rid))
                        else:
                            append((primary_key, rid))

        self.index = Index(self, data_index)

    def open(self):
        data = bufferpool.shared.read_metadata(self.name + '.metadata')
        if data is not None:
            self.key = data[0]
            self.num_columns = data[1]
            self.page_range_number = data[2]

            for page_range in range(self.page_range_number + 1):
                self.page_ranges.append(PageRange(
                    self.name,
                    self.num_columns,
                    page_range,
                    True,
                    merge_worker=self.merge_worker,
                    lock_manager=self.lock_manager
                ))

            # Build index
            self.rebuild_index()
            return True

        return False

    def close(self):
        bufferpool.shared.write_metadata(self.name + '.metadata', (
            self.key,
            self.num_columns,
            self.page_range_number,
        ))

        for page_range in self.page_ranges:
            page_range.close()

    def __merge(self):
        # merge all base page with num_records == 511
        print("merge is happening")
        for page_range in self.page_ranges:
            for page_number in range(page_range.base_page_number + 1):
                if page_range.arr_of_base_pages[page_number].num_records >= 511:
                    page_range.merge(page_number)
