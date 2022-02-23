from lstore.basepage import BasePage
from lstore.record import Record
from lstore.parser import *
import lstore.bufferpool as bufferpool
import copy
from lstore.config import PAGE_RANGE_SIZE, MERGE_BASE_AFTER

class PageRange:

    __slots__ = ('num_of_columns', 'arr_of_base_pages', 'arr_of_tail_pages',
        'range_number', 'base_page_number', 'tail_page_number', 'page_range_path',
        'num_records', 'num_updates', 'merge_worker')

    def __init__(self, table_name, columns, range_number, open_from_db=False, merge_worker=None):
        self.num_of_columns = columns
        self.arr_of_base_pages=[]
        self.arr_of_tail_pages=[]
        self.range_number = range_number
        self.base_page_number = -1
        self.tail_page_number = -1

        self.page_range_path = table_name + '/page_range_' + str(range_number)

        self.num_records = 0
        self.num_updates = 0 # If we decided to merge each page range

        bufferpool.shared.create_folder(self.page_range_path + '_b')
        bufferpool.shared.create_folder(self.page_range_path + '_t')

        if open_from_db:
            self.open()

        self.merge_worker = merge_worker

    """
    Debug Only
    """

    def __str__(self):
        string = 'Number: {}, Base: {}/8, tail: {}\n'.format(
            self.range_number,
            self.base_page_number + 1,
            self.tail_page_number + 1
        )
        string += '=' * 25 + '\n'
        string += 'BasePages:\n'
        if self.base_page_number > -1:
            for index, base_page in enumerate(self.arr_of_base_pages):
                string += 'BasePage {}\n'.format(index)
                string += str(base_page)
        else:
            string += 'Empty\n'

        string += '\nTailPages:\n'
        if self.tail_page_number > -1:
            for index, tail_page in enumerate(self.arr_of_tail_pages):
                string += 'TailPage {}\n'.format(index)
                string += str(tail_page)
        else:
            string += 'Empty\n'
        return string

    """
    Checks if page range has capacity, which we need to define 8????
    """

    def pageRange_has_capacity(self):
        return self.base_page_number + 1 < PAGE_RANGE_SIZE

    def is_page_full(self, page_number, isTail=False):
        if page_number == -1:
            """ We don't have any page yet """
            return True

        if isTail:
            return not self.arr_of_tail_pages[page_number].has_capacity()
        else:
            return not self.arr_of_base_pages[page_number].has_capacity()

    """
    creates a base page or tail page
    """

    def create_a_new_page(self, isTail=False):
        if isTail:
            self.tail_page_number += 1
            self.arr_of_tail_pages.append(BasePage(
                self.num_of_columns,
                self.page_range_path + '_t/tail_' + str(self.tail_page_number)
            ))
        else:
            assert self.pageRange_has_capacity()
            self.base_page_number += 1
            self.arr_of_base_pages.append(BasePage(
                self.num_of_columns,
                self.page_range_path + '_b/base_' + str(self.base_page_number),
                0,
                0
            ))

    """
    Write a record, given the columns data
    Return Record ID for table to build the index
    """

    def write(self, *columns):
        assert len(columns) == self.num_of_columns
        # Data OK, create new RID for this record
        if self.is_page_full(self.base_page_number):
            self.create_a_new_page()

        rid = create_rid(
            0,
            self.range_number,
            self.base_page_number,
            self.arr_of_base_pages[self.base_page_number].get_next_rec_num()
        )

        record = Record(rid, -1, columns)
        self.arr_of_base_pages[self.base_page_number].write(record)
        self.num_records += 1
        return rid

    """
    Get a record given page_number and offset
    """

    def get(self, page_number, offset, Q_col=None, isTail=False, rid=None):
        if isTail:
            if Q_col is None:
                return self.arr_of_tail_pages[page_number].get_user_cols(offset)
            return self.arr_of_tail_pages[page_number].get_cols(offset, Q_col)
        else:
            indirection = self.arr_of_base_pages[page_number].get(offset, 0)

            if indirection == 200000000:
                # we set indirection to 200000000 when we deleted the record
                return None

            base_record = self.arr_of_base_pages[page_number].get_cols(offset, Q_col)

            if indirection == 0 or indirection <= self.arr_of_base_pages[page_number].tps:
                # No tail update or no tail update after merged
#               if indirection <= self.arr_of_base_pages[page_number].tps:
#               print('shortcut!', indirection, 'vs', self.arr_of_base_pages[page_number].tps, 'for', rid)
                return base_record

#           print('no shortcut!', indirection, 'vs', self.arr_of_base_pages[page_number].tps, 'for', rid)
#           base_schema = self.arr_of_base_pages[page_number].get(offset, 3)
            tail_page_number, tail_offset = get_page_number_and_offset(indirection)
            tail_schema = self.arr_of_tail_pages[tail_page_number].get(tail_offset, 3)
            updated_record = self.arr_of_tail_pages[tail_page_number].get_cols(tail_offset, Q_col)

            for index in range(self.num_of_columns):
                if (tail_schema & (1 << (self.num_of_columns - index - 1)) and
                    (Q_col is None or Q_col[index] == 1)):
                    base_record[index] = updated_record[index]
            return base_record

    """
    Get a record given RID
    If query columns (Q_col) is None, return all
    """

    def get_withRID(self, rid, Q_col=None, isTail=False):
        page_number, offset = get_page_number_and_offset(rid)
        return self.get(page_number, offset, Q_col, isTail, rid)

    """
    Get a tail record given RID
    return all columns (internal and user)
    """

    def get_tail_withRID(self, rid):
        page_number, offset = get_page_number_and_offset(rid)
        return self.arr_of_tail_pages[page_number].get_all_cols(offset)

    """
    Delete a record given RID
    """

    def delete_withRID(self, rid):
        base_page_number, base_offset = get_page_number_and_offset(rid)
        # set indir to 200000000
        indirection = self.arr_of_base_pages[base_page_number].set(base_offset, 200000000, 0)

    """
    Adds a tail page, return the tail id of the new page
    Needs a base RID to update and columns of data.
    """

    def update(self, base_rid, *columns):
        assert len(columns) == self.num_of_columns

        if self.is_page_full(self.tail_page_number, True):
            self.create_a_new_page(True)

        page_number, offset = get_page_number_and_offset(base_rid)

        # indirection aka previous tail RID
        previous_tail_rid = self.arr_of_base_pages[page_number].get(offset, 0)

        new_tail_rid = create_rid(
            1,
            self.range_number,
            self.tail_page_number,
            self.arr_of_tail_pages[self.tail_page_number].get_next_rec_num()
        ) # creates a tail_rid

        record = Record(new_tail_rid, -1, columns)
         # set the new tail record data
        new_schema = 0
        if previous_tail_rid == 0:
            new_schema = self.arr_of_tail_pages[self.tail_page_number].update(base_rid, record)
        else:
            new_schema = self.arr_of_tail_pages[self.tail_page_number].tail_update(
                base_rid, self.get_tail_withRID(previous_tail_rid), record
            )

        # set base record indirection to new tail page rid
        self.arr_of_base_pages[page_number].set(offset, new_tail_rid, 0)
        self.arr_of_base_pages[page_number].set(offset, new_schema, 3)

        self.arr_of_base_pages[page_number].num_updates += 1
        # Merge each base page only
        if (self.arr_of_base_pages[page_number].num_records >= 511 and 
            self.arr_of_base_pages[page_number].num_updates >= MERGE_BASE_AFTER and
            not self.arr_of_base_pages[page_number].merging):
            # start merging
            self.merge(page_number)


        """
        # Merge page range
        if self.num_updates >= MERGE_BASE_AFTER:
            self.merge()
        else:
            self.num_updates += 1
        """

    def open(self):
        data = bufferpool.shared.read_metadata(self.page_range_path + '.metadata')
        if data is not None:
            self.num_of_columns = data[0]
            self.base_page_number = data[1]
            self.tail_page_number = data[2]
            self.num_records = data[3]

            for page_number, page_data in enumerate(data[4]):
                self.arr_of_base_pages.append(BasePage(
                        self.num_of_columns,
                        self.page_range_path + '_b/base_' + str(page_number),
                        page_data[0],   # num_records
                        page_data[1],   # tps
                        page_data[2],   # num_updates, merge each base page
                ))

            for page_number, num_records in enumerate(data[5]):
                self.arr_of_tail_pages.append(BasePage(
                    self.num_of_columns,
                    self.page_range_path + '_t/tail_' + str(page_number),
                    num_records
                ))

#           self.num_updates = data[6] # Merge page range

    def close(self):
        bufferpool.shared.write_metadata(self.page_range_path + '.metadata', (
            self.num_of_columns,
            self.base_page_number,
            self.tail_page_number,
            self.num_records,
            [(base_page.num_records, base_page.tps, base_page.num_updates) for base_page in self.arr_of_base_pages],
            [tail_page.num_records for tail_page in self.arr_of_tail_pages]

#           self.num_updates # Merge page range
        ))

    # Merge each base page
    def merge(self, page_number):
        if 0 <= page_number <= self.base_page_number:
            self.arr_of_base_pages[page_number].num_updates = 0
            self.arr_of_base_pages[page_number].merging = True
            self.merge_worker.queue.put(
                (self.arr_of_base_pages[page_number], copy.deepcopy(self.arr_of_tail_pages))
            )
#           self.merge_worker.queue.put(
#               (self.arr_of_base_pages[page_number], self.arr_of_tail_pages)
#           )
