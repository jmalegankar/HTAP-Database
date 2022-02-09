from lstore.basepage import BasePage
from lstore.record import Record
from lstore.parser import *

class PageRange:

    __slots__ = 'num_of_columns', 'arr_of_base_pages', 'arr_of_tail_pages', 'range_number', 'base_page_number', 'tail_page_number', 'num_records'

    def __init__(self, columns, range_number):
        self.num_of_columns = columns
        self.arr_of_base_pages=[]
        self.arr_of_tail_pages=[]
        self.range_number = range_number
        self.base_page_number = -1
        self.tail_page_number = -1
        self.num_records = 0

    """
    Debug Only
    """

    def __str__(self):
        string = 'Number: {}, Base: {}/8, tail: {}\n'.format(self.range_number, self.base_page_number + 1, self.tail_page_number + 1)
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
        return len(self.arr_of_base_pages) < 8
    
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
            self.arr_of_tail_pages.append(BasePage(self.num_of_columns))
            self.tail_page_number += 1
        else:
            assert self.pageRange_has_capacity()
            self.arr_of_base_pages.append(BasePage(self.num_of_columns))
            self.base_page_number += 1

    """
    Write a record, given the columns data
    Return Record ID for table to build the index
    """

    def write(self, *columns):
        assert len(columns) == self.num_of_columns
        # Data OK, create new RID for this record
        if self.is_page_full(self.base_page_number):
            self.create_a_new_page()
    
        rid = create_rid(0, self.range_number, self.base_page_number, self.arr_of_base_pages[self.base_page_number].get_next_rec_num())
        record = Record(rid, -1, list(columns))
        self.arr_of_base_pages[self.base_page_number].write(record)
        self.num_records += 1
        return rid

    """
    Get a record given page_number and offset
    """
    
    def get(self, page_number, offset, Q_col=None, isTail=False):
        if isTail:
            if Q_col == None:
                return self.arr_of_tail_pages[page_number].get_user_cols(offset)
            else:
                return self.arr_of_tail_pages[page_number].get_cols(offset, Q_col)
        else:
            return self.traverse_ind(self.arr_of_base_pages[page_number].get_cols(offset, Q_col),
                                    Q_col,
                                    self.arr_of_base_pages[page_number].get(offset, 0),
                                    self.arr_of_base_pages[page_number].get(offset, 3)
                                    )
            
            
    def traverse_ind(self, base_record, Q_col, indirection, base_schema):
        if indirection == 200000000: # we set indirection to 200000000 when we deleted the record
            return None
    
        if indirection != 0:
            schema = [int(i) for i in bin(base_schema)[2:].zfill(self.num_of_columns)] # .zfill(self.num_of_columns)
            updated_record = self.get_withRID(indirection, Q_col, True)

            for index, change in enumerate(schema):
                if change == 1 and (Q_col == None or Q_col[index] == 1):
                    base_record[index] = updated_record[index]

        return base_record

    
    def check_schema(self, schema):
        return list(bytes(schema)) == (2 ** self.num_of_columns)-1

    """
    Get a record given RID
    If query columns (Q_col) is None, return all
    """

    def get_withRID(self, rid, Q_col=None, isTail=False):
        page_number, offset = get_page_number_and_offset(rid)
        return self.get(page_number, offset, Q_col, isTail)

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
        indirection = self.arr_of_base_pages[base_page_number].get_and_set(base_offset, 200000000, 0) # set indir to 200000000
        
        while indirection != 200000000:
            if get_page_type(indirection) == 1:
                tail_page_number, tail_offset = get_page_number_and_offset(indirection)
                indirection = self.arr_of_tail_pages[tail_page_number].get_and_set(tail_offset, 200000000, 0)
            else:
                break


    # adds a tail page, return the tail id of the new page
    # needs a base RID to update and columns of data.
    # columns =[1,43,None,34]
    # None for not wanting to update
    def update(self, base_rid, *columns):
        assert len(columns) == self.num_of_columns

        if self.is_page_full(self.tail_page_number, True):
            self.create_a_new_page(True)

        page_number, offset = get_page_number_and_offset(base_rid)

        previous_tail_rid = self.arr_of_base_pages[page_number].get(offset, 0) # indirection aka previous tail RID
        new_tail_rid = create_rid(1, self.range_number, self.tail_page_number, self.arr_of_tail_pages[self.tail_page_number].get_next_rec_num()) # creates a tail_rid


        record = Record(new_tail_rid, -1, list(columns))
         # set the new tail record data
        new_schema = 0
        if previous_tail_rid == 0:
            new_schema = self.arr_of_tail_pages[self.tail_page_number].update(base_rid, record)
        else:
            new_schema = self.arr_of_tail_pages[self.tail_page_number].tail_update(self.get_tail_withRID(previous_tail_rid), record)

        # set base record indirection to new tail page rid
        self.arr_of_base_pages[page_number].set(offset, new_tail_rid, 0)
        self.arr_of_base_pages[page_number].set(offset, new_schema, 3)
