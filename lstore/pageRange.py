from lstore.basepage import BasePage
from lstore.table import Record
from lstore.parser import *

class PageRange:

    def __init__(self, columns, range_number):
        self.num_of_columns = columns
        self.arr_of_base_pages=[]
        self.arr_of_tail_pages=[]
        self.range_number = range_number
        self.base_page_number = -1
        self.tail_page_number = -1

    def __str__(self):
        return "PageRange > " + str(self.base_page_number + 1) + " base pages, " + str(self.tail_page_number + 1) + " tail pages"

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
        return rid

    """
    Get a record given page_number and offset
    """
    
    def get(self, page_number, offset, isTail=False):
        if isTail:
            return self.arr_of_tail_pages[page_number].get_user_cols(offset)
        else:
            return self.arr_of_base_pages[page_number].get_user_cols(offset)
            # my start on indirection traverse
            #rec=self.arr_of_base_pages[page_number].get_user_cols(offset)
            #return self.traverse_tail(rec)
    
    
    """
    Get a record given RID
    """

    def get_withRID(self, rid, isTail=False):
        page_number = get_page_number(rid)
        offset = get_physical_page_offset(rid)
        return self.get(page_number, offset, isTail)

    # oof
    def update(self, rid, *columns):
        assert len(columns) == self.num_of_columns
        # Data OK, create new RID for this record

        if self.is_page_full(self.tail_page_number, True):
            self.create_a_new_page(True)


        # i assumed type was the type of page so 1 for tail
        tail_rid = create_rid(1, self.range_number, self.tail_page_number, self.arr_of_tail_pages[self.tail_page_number].get_next_rec_num())
        record = Record(tail_rid, -1, list(columns))

        # need page number and offset to access old rid of basepage and set a new one
        page_number = get_page_number(rid)
        offset = get_physical_page_offset(rid)

        self.arr_of_tail_pages[self.tail_page_number].update(self.arr_of_base_pages[page_number].get(offset,0),record)
        self.arr_of_base_pages[page_number].set(offset,tail_rid,0) # set base page to new tail page rid

        return tail_rid
        