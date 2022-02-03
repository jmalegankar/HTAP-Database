from lstore.basepage import BasePage
from lstore.tabel import Record
from lstore.parser import create_rid

class PageRange:
    def __init__(self, columns):
        self.num_of_columns = columns
        self.arr_of_base_pages=[]
        self.arr_of_tail_pages=[]
        # self.create_a_page(True)

        """
    Checks if page range has capacity, which we need to define 8????
        """
    def pageRange_has_capacity(self):
        return len(self.arr_of_base_pages) < 8
    
        """
    
        """
    def is_page_full(self, page_number, isTail=False):
        if isTail:
                return self.arr_of_tail_pages[page_number].has_capacity()
        else:
            return self.arr_of_base_pages[page_number].has_capacity()
          
        """
    creates a base page or tail page
        """

    def create_a_new_page(self, isTail=False):
        if isTail:
            self.arr_of_tail_pages.append(BasePage(self.num_of_columns))
            return len(self.arr_of_tail_pages) - 1
        else:
            assert self.pageRange_has_capacity()
            self.arr_of_base_pages.append(BasePage(self.num_of_columns))
            return len(self.arr_of_base_pages) - 1

    # UNDERCONSTRUCTION 
    # probaly needs our RID schema first before i would even bother with this tbh

    # this kind of works as shown in the myTester but not the best implmentation yet probably
    def write(self, *colunms):
                assert len(columns) == self.num_of_columns
        pass

    #UNDERCONSTRUCTION
    # lol also need some more thought on how to return more records

    # alsoooo works but not too sure if it is the best way to do it
    def get_record(self, base_num, col_num, arr_off_set):
            pass

    # oof
    def update():
        pass
