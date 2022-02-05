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

    """
    Debug Only
    """

    def __str__(self):
        string = 'Base: {}/8, tail: {}\n'.format(self.base_page_number + 1, self.tail_page_number + 1)
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
        return rid

    """
    Get a record given page_number and offset
    """
    
    def get(self, page_number, offset, isTail=False):
        if isTail:
            return self.arr_of_tail_pages[page_number].get_user_cols(offset)
        else:
            # return self.arr_of_base_pages[page_number].get_user_cols(offset)
            # still does the same thing as above if there is no update
            # if there is an update follow the indir
            # sorry kevin for the amount of characters in this one line lol
            return self.traverse_ind(self.arr_of_base_pages[page_number].get_user_cols(offset),
                                    [0]* self.num_of_columns,
                                    self.arr_of_base_pages[page_number].get(offset,0)
                                    )
            
    # some recursion not very good recursion lol
    # basically take in the schema and rec of the prev caller and the next_rid
    # which is the current recs rid
    # the update the rec as needed based on schema

    def traverse_ind(self, rec, schema, next_rid):
        # base case if next rid is last tail it should point 0
        # THIS wil change when we decide out indirection default!
        # or if schema is updated
        if next_rid==0 or self.check_schema(schema):
            return rec

        next_schema=[int(i) for i in list('{0:0b}'.format(self.arr_of_tail_pages[get_page_number(next_rid)].get(get_physical_page_offset(next_rid),3)))]

        next_rec=self.get_withRID(next_rid,True)

        next_rid=self.arr_of_tail_pages[get_page_number(next_rid)].get(get_physical_page_offset(next_rid),0)

        for count,change in enumerate(next_schema):
            if change==1 and schema[count]!=1:
                schema[count]=1
                rec[count]=next_rec[count]
                print(next_rec[count])

        print(schema)
        return self.traverse_ind(rec,schema, next_rid)
        #for check in list(next_rec[3]):

    
    def check_schema(self, schema):
        return list(bytes(schema)) == (2 ** self.num_of_columns)-1
    """
    Get a record given RID
    """

    def get_withRID(self, rid, isTail=False):
        page_number = get_page_number(rid)
        offset = get_physical_page_offset(rid)
        return self.get(page_number, offset, isTail)

    # adds a tail page, return the tail id of the new page
    # needs a base RID to update and columns of data.
    # columns =[1,43,None,34]
    # None for not wanting to update
    def update(self, rid, *columns):
        assert len(columns) == self.num_of_columns

        if self.is_page_full(self.tail_page_number, True):
            self.create_a_new_page(True)


        # creates a tail_rid
        tail_rid = create_rid(1, self.range_number, self.tail_page_number, self.arr_of_tail_pages[self.tail_page_number].get_next_rec_num())
        record = Record(tail_rid, -1, list(columns))

        # grab the base id's location
        page_number = get_page_number(rid)
        offset = get_physical_page_offset(rid)

        # set the new tail record data
        self.arr_of_tail_pages[self.tail_page_number].update(self.arr_of_base_pages[page_number].get(offset,0),record)

        # set base page to new tail page rid
        self.arr_of_base_pages[page_number].set(offset,tail_rid,0) 

        #Note further implmentation would include updating the time accessed maybe or renaimng this
        #function to a dif name

        return tail_rid
        