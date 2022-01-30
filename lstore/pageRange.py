from lstore.page import Page
class PageRange:
    def __init__(self, columns):
        self.num_of_colmuns = columns + 4
        self.ARR_of_base_pages=[]
        self.num_tails=0
        self.create_a_new_base_page(True)

        # What I have done PC
        # page range is full if we have more than 6 base pages
        # no reason for this it is arbitary we can change this at a whim tbh whatever
        # we want
        # this will probabaly be used one level up 
    def PageRange_has_capacity(self):
        return len(self.ARR_of_base_pages) < 6
    
    #PC
    # this basically checks if the base page has room for new info again probably
    # used by the next upper abstraction to check if there is room and then creation of another
    # set of base pages
    # Can also check if tail pages are full just pass the len of the array might
    # include a function for that too for more abstraction / encapsulation if needed
    def Physical_page_is_full(self, Base_Page_number):
        return self.ARR_of_base_pages[Base_Page_number][0].has_capacity()

    # PC
    # this creates a base page of num_colums given by user plus the preset columns that we need
    # see on top of table as to what those 4 are tbh 
    # this creates that base page and slaps it behind any other base pages
    # and behind any created tail pages or if we need a TAIL
    # pass it true and it will create a tail
    def create_a_new_base_page(self, ISTAIL=False):
        base_page=[]
        for col_num in range(self.num_of_colmuns):
            base_page.append(Page())
        if ISTAIL:
            self.ARR_of_base_pages.append(base_page)
            self.num_tails += 1
        else:
            self.ARR_of_base_pages.insert(len(self.ARR_of_base_pages)-self.num_tails, base_page)
        pass

    # UNDERCONSTRUCTION 
    # probaly needs our RID schema first before i would even bother with this tbh

    # this kind of works as shown in the myTester but not the best implmentation yet probably
    def write(self, Base_num, rec):
        temprec=self.ARR_of_base_pages[Base_num]
        i=0
        for x in range(4,self.num_of_colmuns):
            temprec[x].write(rec[i])
            i +=1
        pass

    #UNDERCONSTRUCTION
    # lol also need some more thought on how to return more records

    # alsoooo works but not too sure if it is the best way to do it
    def GET_record(self, Base_num, col_num, arr_off_set):
        temprec=self.ARR_of_base_pages[Base_num]
        target_rec=[]
        for x in col_num:
            target_rec.append(temprec[x].get(arr_off_set))
        return target_rec

    # oof
    def update():
        pass