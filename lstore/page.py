#Patrick's comments/thoughts

#here create pages one page per column of data. So if user
#creates table with 5 colums we should make 7 pages.
#5 for the defined column values of the user,
#1 for bit schema to tell us about updates so for this example
#maybe like 00000 where the bit flips to a one when an update is found traversing
#the indirection pointer,
#and here 1 column for indirection which, for base page points to latest update.
#the that points to previous update until a null value is reached

#Not our definite structure just my thoughts

#other structural details is as follows but not limited to:

#How do we store these pages? meaning should we define a class
#called page range which implments some data structure to store our page range?

#How many pages should we have per page range?

#For now now implment bytearray for ints IDK if we need strings as tester1 does not use it

# I am assuming we are allowed to use our own imports lol
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
    
    """
    Debug Only
    """

    def __str__(self):
        string = 'Current size: {}/512 records\n'.format(self.num_records)
        string += '=' * 5 + '\n'
        if self.num_records > 0:
            for i in range(self.num_records):
                string += '{}: {}\n'.format(i, self.get(i))
        else:
            string += 'Empty\n'
        return string

    def has_capacity(self):
        return self.num_records < 512

    # writes a record of an int up to size of 64 bits, ALL records will take up 8 spaces
    # in this implmentation we can switch to a variable length storage if we want but for now this was
    # easiest to do
    def write(self, value):
        assert self.has_capacity()
        self.data[self.num_records * 8 : self.num_records * 8 + 8] = \
        value.to_bytes(8, 'big', signed=True)
        self.num_records += 1

    # pass in the rec number we want. so rec 0 ..< 512
    # can aslso use rec offset which will probably be better tbh
    def get(self, rec_num):
        assert 0 <= rec_num < 512
        return int.from_bytes(self.data[rec_num * 8 : rec_num * 8 + 8], "big", signed=True)
        
    #pass in rec number we want to change for updates
    def set(self, rec_num, value):
        assert 0 <= rec_num < 512
        self.data[rec_num * 8 : rec_num * 8 + 8] = \
        value.to_bytes(8, 'big', signed=True)
