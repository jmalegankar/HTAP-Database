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

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        pass

    def write(self, value):
        self.num_records += 1
        pass

