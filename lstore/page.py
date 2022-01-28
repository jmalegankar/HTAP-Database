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
# import struct

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        pass

    # Patrick
    # Possible need for future implmentation
    # This stores int values greater than 255. as bytearray
    # can only store 0 < x < 256 integer in an index
    # I am assuming we do no need to store ints bigger than this 
    # so for now we only need the "else" part tbh
    # need a differnet implmentation for strings maybe 
    # have not looked into it

    # The question: When storing values shouls we use a fixed length or
    # a variable length? 
    # because the student ID given is 92106429 + 1000 in the tester which
    # at max is a 32 bit int

    #def write(self, value):
    #    if value > 255:
    #        value=(self.intTobit(value)).lstrip(b'\x00')
    #        for eightBit in value:
    #            self.data[self.num_records]=eightBit
    #            self.num_records += 1
    #    else:
    #       self.data[self.num_records]=value
    #       self.num_records += 1 
    #    pass


    def write(self, value):
        self.data[self.num_records]=value
        self.num_records += 1
        pass

    # turns number into a string of bits for function commented out above
    #def intTobit(intNumber):
    #    byteSTR = struct.pack('>Q',intNumber)
    #    return byteSTR

