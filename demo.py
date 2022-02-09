from lstore.db import Database
from lstore.table import Table
from lstore.query import Query
import os
table = Database().create_table('Test', 3, 0)
query = Query(table)

query.insert(1, 2, 3)

table # show first record
x=os.system('clear')

query.insert(4, 5, 6)

table # show second record
x=os.system('clear')

query.update(1, 4, 5, 6)  # show False because 4 already exists

query.update(1, 7, 8, 9)

table # show schema is 111, show new tail page, show indirection
x=os.system('clear')

query.update(4, None, None, -1)

table # show second record has new tail record, schema is 001
x=os.system('clear')

query.select(-1, 2, [1, 1, 1]) # find all record where the third col is -1

query.insert(-1, -2, -1)

table # show third record
x=os.system('clear')

query.select(-1, 2, [1, 1, 1]) # two records

query.delete(-1) # Delete last record

query.delete(-2) # Delete a invalid record -> False

table # show the third records has indir 200000000
x=os.system('clear')

query.sum(-100, 100, 2) # 8

query.sum(-100, 100, 1) # 13 

query.increment(7, 1)
