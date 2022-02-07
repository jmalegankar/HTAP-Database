from lstore.db import Database
from lstore.table import Table
from lstore.query import Query
from lstore.pageRange import PageRange
from lstore.basepage import BasePage
from lstore.record import Record
from lstore.page import Page
from lstore.parser import *
"""
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

for i in range(10000):
	query.insert(i, i+1, i+2, i+3, i+4)

#print(query.insert(0, 1, 2, 3, 4))
#print(query.insert(-1, 1, 2, 3, 4))
#print(query.select(0, 1, [1] * 5))
#
#
#print(query.select(5000, 2, [1] * 5))
#print(query.delete(4998))
#print(query.select(5000, 2, [1] * 5))
total = 0
for i in range(500, 511):
	total += (i + 1)
print(total)
print(query.sum(500, 510, 1))
"""

table2 = Database().create_table('Test2', 3, 0)
query2 = Query(table2)

query2.insert(100, 200, 300)
query2.insert(1000, 2000, 3000)
print(query2.sum(100, 150, 2) == 300)

query2.update(1000, None, 1000, None)
print(query2.sum(100, 1000, 2) == 3300)

query2.update(1000, None, None, 2000)
print(query2.sum(100, 1000, 2) == 2300)

query2.update(1000, 3000, None, None)

print(query2.sum(100, 1000, 2) == 300)
print(query2.sum(100, 3000, 2) == 2300)