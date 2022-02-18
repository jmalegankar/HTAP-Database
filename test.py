from lstore.db import Database
from lstore.table import Table
from lstore.query import Query
from lstore.pageRange import PageRange
from lstore.basepage import BasePage
from lstore.record import Record
from lstore.page import Page
from lstore.parser import *

db = Database()
db.open('./test')
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

#query.insert(1, 2, 3, 4, 5)
#query.insert(5, 4, 3, 2, 1)
print(query.select(1, 0, [0, 0, 0, 0, 1]))

db.merge_worker.queue.put(([1], [2]))
db.merge_worker.queue.put(([10], [20]))

db.close()