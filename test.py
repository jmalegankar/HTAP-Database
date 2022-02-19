from lstore.db import Database
from lstore.table import Table
from lstore.query import Query
from lstore.pageRange import PageRange
from lstore.basepage import BasePage
from lstore.record import Record
from lstore.page import Page
from lstore.parser import *
import threading

db = Database()
db.open('./test')
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

print('thread C', threading.get_ident())

for i in range(511):
	query.insert(i, i*2, i*3, i*4, i*5)

#query.update(1, None, -2, None, -4, None)

for i in range(511):
	if not query.update(i, None, -2*i, None, -4*i, None):
		print('error')

db.close()