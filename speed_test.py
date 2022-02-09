from lstore.db import Database
from lstore.query import Query
from time import process_time
import sys

db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

insert_time_0 = process_time()
for i in range(0, 10000):
	query.insert(906659671 + i, 93, 0, 0, 0)
	keys.append(906659671 + i)
insert_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

query.update(906659671, None, None, None, None, 1)

for i in range(0, 10000):
	query.update(906659671, None, i + 1, None, None, None)
	
select_one_0 = process_time()
print(query.select(906659671, 0, [1,1,1,1,1]))
select_one_1 = process_time()
print("Select 1 record after 10k update took:  \t\t\t", select_one_1 - select_one_0)