from lstore.db import Database
from lstore.table import Table
from lstore.query import Query
table = Database().create_table('Test', 3, 0)
query = Query(table)

# insert 100k items
for i in range(100000):
	_ = query.insert(i, i+1, i+2)

# find all records where the first column is 10000
query.select(10000, 0, [1,1,1])

# find all records where the second column is 10000
# fast thanks to index all columns
query.select(10000, 1, [1,1,1])

# find all records where the third column is 10000
# fast thanks to index all columns
query.select(10000, 2, [1,1,1])

# change the second column to -1
query.update(0, None, -1, None)

# select record where the primary key is 0
query.select(0, 0, [1,1,1])

# update 100k time
for i in range(100000):
	_ = query.update(0, None, None, i)

# fast thanks to cumulative tail record
query.select(0, 0, [1,1,1])