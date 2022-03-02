import unittest
from lstore.db import Database
from lstore.table import Table
from lstore.query import Query
import time
import random
import shutil
import sys
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
import copy

db = Database()
db.open('./database')
table = db.create_table('test', 5, 0)

transactions = []

for i in range(100):
	transactions.append(Transaction())
	xxx.append(i)

for i in range(1000):
	q = Query(table)
	t = transactions[i % 100]
#	t.add_query(q.select, table, i, 0, [1, 1, 1, 1, 1])
	t.add_query(q.insert, table, [i * 1, i * 2, i * 3, i * 4, i * 5])

transaction_workers = []
for i in range(10):
	transaction_workers.append(TransactionWorker())

for i in range(100):
	transaction_workers[i % 10].add_transaction(transactions[i])

for i in range(10):
	transaction_workers[i].run()
	print('thread {} is running'.format(i))

db.close()