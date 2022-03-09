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
"""

import shutil
try:
	shutil.rmtree('./database')
except:
	pass
	
db = Database()
db.open('./database')
table = db.create_table('test', 5, 0)

transactions = []

for i in range(100):
	transactions.append(Transaction())

for i in range(30000):
	q = Query(table)
	t = transactions[i % 100]
#	t.add_query(q.select, table, i, 0, [1, 1, 1, 1, 1])
	t.add_query(q.insert, table, *[i * 1, i * 2, i * 3, i * 4, i * 5])
#	t.add_query(q.delete, table, i)

transaction_workers = []
for i in range(10):
	transaction_workers.append(TransactionWorker())

for i in range(100):
	transaction_workers[i % 10].add_transaction(transactions[i])

for i in range(10):
	transaction_workers[i].run()
	print('thread {} is running'.format(i))

for i in range(10):
	transaction_workers[i].join()

for i in range(10):
	# print(transaction_workers[i].result, 'ok, we have',  len(transaction_workers[i].transactions))
	if transaction_workers[i].result != len(transaction_workers[i].transactions):
		print('[A] Something is wrong with transaction_workers', i, transaction_workers[i].result, 'vs', len(transaction_workers[i].transactions))

q = Query(table)
for i in range(30000):
	result = q.select(i, 0, [1, 1, 1, 1, 1])
	if result is not None and len(result) > 0:
		if result[0].columns != [i, i * 2, i * 3, i * 4, i * 5]:
			print(i, 'is incorrect, got', result[0], 'expect', [i, i * 2, i * 3, i * 4, i * 5])
		q.update(i, None, None, None, None, i)
	else:
		print(i, 'is incorrect, got', result, 'expect', [i, i * 2, i * 3, i * 4, i * 5])

print('OK')

db.close()



db = Database()
db.open('./database')
table = db.get_table('test')

q = Query(table)


for i in range(30000):
	result = q.select(i, 0, [1, 1, 1, 1, 1])
	if result is not None and len(result) > 0:
		if result[0].columns != [i, i * 2, i * 3, i * 4, i]:
			print(i, 'is incorrect, got', result[0], 'expect', [i, i * 2, i * 3, i * 4, i])
		if not q.update(i, -1 * i, None, None, None, None):
			print(i, 'update failed!')
	else:
		print(i, 'is incorrect, got', result, 'expect', [i, i * 2, i * 3, i * 4, i])

for i in range(30000):
	result = q.select(-1 * i, 0, [1, 1, 1, 1, 1])
	if result is not None and len(result) > 0:
		if result[0].columns != [-1 * i, i * 2, i * 3, i * 4, i]:
			print(i, 'is incorrect, got', result[0], 'expect', [-1 * i, i * 2, i * 3, i * 4, i])
	else:
		print(i, 'is incorrect, got', result, 'expect', [-1 * i, i * 2, i * 3, i * 4, i])

db.close()

"""

db = Database()
db.open('./database')
table = db.get_table('test')
db.close()
