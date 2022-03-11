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
import shutil



try:
	shutil.rmtree('./database')
except:
	pass
	
db = Database()
db.open('./database')
table = db.create_table('test', 10, 0)

transaction_workers = []
transactions = []

for i in range(100):
	transactions.append(Transaction())

for i in range(10000):
	q = Query(table)
	t = transactions[i % 100]
	t.add_query(q.insert, table, *[i * 1, i * 2, i * 3, i * 4, i * 5, i * 6, i * 7, i * 8, i * 9, i * 10])

for i in range(10):
	transaction_workers.append(TransactionWorker())

for i in range(100):
	transaction_workers[i % 10].add_transaction(transactions[i])

for i in range(10):
	transaction_workers[i].run()

for i in range(10):
	transaction_workers[i].join()

for i in range(10):
	if transaction_workers[i].result != len(transaction_workers[i].transactions):
		print('[A] Something is wrong with transaction_workers', i, transaction_workers[i].result, 'vs', len(transaction_workers[i].transactions))


print('Select after insert')

q = Query(table)
for i in range(10000):
	result = q.select(i, 0, [1] * 10)
	if result is not None and len(result) > 0:
		if result[0].columns != [i, i * 2, i * 3, i * 4, i * 5, i * 6, i * 7, i * 8, i * 9, i * 10]:
			print(i, 'is incorrect, got', result[0], 'expect', [i, i * 2, i * 3, i * 4, i * 5, i * 6, i * 7, i * 8, i * 9, i * 10])
	else:
		print(i, 'is incorrect, got', result, 'expect', [i, i * 2, i * 3, i * 4, i * 5, i * 6, i * 7, i * 8, i * 9, i * 10])


print('Update')


transaction_workers = []
transactions = []

for i in range(100):
	transactions.append(Transaction())

for i in range(10000):
	q = Query(table)
	t = transactions[i % 100]
	t.add_query(q.update, table, i, *[None, -2 * i, -3 * i, -4 * i, -5 * i, i * -6, i * -7, i * -8, i * -9, i * -10])

for i in range(10):
	transaction_workers.append(TransactionWorker())

for i in range(100):
	transaction_workers[i % 10].add_transaction(transactions[i])

for i in range(10):
	transaction_workers[i].run()
	
for i in range(10):
	transaction_workers[i].join()
	
for i in range(10):
	if transaction_workers[i].result != len(transaction_workers[i].transactions):
		print('[B] Something is wrong with transaction_workers', i, transaction_workers[i].result, 'vs', len(transaction_workers[i].transactions))


print('Select after update')

q = Query(table)
for i in range(10000):
	result = q.select(i, 0, [1] * 10)
	if result is not None and len(result) > 0:
		if result[0].columns != [i, i * -2, i * -3, i * -4, i * -5, i * -6, i * -7, i * -8, i * -9, i * -10]:
			print(i, 'is incorrect, got', result[0], 'expect', [i, i * -2, i * -3, i * -4, i * -5, i * -6, i * -7, i * -8, i * -9, i * -10])
	else:
		print(i, 'is incorrect, got', result, 'expect', [i, i * -2, i * -3, i * -4, i * -5, i * -6, i * -7, i * -8, i * -9, i * -10])


print('Test abort')

worker1 = TransactionWorker()
worker2 = TransactionWorker()

t1 = Transaction()
t2 = Transaction()

q = Query(table)
for i in range(10000):
	t1.add_query(q.select, table, i, 0, [1] * 10)

	if i == 5000:
		t2.add_query(q.update, table, 0, *[None, None, None, None, 1, None, None, None, None, None])
	else:
		if i > 0:
			t2.add_query(q.insert, table, *[-1 * i, 2 * i, 3 * i, 4 * i, 5 * i, i * -6, i * -7, i * -8, i * -9, i * -10])

worker1.add_transaction(t1)
worker2.add_transaction(t2)

worker1.run()
worker2.run()

worker1.join()
worker2.join()


q = Query(table)
for i in range(10000):
	result = q.select(-1 * i, 0, [1] * 10)
	if result is not None and len(result) > 0:
		print(i)
	else:
		break


transaction_workers = []
transactions = []

for i in range(100):
	transactions.append(Transaction())
	
for i in range(10000):
	q = Query(table)
	t = transactions[i % 100]
	t.add_query(q.update, table, i, *[None, 0, 0, 0, 0, 0, 0, 0, 0, 0])

for i in range(10):
	transaction_workers.append(TransactionWorker())
	
for i in range(100):
	transactions[i].add_query(q.update, table, -1, *[0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
	transaction_workers[i % 10].add_transaction(transactions[i])
	
for i in range(10):
	transaction_workers[i].run()
	
for i in range(10):
	transaction_workers[i].join()
	
for i in range(10):
	if transaction_workers[i].result != len(transaction_workers[i].transactions):
		print('[C] Something is wrong with transaction_workers', i, transaction_workers[i].result, 'vs', len(transaction_workers[i].transactions))

q = Query(table)
for i in range(10000):
	result = q.select(i, 0, [1] * 10)
	if result is not None and len(result) > 0:
		if result[0].columns != [i, i * -2, i * -3, i * -4, i * -5, i * -6, i * -7, i * -8, i * -9, i * -10]:
			print(i, 'is incorrect, got', result[0], 'expect', [i, i * -2, i * -3, i * -4, i * -5, i * -6, i * -7, i * -8, i * -9, i * -10])
	else:
		print(i, 'is incorrect, got', result, 'expect', [i, i * -2, i * -3, i * -4, i * -5, i * -6, i * -7, i * -8, i * -9, i * -10])

db.close()
