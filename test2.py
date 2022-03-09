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
table = db.create_table('test', 5, 0)

transaction_workers = []
transactions = []

for i in range(100):
	transactions.append(Transaction())

for i in range(30000):
	q = Query(table)
	t = transactions[i % 100]
	t.add_query(q.insert, table, *[i * 1, i * 2, i * 3, i * 4, i * 5])

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

print('Updating non primary key')

transaction_workers = []
transactions = []

for i in range(100):
	transactions.append(Transaction())

q = Query(table)
for i in range(30000):
	result = q.select(i, 0, [1, 1, 1, 1, 1])
	if result is not None and len(result) > 0:
		if result[0].columns != [i, i * 2, i * 3, i * 4, i * 5]:
			print(i, 'is incorrect, got', result[0], 'expect', [i, i * 2, i * 3, i * 4, i * 5])
		t = transactions[i % 100]
		t.add_query(q.update, table, i, None, None, None, None, i)
	else:
		print(i, 'is incorrect, got', result, 'expect', [i, i * 2, i * 3, i * 4, i * 5])
	
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

db.close()

print('Updating primary key')


db = Database()
db.open('./database')
table = db.get_table('test')

q = Query(table)

transaction_workers = []
transactions = []

for i in range(100):
	transactions.append(Transaction())

for i in range(30000):
	result = q.select(i, 0, [1, 1, 1, 1, 1])
	if result is not None and len(result) > 0:
		if result[0].columns != [i, i * 2, i * 3, i * 4, i]:
			print(i, 'is incorrect, got', result[0], 'expect', [i, i * 2, i * 3, i * 4, i])
		t = transactions[i % 100]
		if i > 0:
			t.add_query(q.update, table, i,-1 * i, None, None, None, None)
	else:
		print(i, 'is incorrect, got', result, 'expect', [i, i * 2, i * 3, i * 4, i])


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
		print('[C] Something is wrong with transaction_workers', i, transaction_workers[i].result, 'vs', len(transaction_workers[i].transactions))
		

for i in range(30000):
	result = q.select(-1 * i, 0, [1, 1, 1, 1, 1])
	if result is not None and len(result) > 0:
		if result[0].columns != [-1 * i, i * 2, i * 3, i * 4, i]:
			print(i, 'is incorrect, got', result[0], 'expect', [-1 * i, i * 2, i * 3, i * 4, i])
	else:
		print(i, 'is incorrect, got', result, 'expect', [-1 * i, i * 2, i * 3, i * 4, i])

db.close()



db = Database()
db.open('./database')
table = db.get_table('test')

q = Query(table)

print('Last check!')

for i in range(30000):
	result = q.select(-1 * i, 0, [1, 1, 1, 1, 1])
	if result is not None and len(result) > 0:
		if result[0].columns != [-1 * i, i * 2, i * 3, i * 4, i]:
			print(i, 'is incorrect, got', result[0], 'expect', [-1 * i, i * 2, i * 3, i * 4, i])
	else:
		print(i, 'is incorrect, got', result, 'expect', [-1 * i, i * 2, i * 3, i * 4, i])

db.close()
