from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from time import process_time, sleep, time
import sys
import random
import shutil


number_of_transactions = 100
num_threads = 8

def insert_test():
	#
	# TEST INSERT
	#
	random.seed(1234567890)
	try:
		shutil.rmtree('./speed_test')
	except:
		pass

	db = Database()
	db.open('./speed_test')
	table = db.create_table('test1', 5, 0)

	insert_transactions = []
	
	for i in range(number_of_transactions):
		insert_transactions.append(Transaction())

	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)

	for i, key in enumerate(keys):
		t = insert_transactions[i % number_of_transactions]
		t.add_query(query.insert, table, key, key * 2, key * 3, key * 4, key * 5)


	transaction_workers = []
	for i in range(num_threads):
		transaction_workers.append(TransactionWorker())
		
	for i in range(number_of_transactions):
		transaction_workers[i % num_threads].add_transaction(insert_transactions[i])

	start_time = time()
	# run transaction workers
	for i in range(num_threads):
		transaction_workers[i].run()
		
	# wait for workers to finish
	for i in range(num_threads):
		transaction_workers[i].join()
	end_time = time()
	
#	print(end_time - start_time)

	for i in range(num_threads):
		if transaction_workers[i].result != len(transaction_workers[i].transactions):
			print('Something is wrong with transaction_workers', i, transaction_workers[i].result, 'vs', len(transaction_workers[i].transactions))

	for key in keys:
		record = query.select(key, 0, [1, 1, 1, 1, 1])[0]

		if record.columns != [key, key * 2, key * 3, key * 4, key * 5]:
			print('select error on', key, ':', record, ', correct:', [key, key * 2, key * 3, key * 4, key * 5])

	db.close()
	del db, table, query

	print('INSERT FINISHED')

	#
	# TEST SELECT
	#
	random.seed(1234567890)
	db = Database()
	db.open('./speed_test')
	table = db.get_table('test1')
	
	insert_transactions = []
	
	for i in range(number_of_transactions):
		insert_transactions.append(Transaction())
		
	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)
	
	for i, key in enumerate(keys):
		t = insert_transactions[i % number_of_transactions]
		t.add_query(query.select, table, key, 0, [1] * 5)
		
		
	transaction_workers = []
	for i in range(num_threads):
		transaction_workers.append(TransactionWorker())
		
	for i in range(number_of_transactions):
		transaction_workers[i % num_threads].add_transaction(insert_transactions[i])
		
	start_time = time()
	# run transaction workers
	for i in range(num_threads):
		transaction_workers[i].run()
		
	# wait for workers to finish
	for i in range(num_threads):
		transaction_workers[i].join()
	end_time = time()
	
	print(end_time - start_time)

	for i in range(num_threads):
		if transaction_workers[i].result != len(transaction_workers[i].transactions):
			print('Something is wrong with transaction_workers', i, transaction_workers[i].result, 'vs', len(transaction_workers[i].transactions))
	
	db.close()
	del db, table, query

	print('SELECT FINISHED')
	return

	#
	# TEST UPDATE
	#
	
	random.seed(1234567890)
		
	db = Database()
	db.open('./speed_test')
	table = db.get_table('test1')
	
	insert_transactions = []
	
	for i in range(number_of_transactions):
		insert_transactions.append(Transaction())
		
	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)
	
	for i, key in enumerate(keys):
		t = insert_transactions[i % number_of_transactions]
		t.add_query(query.update, table, key, *[None, key * -2, key * -3, key * -4, key * -5])

	transaction_workers = []
	for i in range(num_threads):
		transaction_workers.append(TransactionWorker())
		
	for i in range(number_of_transactions):
		transaction_workers[i % num_threads].add_transaction(insert_transactions[i])
		
	start_time = time()
	# run transaction workers
	for i in range(num_threads):
		transaction_workers[i].run()
		
	# wait for workers to finish
	for i in range(num_threads):
		transaction_workers[i].join()
	end_time = time()
	
#	print(end_time - start_time)
	
	for i in range(num_threads):
		if transaction_workers[i].result != len(transaction_workers[i].transactions):
			print('Something is wrong with transaction_workers', i, transaction_workers[i].result, 'vs', len(transaction_workers[i].transactions))
			
	for key in keys:
		record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
		
		if record.columns != [key, key * -2, key * -3, key * -4, key * -5]:
			print('select error on', key, ':', record, ', correct:', [key, key * -2, key * -3, key * -4, key * -5])
			
	db.close()
	del db, table, query

if __name__ == '__main__':
	insert_test()
	