from lstore.db import Database
from lstore.query import Query
from time import process_time
import sys
import random
import shutil


def insert_test_setup():
	random.seed(1234567890)
	try:
		shutil.rmtree('./speed_test')
	except:
		pass


def insert_test():
	db = Database()
	db.open('./speed_test')
	table = db.create_table('test1', 5, 0)

	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)

	for key in keys:
		if not query.insert(key, key * 2, key * 3, key * 4, key * 5):
			print('insert error')
	db.close()
	del db, table, query


def select_test_setup():
	random.seed(1234567890)


def sequential_select_test():
	db = Database()
	db.open('./speed_test')
	table = db.get_table('test1')

	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)

	for key in keys:
		if query.select(key, 0, [1,1,1,1,1])[0].columns != [key, key * 2, key * 3, key * 4, key * 5]:
			print('select error')
	db.close()
	del db, table, query


def random_select_test():
	db = Database()
	db.open('./speed_test')
	table = db.get_table('test1')
	
	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)
	random.shuffle(keys)

	for key in keys:
		if query.select(key, 0, [1,1,1,1,1])[0].columns != [key, key * 2, key * 3, key * 4, key * 5]:
			print('select error')
	db.close()
	del db, table, query


def insert_select_test_setup():
	random.seed(1234567890)
	try:
		shutil.rmtree('./speed_test2')
	except:
		pass


def insert_sequential_select_test():
	db = Database()
	db.open('./speed_test2')
	table = db.create_table('test1', 5, 0)
	
	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)
	
	for key in keys:
		if not query.insert(key, key * 2, key * 3, key * 4, key * 5):
			print('insert error')
			
	for key in keys:
		if query.select(key, 0, [1,1,1,1,1])[0].columns != [key, key * 2, key * 3, key * 4, key * 5]:
			print('select error')
	db.close()
	del db, table, query


def insert_random_select_test():
	db = Database()
	db.open('./speed_test2')
	table = db.create_table('test1', 5, 0)
	
	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)
	random.shuffle(keys)
	
	for key in keys:
		if not query.insert(key, key * 2, key * 3, key * 4, key * 5):
			print('insert error')
			
	for key in keys:
		if query.select(key, 0, [1,1,1,1,1])[0].columns != [key, key * 2, key * 3, key * 4, key * 5]:
			print('select error')
	db.close()
	del db, table, query


def update_test_setup():
	random.seed(1234567890)
	try:
		shutil.rmtree('./speed_test')
	except:
		pass
	db = Database()
	db.open('./speed_test')
	table = db.create_table('test1', 5, 0)
	
	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)
	
	for key in keys:
		query.insert(key, key * 2, key * 3, key * 4, key * 5)
	db.close()
	random.seed(1234567890)


def update_test():
	db = Database()
	db.open('./speed_test')
	table = db.get_table('test1')
	
	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)

	for key in keys:
		cols = random.sample(range(1, 5), random.randint(1, 4))
		update_cols = [None] * 5
		for col in cols:
			update_cols[col] = random.randint(-100000000, 100000000)

		if not query.update(key, *update_cols):
			print('update error')
	db.close()
	del db, table, query

def update_random_test():
	db = Database()
	db.open('./speed_test')
	table = db.get_table('test1')
	
	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)
	random.shuffle(keys)
	
	for key in keys:
		cols = random.sample(range(1, 5), random.randint(1, 4))
		update_cols = [None] * 5
		for col in cols:
			update_cols[col] = random.randint(-100000000, 100000000)
			
		if not query.update(key, *update_cols):
			print('update error')
	db.close()
	del db, table, query

def update_correctness_check():
	random.seed(1234567890)
	db = Database()
	db.open('./speed_test')
	table = db.get_table('test1')

	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)

	for key in keys:
		cols = random.sample(range(1, 5), random.randint(1, 4))
		update_cols = [key, key * 2, key * 3, key * 4, key * 5]

		for col in cols:
			update_cols[col] = random.randint(-100000000, 100000000)
		
		get = query.select(key, 0, [1, 1, 1, 1, 1])[0].columns
		if get != update_cols:
			print('get', get, 'want', update_cols)

	print(len(keys), 'keys checked!')
	db.close()


def delete_select_test_setup():
	update_test_setup()
	update_test()
	random.seed(1234567890)


def delete_select_test():
	db = Database()
	db.open('./speed_test')
	table = db.get_table('test1')
	
	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)
	
	for key in keys:
		if not query.delete(key):
			print('delete error')
	
	for key in keys:
		if query.select(key, 0, [1, 1, 1, 1, 1]) != []:
			print('delete check error')
	
	db.close()


def sum_test_setup():
	update_test_setup()
	random.seed(1234567890)
	db = Database()
	db.open('./speed_test')
	table = db.get_table('test1')
	
	query = Query(table)
	keys = random.sample(range(-100000000, 100000001), 10000)
	
	for i in range(5):
		for key in keys:
			cols = random.sample(range(1, 5), random.randint(1, 4))
			update_cols = [None] * 5
			for col in cols:
				update_cols[col] = random.randint(-100000000, 100000000)
				
			if not query.update(key, *update_cols):
				print('update error')

	db.close()
	del db, table, query

def sum_test():
	db = Database()
	db.open('./speed_test')
	table = db.get_table('test1')
	
	query = Query(table)
	query.sum(0, 100000000, random.randint(0, 4))


if __name__ == '__main__':
	repeat = 5
	import timeit
	import statistics

	tests = [
		('insert_test', 'insert_test_setup'),
		('sequential_select_test', 'select_test_setup'),
		('random_select_test', 'select_test_setup'),
		('insert_sequential_select_test', 'insert_select_test_setup'),
		('insert_random_select_test', 'insert_select_test_setup'),
		('update_test', 'update_test_setup')
	]

	for test in tests:
		results = timeit.repeat('{}()'.format(test[0]), setup='from __main__ import {}, {}; {}()'.format(test[0], test[1], test[1]), repeat=repeat, number=1)
		print('{}:'.format(test[0]))
#		print('Running {} times'.format(repeat))
#		print('Min {} seconds'.format(min(results)))
#		print('Max {} seconds'.format(max(results)))
		print('Mean {} seconds'.format(statistics.mean(results)))
#		print('Median {} seconds'.format(statistics.median(results)))
		print('=' * 30)
	update_correctness_check()
	
	tests = [
		('update_random_test', 'update_test_setup'),
		('delete_select_test', 'update_test_setup'),
		('delete_select_test', 'delete_select_test_setup'),
		('sum_test', 'update_test_setup'),
		('sum_test', 'sum_test_setup')
	]

	for test in tests:
		results = timeit.repeat('{}()'.format(test[0]), setup='from __main__ import {}, {}; {}()'.format(test[0], test[1], test[1]), repeat=repeat, number=1)
		print('{}:'.format(test[0]))
#		print('Running {} times'.format(repeat))
#		print('Min {} seconds'.format(min(results)))
#		print('Max {} seconds'.format(max(results)))
		print('Mean {} seconds'.format(statistics.mean(results)))
#		print('Median {} seconds'.format(statistics.median(results)))
		print('=' * 30)