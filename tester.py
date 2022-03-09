import unittest
from lstore.db import Database
from lstore.table import Table
from lstore.query import Query
import time
import random
import shutil
import sys

class ExtendedTester(unittest.TestCase):

	def setUp(self):
		seed = random.randrange(sys.maxsize)
		random.seed(seed)
		print(str(seed) + '\n')
		try:
			shutil.rmtree('./database')
		except:
			pass

	"""
	Insert 10k objects
	Select 10k objects in order using primary key
	Select 10k objects randomly using primary key
	Shutdown database and select 10k objects randomly using primary key
	"""
	def test_select(self):
		db = Database()
		db.open('./database')
		table = db.create_table('select', 5, 0)
		query = Query(table)

		records = []
		index = [i for i in range(10000)]
		random.shuffle(index)

		keys = random.sample(range(-100000000, 100000001), 10000)

		for key in keys:
			record = [key, random.randint(-100000000, 100000000), random.randint(-100000000, 100000000), random.randint(-100000000, 100000000), random.randint(-100000000, 100000000)]
			records.append(record)
			self.assertTrue(query.insert(*record))

		for i in index:
			correct = records[i]
			self.assertEqual(query.select(correct[0], 0, [1] * 5)[0].columns, correct)

		db.close()
		del db, table, query

		random.shuffle(index)

		db = Database()
		db.open('./database')
		table = db.get_table('select')
		query = Query(table)

		for i in index:
			correct = records[i]
			self.assertEqual(query.select(correct[0], 0, [1] * 5)[0].columns, correct)

		db.close()


	"""
	Create index for column 1, 2, 3, 4
	Insert 1k objects
	Select 1k objects in order using random colummn value
	Select 1k objects randomly using random colummn value
	Shutdown database and select 10k objects randomly using random colummn value
	"""
	def test_select_with_index(self):
		db = Database()
		db.open('./database')
		table = db.create_table('select', 5, 0)
		for i in range(1, 5):
			table.index.create_index(i)
		query = Query(table)

		records = []
		index = [i for i in range(1000)]
		random.shuffle(index)

		keys = random.sample(range(-100000000, 100000001), 1000)

		for key in keys:
			record = [key, random.randint(-100000000, 100000000), random.randint(-100000000, 100000000), random.randint(-100000000, 100000000), random.randint(-100000000, 100000000)]
			records.append(record)
			self.assertTrue(query.insert(*record))

		for i in index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = False
				for result in results:
					if result.columns == correct:
						ok = True
				self.assertTrue(ok)
			else:
				self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)
		
		db.close()
		del db, table, query
		
		random.shuffle(index)
		
		db = Database()
		db.open('./database')
		table = db.get_table('select')
		query = Query(table)

		for i in index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = False
				for result in results:
					if result.columns == correct:
						ok = True
				self.assertTrue(ok)
			else:
				self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)

		db.close()

	"""
	Create index for column 1, 2, 3, 4
	Insert 1k objects
	Select 1k objects randomly using random colummn value
	Update 1k objects randomly and select it
	Select 1k objects randomly using random column value
	Shutdown database and select 10k objects randomly using random colummn value
	Update 1k objects randomly and select it
	"""
	def test_update(self):	
		db = Database()
		db.open('./database')
		table = db.create_table('update', 5, 0)
		for i in range(1, 5):
			table.index.create_index(i)
		query = Query(table)
		
		records = []
		index = [i for i in range(1000)]
		random.shuffle(index)
		
		keys = random.sample(range(-100000000, 100000001), 10000)
		
		for key in keys:
			record = [key, random.randint(-100000000, 100000000), random.randint(-100000000, 100000000), random.randint(-100000000, 100000000), random.randint(-100000000, 100000000)]
			records.append(record)
			self.assertTrue(query.insert(*record))
		print('A')
		for i in index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = False
				for result in results:
					if result.columns == correct:
						ok = True
				self.assertTrue(ok)
			else:
				self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)
		print('B')
		random.shuffle(index)
		for i in index:
			primary_key = records[i][0]
			update = [None, None, None, None, None]
			update_bit = [False, False, False, False, False]
			# randomly select columns to update
			how_many_columns = random.randint(1, 5)
			for j in range(how_many_columns):
				update_bit[random.randint(0, 4)] = True
	
			for idx, do_update in enumerate(update_bit):
				if do_update:
					if idx == 0:
						update[idx] = records[i][idx] + 1000000000
					else:
						update[idx] = random.randint(-100000000, 100000000)
					records[i][idx] = update[idx]
			self.assertTrue(query.update(primary_key, *update))
		
			# test select after update
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = False
				for result in results:
					if result.columns == correct:
						ok = True
				self.assertTrue(ok)
			else:
				self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)
		print('C')
		# test select ALL after update
		random.shuffle(index)
		for i in index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = False
				for result in results:
					if result.columns == correct:
						ok = True
				self.assertTrue(ok)
			else:
				self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)

		db.close()
		del db, table, query
		
		random.shuffle(index)
		print('D')
		db = Database()
		db.open('./database')
		table = db.get_table('update')
		query = Query(table)
		
		for i in range(1, 5):
			table.index.create_index(i)
		print('X')
		
		for i in index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = False
				for result in results:
					if result.columns == correct:
						ok = True
				self.assertTrue(ok)
			else:
				self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)

		print('E')
		random.shuffle(index)
		for i in index:
			primary_key = records[i][0]
			update = [None, None, None, None, None]
			update_bit = [False, False, False, False, False]
			# randomly select columns to update
			how_many_columns = random.randint(1, 5)
			for j in range(how_many_columns):
				update_bit[random.randint(0, 4)] = True
				
			for idx, do_update in enumerate(update_bit):
				if do_update:
					if idx == 0:
						update[idx] = records[i][idx] + 1000000000
					else:
						update[idx] = random.randint(-100000000, 100000000)
					records[i][idx] = update[idx]
			self.assertTrue(query.update(primary_key, *update))
			
			# test select after update
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = False
				for result in results:
					if result.columns == correct:
						ok = True
				self.assertTrue(ok)
			else:
				self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)

		db.close()

	"""
	Delete select
	"""
	def test_delete(self):
		db = Database()
		db.open('./database')
		table = db.create_table('delete', 5, 0)
		for i in range(1, 5):
			table.index.create_index(i)
		query = Query(table)

		records = []
		index = [i for i in range(1000)]
		random.shuffle(index)
		
		keys = random.sample(range(-100000000, 100000001), 1000)
		
		for key in keys:
			record = [key, random.randint(-100000000, 100000000), random.randint(-100000000, 100000000), random.randint(-100000000, 100000000), random.randint(-100000000, 100000000)]
			records.append(record)
			self.assertTrue(query.insert(*record))

		# randomly delete 2500 records
		random.shuffle(index)
		deleted_index = []
		for i in range(250):
			x = index.pop(0)
			correct = records[x]
			deleted_index.append(x)
			self.assertTrue(query.delete(records[x][0]))
			
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = True
				for result in results:
					if result.columns == correct:
						ok = False
				self.assertTrue(ok)

		for i in index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = False
				for result in results:
					if result.columns == correct:
						ok = True
				self.assertTrue(ok)
			else:
				self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)
	
		for i in deleted_index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = True
				for result in results:
					if result.columns == correct:
						ok = False
				self.assertTrue(ok)

		db.close()
		del db, table, query

		db = Database()
		db.open('./database')
		table = db.get_table('delete')
		query = Query(table)
		
		random.shuffle(index)
		random.shuffle(deleted_index)
		
		for i in index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = False
				for result in results:
					if result.columns == correct:
						ok = True
				self.assertTrue(ok)
			else:
				self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)
				
		for i in deleted_index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = True
				for result in results:
					if result.columns == correct:
						ok = False
				self.assertTrue(ok)

		# randomly delete 2500 records
		random.shuffle(index)
		deleted_index = []
		for i in range(250):
			x = index.pop(0)
			correct = records[x]
			deleted_index.append(x)
			self.assertTrue(query.delete(records[x][0]))
			
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = True
				for result in results:
					if result.columns == correct:
						ok = False
				self.assertTrue(ok)
		
		random.shuffle(index)
		random.shuffle(deleted_index)
		
		for i in index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = False
				for result in results:
					if result.columns == correct:
						ok = True
				self.assertTrue(ok)
			else:
				self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)
				
		for i in deleted_index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = True
				for result in results:
					if result.columns == correct:
						ok = False
				self.assertTrue(ok)
		
		db.close()
		del db, table, query
		
		db = Database()
		db.open('./database')
		table = db.get_table('delete')
		query = Query(table)
		
		random.shuffle(index)
		random.shuffle(deleted_index)
		
		for i in index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = False
				for result in results:
					if result.columns == correct:
						ok = True
				self.assertTrue(ok)
			else:
				self.assertEqual(query.select(correct[random_index], random_index, [1] * 5)[0].columns, correct)
				
		for i in deleted_index:
			correct = records[i]
			random_index = random.randint(0, 4)
			results = query.select(correct[random_index], random_index, [1] * 5)
			if len(results) > 0:
				# need to check one by one
				ok = True
				for result in results:
					if result.columns == correct:
						ok = False
				self.assertTrue(ok)

	def test_sum(self):
		db = Database()
		db.open('./database')
		table = db.create_table('sum', 5, 0)
		query = Query(table)
		
		records = []
		index = [i for i in range(1000)]
		random.shuffle(index)
		
		keys = random.sample(range(-100000000, 100000001), 1000)
		
		for key in keys:
			record = [key, random.randint(-100000000, 100000000), random.randint(-100000000, 100000000), random.randint(-100000000, 100000000), random.randint(-100000000, 100000000)]
			records.append(record)
			self.assertTrue(query.insert(*record))
		
		sum_res = 0
		random_index = random.randint(0, 4)
		left = random.randint(-100000000, -1)
		right = random.randint(1, 100000000)
		for i in index:
			if left <= records[i][0] <= right:
				sum_res += records[i][random_index]
	
		self.assertEqual(query.sum(left, right, random_index), sum_res)
		
		random.shuffle(index)
		# random update
		for i in range(random.randint(100, 200)):
			primary_key = records[i][0]
			update = [None, None, None, None, None]
			update_bit = [False, False, False, False, False]
			# randomly select columns to update
			how_many_columns = random.randint(1, 5)
			for j in range(how_many_columns):
				update_bit[random.randint(0, 4)] = True
				
			for idx, do_update in enumerate(update_bit):
				if do_update:
					if idx == 0:
						update[idx] = records[i][idx] + 1000000000
					else:
						update[idx] = random.randint(-100000000, 100000000)
					records[i][idx] = update[idx]
			self.assertTrue(query.update(primary_key, *update))
			
		sum_res = 0
		random_index = random.randint(0, 4)
		left = random.randint(-100000000, -1)
		right = random.randint(1, 100000000)
		for i in index:
			if left <= records[i][0] <= right:
				sum_res += records[i][random_index]
				
		self.assertEqual(query.sum(left, right, random_index), sum_res)
		
		random.shuffle(index)
		# random delete
		for i in range(random.randint(100, 200)):
			x = index.pop(0)
			self.assertTrue(query.delete(records[x][0]))
			
		sum_res = 0
		random_index = random.randint(0, 4)
		left = random.randint(-100000000, -1)
		right = random.randint(1, 100000000)
		for i in index:
			if left <= records[i][0] <= right:
				sum_res += records[i][random_index]
				
		self.assertEqual(query.sum(left, right, random_index), sum_res)
		
		db.close()
		del db, table, query
		
		db = Database()
		db.open('./database')
		table = db.get_table('sum')
		query = Query(table)
		
		random.shuffle(index)
		sum_res = 0
		random_index = random.randint(0, 4)
		left = random.randint(-100000000, -1)
		right = random.randint(1, 100000000)
		for i in index:
			if left <= records[i][0] <= right:
				sum_res += records[i][random_index]
				
		self.assertEqual(query.sum(left, right, random_index), sum_res)
		
		random.shuffle(index)
		# random delete
		for i in range(random.randint(1000, 2000)):
		for i in range(random.randint(100, 200)):
			x = index.pop(0)
			self.assertTrue(query.delete(records[x][0]))
			
		sum_res = 0
		random_index = random.randint(0, 4)
		left = random.randint(-100000000, -1)
		right = random.randint(1, 100000000)
		for i in index:
			if left <= records[i][0] <= right:
				sum_res += records[i][random_index]
				
		self.assertEqual(query.sum(left, right, random_index), sum_res)
	
	def test_multiple_tables(self):
		db = Database()
		db.open('./database')
		tables = {}
		for i in range(10):
			num_columns = random.randint(1, 10)
			table = db.create_table('table-{}'.format(i), num_columns, num_columns - 1)
			tables[i] = [num_columns, []]

		for i in range(10):
			records = []
			index = [i for i in range(1000)]
			random.shuffle(index)

			table = db.get_table('table-{}'.format(i))
			query = Query(table)
			keys = random.sample(range(-100000000, 100000001), 1000)
			
			for j in range(1000):
				record = []
				for k in range(tables[i][0]):
					if k == tables[i][0] - 1:
						record.append(keys[j])
					else:
						record.append(random.randint(-100000000, 100000000))
				
				self.assertTrue(query.insert(*record))
				records.append(record)
			
			for j in index:
				correct = records[j]
				random_index = tables[i][0] - 1
				results = query.select(correct[random_index], random_index, [1] * tables[i][0])
				self.assertEqual(query.select(correct[random_index], random_index, [1] * tables[i][0])[0].columns, correct)
			
			for z in index:
				primary_key = records[z][tables[i][0] - 1]
				update = [None] * tables[i][0]
				update_bit = [False] * tables[i][0]
				# randomly select columns to update
				how_many_columns = random.randint(1, tables[i][0])
				for j in range(how_many_columns):
					update_bit[random.randint(0, tables[i][0] - 1)] = True
					
				for idx, do_update in enumerate(update_bit):
					if do_update:
						if idx == tables[i][0] - 1:
							update[idx] = records[z][idx] + 1000000000
						else:
							update[idx] = random.randint(-100000000, 100000000)
						records[z][idx] = update[idx]
				self.assertTrue(query.update(primary_key, *update))
				
				# test select after update
				correct = records[j]
				random_index = tables[i][0] - 1
				results = query.select(correct[random_index], random_index, [1] * tables[i][0])
				self.assertEqual(query.select(correct[random_index], random_index, [1] * tables[i][0])[0].columns, correct)


if __name__ == '__main__':
	repeat = 3
	for i in range(repeat):
		print('{} test'.format(i))
		if not unittest.main(exit=False, verbosity=2).result.wasSuccessful():
			exit(1)
