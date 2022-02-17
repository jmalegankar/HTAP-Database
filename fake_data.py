import unittest
from lstore.db import Database
from lstore.table import Table
from lstore.query import Query
from lstore.pageRange import PageRange
from lstore.basepage import BasePage
from lstore.record import Record
from lstore.page import Page
from lstore.parser import *
import time
import random
import shutil

class TestLowLevelStuffs(unittest.TestCase):

	def test_page(self):
		page = Page()
		page.write(1234567890)
		self.assertEqual(page.get(0), 1234567890)
		self.assertEqual(page.num_records, 1)
		page.close()
		
		data = page.data
		
		page2 = Page()
		self.assertEqual(page2.num_records, 0)

		page2.open(data)
		self.assertEqual(page2.get(0), 1234567890)
		self.assertEqual(page2.num_records, 1)
		page2.write(9876543210)
		
		self.assertEqual(page2.get(0), 1234567890)
		self.assertEqual(page2.get(1), 9876543210)
		self.assertEqual(page2.num_records, 2)
		
		self.assertEqual(page.get(0), 1234567890)
		self.assertEqual(page.num_records, 1)


class TestDatabase(unittest.TestCase):
	
	def setUp(self):
		try:
			shutil.rmtree('./database')
		except:
			pass

	def test_query_insert_select(self):
		try:
			shutil.rmtree('./query_insert_select')
		except:
			pass

		db = Database()
		db.open('./query_insert_select')

		grades_table = db.create_table('Grades', 5, 0)
		query = Query(grades_table)
		
		for i in range(5000):
			self.assertEqual(query.insert(i, i+1, i+2, i+3, i+4), True)
			
		for i in range(5000):
			results = query.select(i, 0, [1] * 5)
			self.assertEqual(len(results), 1)
			self.assertEqual(results[0].columns, [i, i+1, i+2, i+3, i+4])
		
		for i in range(50, 100):
			results = query.select(i+2, 2, [1] * 5)
			self.assertEqual(len(results), 1)
			self.assertEqual(results[0].columns, [i, i+1, i+2, i+3, i+4])
	
		query.update(50, None, None, None, None, 1234567890)
		self.assertEqual(query.select(50, 0, [1] * 5)[0].columns, [50, 51, 52, 53, 1234567890])
		self.assertEqual(query.select(51, 1, [1] * 5)[0].columns, [50, 51, 52, 53, 1234567890])
		self.assertEqual(query.select(52, 2, [1] * 5)[0].columns, [50, 51, 52, 53, 1234567890])
		self.assertEqual(query.select(53, 3, [1] * 5)[0].columns, [50, 51, 52, 53, 1234567890])
		self.assertEqual(query.select(1234567890, 4, [1] * 5)[0].columns, [50, 51, 52, 53, 1234567890])
		
		db.close()

	def test_query_insert_select_restarted(self):
		db = Database()
		db.open('./query_insert_select')

		grades_table = db.get_table('Grades')
		query = Query(grades_table)
		
		for i in range(5000):
			results = query.select(i, 0, [1] * 5)
			self.assertEqual(len(results), 1)
			if i == 50:
				self.assertEqual(results[0].columns, [50, 51, 52, 53, 1234567890])
			else:
				self.assertEqual(results[0].columns, [i, i+1, i+2, i+3, i+4])
		self.assertEqual(query.select(50, 0, [1] * 5)[0].columns, [50, 51, 52, 53, 1234567890])
		self.assertEqual(query.select(51, 1, [1] * 5)[0].columns, [50, 51, 52, 53, 1234567890])
		self.assertEqual(query.select(52, 2, [1] * 5)[0].columns, [50, 51, 52, 53, 1234567890])
		self.assertEqual(query.select(53, 3, [1] * 5)[0].columns, [50, 51, 52, 53, 1234567890])
		self.assertEqual(query.select(1234567890, 4, [1] * 5)[0].columns, [50, 51, 52, 53, 1234567890])

#		print('about to udpate!')
#		print(query.select(0, 0, [1]*5))
		self.assertTrue(query.insert(-1, -1, -1, -1, -1))
		self.assertTrue(query.update(0, None, None, -1, -1, -1))
		self.assertTrue(query.update(1, None, None, 2, None, 3))
		
		self.assertTrue(query.select(0, 0, [1] * 5)[0].columns, [0, 1, -1, -1, -1])
		self.assertTrue(query.select(1, 0, [1] * 5)[0].columns, [1, 2, 2, 3, 3])
#		print(query.select(0, 0, [1]*5))
		db.close()

		new_db = Database()
		new_db.open('./query_insert_select')
		new_grades_table = new_db.get_table('Grades')
		new_query = Query(new_grades_table)
		
		new_result = new_query.select(0, 0, [1] * 5)
		self.assertEqual(len(new_result), 1)
		self.assertEqual(new_result[0].columns, [0, 1, -1, -1, -1])
		
		new_result = new_query.select(-1, 0, [1] * 5)
		self.assertEqual(len(new_result), 1)
		self.assertEqual(new_result[0].columns, [-1, -1, -1, -1, -1])
	
		self.assertTrue(new_query.select(1, 0, [1] * 5)[0].columns, [1, 2, 2, 3, 3])
		
		new_db.close()

	def test_baby_query_insert(self):
		db = Database()
		db.open('./database')
		table = db.create_table('baby_query_insert', 5, 0)
		query = Query(table)

		self.assertTrue(query.insert(1, 2, 3, 4, 5))
		self.assertEqual(query.select(1, 0, [1, 1, 1, 1, 1])[0].columns, [1, 2, 3, 4, 5])
		self.assertTrue(query.update(1, None, 200, 300, 400, 500))

		db.close()

	def test_parser(self):
		self.assertEqual(get_physical_page_offset(123456789), 789)
		self.assertEqual(get_page_type(123456789), 1)
		self.assertEqual(get_page_range_number(123456789), 23)
		self.assertEqual(get_page_number(123456789), 456)
		
		self.assertEqual(get_page_type(create_rid(1, 0, 0, 0)), 1)
		self.assertEqual(get_page_range_number(create_rid(0, 23, 0, 0)), 23)
		self.assertEqual(get_page_number(create_rid(0, 0, 456, 0)), 456)
		self.assertEqual(get_physical_page_offset(create_rid(0, 0, 0, 789)), 789)
		self.assertEqual(create_rid(1, 23, 456, 789), 123456789)


	def test_query_delete(self):
		db = Database()
		db.create_table('Test', 3, 0)
		table = db.get_table('Test')
		query = Query(table)
		
		keys = random.sample(range(-100000000, 100000000), 10000)
		
		# insert
		for key in keys:
			self.assertEqual(query.insert(key, key * 2, key * 3), True)
		
		# select should return a valid record
		for key in keys:
			self.assertEqual(query.select(key, 0, [1, 1, 1])[0].columns, [key, key * 2, key * 3])
		
		# delete should return True
		for key in keys:
			self.assertEqual(query.delete(key), True)
		
		# delete a invalid key should return False
		self.assertEqual(query.delete(1000000001), False)
		
		# select deleted keys should return None
		for key in keys:
			self.assertEqual(query.select(key, 0, [1, 1, 1]), [])
		
		# delete deleted keys should return False
		for key in keys:
			self.assertEqual(query.delete(key), False)

	def test_query_update(self):
		db = Database()
		db.create_table('Test', 3, 0)
		table = db.get_table('Test')
		query = Query(table)
		
		self.assertFalse(query.update(0, 1, 1, 1))
		
		self.assertTrue(query.insert(1, 2, 3))
		self.assertEqual(query.select(1, 0, [1, 1, 1])[0].columns, [1, 2, 3])
		
		self.assertFalse(query.update(0, 1, 1, 1))
		
		self.assertTrue(query.update(1, None, 9, None))
		self.assertEqual(query.select(1, 0, [1, 1, 1])[0].columns, [1, 9, 3])
		
		self.assertTrue(query.update(1, None, None, 10))
		self.assertEqual(query.select(1, 0, [1, 1, 1])[0].columns, [1, 9, 10])
		
		self.assertTrue(query.increment(1, 1))
		self.assertEqual(query.select(1, 0, [1, 1, 1])[0].columns, [1, 10, 10])
	
		self.assertTrue(query.update(1, 8, None, None))
		self.assertEqual(query.select(8, 0, [1, 1, 1])[0].columns, [8, 10, 10])
		self.assertEqual(query.select(1, 0, [1, 1, 1]), [])

	def test_query_update_after_update(self):
		db = Database()
		db.create_table('Test', 3, 0)
		table = db.get_table('Test')
		query = Query(table)
		
		self.assertFalse(query.update(0, 1, 1, 1))
		
		self.assertTrue(query.insert(1, 2, 3))
		self.assertEqual(query.select(1, 0, [1, 1, 1])[0].columns, [1, 2, 3])
		self.assertTrue(query.update(1, None, None, 1))
		self.assertEqual(query.select(1, 0, [1, 1, 1])[0].columns, [1, 2, 1])
		self.assertEqual(query.delete(1), True)
		self.assertFalse(query.update(1, None, None, 1))
		self.assertTrue(query.insert(1, 2, 3))
		self.assertTrue(query.insert(2, 2, 3))
		self.assertTrue(query.insert(3, 2, 3))
		self.assertTrue(query.insert(4, 2, 3))
		self.assertEqual(query.delete(1), True)

		
		self.assertEqual(query.sum(1, 4, 1), 6)
		self.assertEqual(query.sum(1, 4, 2), 9)

	def test_query_sum(self):
		db = Database()
		db.create_table('Test', 3, 0)
		table = db.get_table('Test')
		query = Query(table)
		
		keys = random.sample(range(-100000000, 100000000), 10000)
		
		sum_first_col = 0
		sum_second_col = 0
		sum_third_col = 0
		
		for key in keys:
			self.assertEqual(query.insert(key, key * 2, key * 3), True)
			if -100000 <= key <= 100000:
				sum_first_col += key
				sum_second_col += key * 2
				sum_third_col += key * 3
		
		self.assertEqual(query.sum(-100000, 100000, 0), sum_first_col)
		self.assertEqual(query.sum(-100000, 100000, 1), sum_second_col)
		self.assertEqual(query.sum(-100000, 100000, 2), sum_third_col)
		
		table2 = db.create_table('Test2', 3, 0)
		query2 = Query(table2)
		
		self.assertEqual(query2.insert(100, 200, 300), True)
		self.assertEqual(query2.insert(1000, 2000, 3000), True)
		self.assertEqual(query2.sum(100, 1000, 2), 3300)
		
		self.assertEqual(query2.update(1000, None, 1000, None), True)
		self.assertEqual(query2.sum(100, 1000, 2), 3300)
		
		self.assertEqual(query2.update(1000, None, None, 2000), True)
		self.assertEqual(query2.sum(100, 1000, 2), 2300)
		
		self.assertEqual(query2.update(1000, 3000, None, None), True)
		self.assertEqual(query2.sum(100, 1000, 2), 300)
		
		self.assertEqual(query2.sum(100, 3000, 2), 2300)
	
	def test_select_duplicate(self):
		db = Database()
		db.create_table('Test', 3, 0)
		table = db.get_table('Test')
		query = Query(table)
		
		self.assertEqual(query.insert(0, 1000, 2000), True)
		self.assertEqual(query.insert(1, 100, 200), True)
		self.assertEqual(query.insert(1, 100, 200), False)
		self.assertEqual(query.insert(2, 100, 300), True)
		self.assertEqual(query.insert(3, 300, 3000), True)
		
		records = query.select(100, 1, [1, 1, 1])
		self.assertEqual(len(records), 2)
		
		for record in records:
			self.assertTrue(record.columns == [1, 100, 200] or record.columns == [2, 100, 300])
		
		self.assertEqual(query.update(3, None, 100, None), True)
		
		records = query.select(100, 1, [1, 1, 1])
		self.assertEqual(len(records), 3)
		
		for record in records:
			if record.rid == 1:
				self.assertEqual(record.columns, [1, 100, 200])
			elif record.rid == 2:
				self.assertEqual(record.columns, [2, 100, 300])
			else:
				self.assertEqual(record.columns, [3, 100, 3000])
		
		self.assertEqual(query.sum(1, 3, 1), 300)
		self.assertEqual(query.sum(1, 3, 2), 3500)
		
		
		
		records = query.select(4, 0, [1, 1, 1])
		self.assertEqual(len(records), 0)
		
		records = query.select(5, 0, [1, 1, 1])
		self.assertEqual(len(records), 0)
		
		self.assertEqual(query.insert(4, 44, 444), True)
		
		records = query.select(4, 0, [1, 1, 1])
		self.assertEqual(len(records), 1)
		
		self.assertNotEqual(query.sum(1, 4, 2), 3500)
		
		
		self.assertEqual(query.update(4, 5, None, None), True)
		
		records = query.select(4, 0, [1, 1, 1])
		self.assertEqual(len(records), 0)
		
		records = query.select(5, 0, [1, 1, 1])
		self.assertEqual(len(records), 1)
		
		self.assertEqual(query.sum(1, 3, 1), 300)
		self.assertEqual(query.sum(1, 3, 2), 3500)
		self.assertEqual(query.sum(1, 4, 2), 3500)
		self.assertNotEqual(query.sum(1, 5, 2), 3500)
		self.assertEqual(query.sum(1, 5, 2), 3500 + 444)
		
		self.assertEqual(query.update(5, None, None, None), True)
		self.assertEqual(query.sum(1, 3, 2), 3500)
		self.assertEqual(query.sum(1, 4, 2), 3500)
		self.assertNotEqual(query.sum(1, 5, 2), 3500)
		self.assertEqual(query.sum(1, 5, 2), 3500 + 444)
		
		self.assertEqual(query.update(5, None, None, 12345), True)
		self.assertEqual(query.sum(1, 5, 2), 3500 + 12345)
		
	
	def test_select_duplicate(self):
		db = Database()
		db.create_table('Test', 3, 0)
		table = db.get_table('Test')
		query = Query(table)
		query.insert(1, 2, 3)
		query.insert(4, 5, 6)
		
		self.assertFalse(query.update(1, 4, None, None))
		self.assertTrue(query.update(1, 7, None, None))
		
		self.assertFalse(query.insert(7, 8, 9))
		self.assertTrue(query.insert(-10, -11, -12))
		
		self.assertFalse(query.update(-10, 7, None, None))
		self.assertFalse(query.update(-10, 4, 1, 1))
		self.assertFalse(query.update(4, -10, None, None))
		self.assertTrue(query.update(4, -11, None, None))
		self.assertTrue(query.update(-10, 4, 1, 1))
		self.assertTrue(query.update(-11, -10, None, None))
	
		self.assertEqual(query.select(-10, 0, [1,1,1])[0].columns, [-10,5,6])
		self.assertEqual(query.select(4, 0, [1,1,1])[0].columns, [4,1,1])
		self.assertEqual(query.select(7, 0, [1,1,1])[0].columns, [7,2,3])
	
	def test_query_primary_key_last_column(self):
		db = Database()
		db.create_table('Test', 5, 4)
		table = db.get_table('Test')
		query = Query(table)
		
		for i in range(10000):
			self.assertTrue(query.insert(i - 4, i - 3, i - 2, i - 1, i))
			
		correct_sum = 0
		for i in range(5000, 6001):
			correct_sum += i - 2
		test_sum = 0
		for i in range(5000, 6001):
			test_sum += query.select(i, 4, [0, 0, 1, 0, 0])[0][2]
		self.assertEqual(test_sum, correct_sum)
		self.assertEqual(query.sum(5000, 6000, 2), correct_sum)
		
		for i in range(10000):
			self.assertEqual(query.select(i, 4, [1,1,1,1,1])[0].columns, [i - 4, i - 3, i - 2, i - 1, i])
		
		for i in range(1, 10000):
			self.assertTrue(query.update(i, None, None, None, None, i * -1))
	
		for i in range(1, 10000):
			self.assertEqual(query.select(i * -1, 4, [1,1,1,1,1])[0].columns, [i - 4, i - 3, i - 2, i - 1, i * -1])
		
		test_sum = 0
		for i in range(5000, 6001):
			test_sum += query.select(i * -1, 4, [0, 0, 1, 0, 0])[0][2]
		self.assertEqual(test_sum, correct_sum)
		self.assertEqual(query.sum(-6000, -5000, 2), correct_sum)
		
		for i in range(1, 10000):
			self.assertTrue(query.update(i * -1, 1, 2, None, 4, i))
		
		self.assertEqual(query.sum(1, 9999, 0), 9999)
		
		self.assertEqual(query.sum(0, 0, 2), -2)
		
		for i in range(10000):
			self.assertTrue(query.delete(i))
	
		self.assertEqual(query.sum(1, 9999, 0), 0)
		
		for i in range(10000):
			self.assertEqual(len(query.select(i, 4, [1,1,1,1,1])), 0)
		
	def test_sql_select(self):
		db = Database()
		db.create_table('Test', 3, 0)
		table = db.get_table('Test')
		query = Query(table)

		self.assertTrue(query.sql('INSERT VALUES (-1,2,3)'))
		self.assertEqual(len(query.sql('SELECT * WHERE 0=-1')), 1)

		self.assertTrue(query.sql('UPDATE 1 = -1, 2 = -1 WHERE -1'))
		self.assertEqual(query.sql('SELECT * WHERE 0=-1')[0].columns, [-1, -1, -1])

		self.assertTrue(query.sql('UPDATE 1 = 100 WHERE -1'))
		self.assertEqual(query.sql('SELECT * WHERE 0=-1')[0].columns, [-1, 100, -1])
		
		self.assertTrue(query.sql('UPDATE 0 = 1 WHERE -1'))
		self.assertEqual(len(query.sql('SELECT * WHERE 0=-1')), 0)
		self.assertEqual(len(query.sql('SELECT * WHERE 0=1')), 1)
		self.assertEqual(query.sql('SELECT * WHERE 0 = 1')[0].columns, [1, 100, -1])
		
		self.assertTrue(query.sql('DELETE 1'))
		self.assertEqual(len(query.sql('SELECT * WHERE 0=1')), 0)

	def test_everything(self):
		try:
			shutil.rmtree('./test_everything')
		except:
			pass

		db = Database()
		db.open('./test_everything')

		table1 = db.create_table('table1', 3, 0)
		query1 = Query(table1)
		
		self.assertTrue(query1.insert(1, 2, 3))
		self.assertTrue(query1.insert(4, 5, 6))
		self.assertEqual(query1.select(1, 0, [1,1,1])[0].columns, [1, 2, 3])
		self.assertEqual(query1.select(4, 0, [1,1,1])[0].columns, [4, 5, 6])

		db.close()

		del db
		del table1
		del query1
		
		db = Database()
		db.open('./test_everything')

		table1 = db.get_table('table1')
		query1 = Query(table1)
		
		self.assertEqual(query1.select(1, 0, [1,1,1])[0].columns, [1, 2, 3])
		self.assertEqual(query1.select(4, 0, [1,1,1])[0].columns, [4, 5, 6])
		
		db.close()
"""
"""
if __name__ == '__main__':
	unittest.main(verbosity=2)
	