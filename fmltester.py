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

class TestGarbage(unittest.TestCase):
	
	def setUp(self):
		try:
			shutil.rmtree('./database')
			shutil.rmtree('./wtf')
			print('deleted')
		except:
			pass
		
		
	def test_query_primary_key_last_column(self):
		print('starting')
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
			

	def test_query_sum(self):
		db = Database()
		db.open('wtf')
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
	
if __name__ == '__main__':
	unittest.main(verbosity=2)
	