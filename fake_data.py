import unittest
from lstore.pageRange import PageRange
from lstore.basepage import BasePage
from lstore.table import Record
from lstore.parser import get_physical_page_offset
import time

class TestPages(unittest.TestCase):

	def test_one_basepage(self):
		start = time.time()
		basepage = BasePage(2) # create two columns

		# Test First Record
		
		self.assertEqual(basepage.get_next_rec_num(), 0)
		basepage.write(Record(0, 0, [123, 321])) # set first column to 123 and second to 321
		self.assertEqual(basepage.get(0, 2), int(start)) # check internal column (timestamp)
		self.assertEqual(basepage.get(0, 4), 123)
		self.assertEqual(basepage.get(0, 5), 321)
		
		self.assertEqual(basepage.get_cols(0, [1, 0]), [123, None])
		self.assertEqual(basepage.get_cols(0, [0, 1]), [None, 321])
		self.assertEqual(basepage.get_cols(0, [1, 1]), [123, 321])
		
		# Insert invalid record, should raise error
		with self.assertRaises(Exception):
			basepage.write(Record(0, 0, [123]))

		# Test 2nd to 512th
		for i in range(1, 512):
			self.assertEqual(basepage.get_next_rec_num(), i)
			
			basepage.write(Record(i, 0, [i * 2, i * 3])) # set first column to 123 and second to 321
			self.assertEqual(basepage.get(i, 2), int(start)) # check internal column (timestamp)
			self.assertEqual(basepage.get(i, 4), i * 2)
			self.assertEqual(basepage.get(i, 5), i * 3)
			
			self.assertEqual(basepage.get_cols(i, [1, 0]), [i * 2, None])
			self.assertEqual(basepage.get_cols(i, [0, 1]), [None, i * 3])
			self.assertEqual(basepage.get_cols(i, [1, 1]), [i * 2, i * 3])

		# Page is full and insert new record, should raise error
		with self.assertRaises(Exception):
			basepage.write(Record(512, 0, [123, 321]))
	

	def test_parser(self):
		self.assertEqual(get_physical_page_offset(123456789), 789)

if __name__ == '__main__':
	unittest.main()
	