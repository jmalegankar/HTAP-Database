import unittest
from lstore.pageRange import PageRange
from lstore.basepage import BasePage
import time

class TestPages(unittest.TestCase):

	def test_one_basepage(self):
		start = time.time()
		basepage = BasePage(6) # create two columns

		# Test First Record
		basepage.write(123, 321) # set first column to 123 and second to 321
		self.assertEqual(basepage.get(0, 2), int(start)) # check internal column (timestamp)
		self.assertEqual(basepage.get(0, 4), 123)
		self.assertEqual(basepage.get(0, 5), 321)
		
		# Insert invalid record, should raise error
		with self.assertRaises(Exception):
			basepage.write(1)

		# Test 2nd to 512th
		for i in range(1, 512):
			basepage.write(i * 2, i * 3) # set first column to 123 and second to 321
			self.assertEqual(basepage.get(i, 2), int(start)) # check internal column (timestamp)
			self.assertEqual(basepage.get(i, 4), i * 2)
			self.assertEqual(basepage.get(i, 5), i * 3)

		# Page is full and insert new record, should raise error
		with self.assertRaises(Exception):
			basepage.write(1, 2)

if __name__ == '__main__':
	unittest.main()
