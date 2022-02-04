from lstore.page import Page
from lstore.table import Record
import time

class BasePage:

	def __init__(self, columns: int):
		assert columns > 0
		self.num_columns = columns + 4
		self.num_user_columns = columns
		self.num_records = 0
		"""
		4 Internal columns = 4 Physical pages for internal metadata
		first page is for the indirection col
		second page is for rid col
		third page is for timestamp col
		fourth page is for schema col
		The rest are the columns provided by the user
		"""
		self.phys_pages = [Page() for i in range(self.num_columns)]
	
	def __str__(self):
		return "Basepage > " + str(self.num_records) + " records"

	"""
	PageRange needs to check has_capacity() before calling the write() function
	"""

	def has_capacity(self):
		return self.num_records < 512

	"""
	Get next record number
	For page range to generate RID
	"""
	
	def get_next_rec_num(self):
		return self.num_records

	"""
	Get a record given page offset (rec_num) and column number (column, 0-3 are internal columns)
	The first user column is 4
	"""

	def get(self, rec_num, column):
		return self.phys_pages[column].get(rec_num)

	"""
	Get multiple col
	rec_num: record number in integer
	ex: [0, 1, 1, 0] will get the second and third user columns
	"""

	def get_cols(self, rec_num, columns):
		assert 0 <= rec_num < self.num_records and len(columns) == self.num_user_columns
		return [None if columns[i] == 0 else self.get(rec_num, 4 + i) for i in range(len(columns))]
	
	
	def get_user_cols(self, rec_num):
		return [self.get(rec_num, 4 + i) for i in range(self.num_user_columns)]

	"""
	Write a record to the physical page
	PageRange should call get_next_rec_num() to generate RID page offset
	Then init Record object, call this function with that object
	PageRange should also check has_capacity(), if this page is full, PageRange should create a new base/tail page
	"""

	def write(self, record: Record):
		assert self.has_capacity() and len(record.columns) == self.num_user_columns
		"""
		Create new base page record
		Need to update page directory and map primary key -> RID
		"""
		
		# Internal columns
		self.phys_pages[0].write(0) # indirection, default = ?
		self.phys_pages[1].write(record.rid) # rid, given by the PageRange
		self.phys_pages[2].write(int(time.time())) # time
		self.phys_pages[3].write(0) # schema, default = 0

		# User columns
		for idx in range(self.num_user_columns):
			self.phys_pages[idx + 4].write(record.columns[idx])
		self.num_records += 1


	def update(self, column, value):
		pass
		