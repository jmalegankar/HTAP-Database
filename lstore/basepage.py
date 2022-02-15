import time
from lstore.page import Page
from lstore.record import Record
import lstore.bufferpool as bufferpool

class BasePage:

	def __init__(self, columns: int, path: str):
		assert columns > 0
		self.num_columns = columns + 4
		self.num_user_columns = columns
		self.num_records = 0
		self.path = path
#		print(path)
		"""
		4 Internal columns = 4 Physical pages for internal metadata
		first page is for the indirection col
		second page is for rid col
		third page is for timestamp col
		fourth page is for schema col
		The rest are the columns provided by the user
		"""
#		self.phys_pages = [Page() for i in range(self.num_columns)]

	"""
	Debug Only
	"""

	def __str__(self):
		string = 'Current size: {}/511 records\n'.format(self.num_records)
		string += '=' * 15 + '\n'
		return string

	"""
	PageRange needs to check has_capacity() before calling the write() function
	"""

	def has_capacity(self):
		return self.num_records < 511

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
		return bufferpool.shared.get_logical_pages(self.path, self.num_columns)[column].get(rec_num)
#		return self.phys_pages[column].get(rec_num)

	"""
	quicker setter to access page set
	"""

	def set(self, rec_num, value, column):
		return bufferpool.shared.get_logical_pages(self.path, self.num_columns)[column].set(rec_num, value)
#		return self.phys_pages[column].set(rec_num, value)

	def get_and_set(self, rec_num, value, column):
		return bufferpool.shared.get_logical_pages(self.path, self.num_columns)[column].get_and_set(rec_num, value)
#		return self.phys_pages[column].get_and_set(rec_num, value)

	"""
	Get multiple col
	rec_num: record number in integer
	ex: [0, 1, 1, 0] will get the second and third user columns
	"""

	def get_cols(self, rec_num, columns):
		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns)
		return [None if v == 0 else phys_pages[4 + i].get(rec_num) for i, v in enumerate(columns)]

	def get_user_cols(self, rec_num):
		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns)
		return [phys_pages[4 + i].get(rec_num) for i in range(self.num_user_columns)]

	def get_all_cols(self, rec_num):
		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns)
		return [phys_pages[i].get(rec_num) for i in range(self.num_columns)]

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

		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns)

		# Internal columns
		phys_pages[0].write(0) # indirection, default = ?
		phys_pages[1].write(record.rid) # rid, given by the PageRange
		phys_pages[2].write(int(time.time())) # time
		phys_pages[3].write(0) # schema, default = 0

		# User columns
		for idx in range(self.num_user_columns):
			phys_pages[idx + 4].write(record.columns[idx])
		self.num_records += 1


	# takes in a record and writes it
	# used to update meaning making a new tail record
	def update(self, base_rid, record: Record):
		assert len(record.columns) == self.num_user_columns

		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns)

		phys_pages[0].write(base_rid)
		phys_pages[1].write(record.rid)
		phys_pages[2].write(int(time.time()))

		schema = 0
		for idx in range(self.num_user_columns):
			if record.columns[idx] is not None:
				schema = (schema | (1 << self.num_user_columns-(idx+1)))
				phys_pages[idx + 4].write(record.columns[idx])
			else:
				phys_pages[idx + 4].write(0)

		phys_pages[3].write(schema)
		self.num_records += 1
		return schema

	def tail_update(self, previous_data, record: Record):
		assert len(record.columns) == self.num_user_columns

		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns)

		phys_pages[0].write(previous_data[1]) # indirection is previous tail RID
		phys_pages[1].write(record.rid)
		phys_pages[2].write(int(time.time()))

		schema = previous_data[3]
		for index in range(self.num_user_columns):
			if record.columns[index] is not None:
				schema = (schema | (1 << self.num_user_columns - (index + 1)))
				phys_pages[index + 4].write(record.columns[index])
			else:
				phys_pages[index + 4].write(previous_data[index + 4])
		phys_pages[3].write(schema)
		self.num_records += 1
		return schema
