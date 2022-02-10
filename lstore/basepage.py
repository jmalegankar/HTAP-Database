import time
from lstore.page import Page
from lstore.record import Record

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

	"""
	Debug Only
	"""

	def __str__(self):
		string = 'Current size: {}/512 records\n'.format(self.num_records)
		string += '=' * 15 + '\n'
		if self.num_records > 0:
			string += 'Indirection\t\tRID\t\t\t\tTime\t\t\tSchema\n'
			for i in range(self.num_records):
				for j in range(self.num_columns):
					if j < 2:
						string += '{:09d}\t\t'.format(self.get(i, j))
					elif j == 3:
						string += '{:b}\t\t'.format(self.get(i, j)).zfill(11) # 11-2=9
					else:
						string += '{}\t\t'.format(self.get(i, j))
				string += '\n'
		else:
			string += 'No Record\n'
		return string

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

		# quicker setter to access page set
	def set(self, rec_num, value, column):
		return self.phys_pages[column].set(rec_num, value)

	def get_and_set(self, rec_num, value, column):
		return self.phys_pages[column].get_and_set(rec_num, value)

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

	def get_all_cols(self, rec_num):
		return [self.get(rec_num, i) for i in range(self.num_columns)]

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


	# takes in a record and writes it
	# used to update meaning making a new tail record
	def update(self, base_rid, record: Record):
		assert len(record.columns) == self.num_user_columns

		self.phys_pages[0].write(base_rid)
		self.phys_pages[1].write(record.rid)
		self.phys_pages[2].write(int(time.time()))

		schema = 0
		for idx in range(self.num_user_columns):
			# creating a schema with bitwise operators of what is updated
			# note this is not a great way of creating a schema as it
			# limits the amount of columns
			# might want to refactor a way later.
			# rn since we have 64 bits can have 63 columns i believe but I def
			# want to change this logic later on but it works rn
			if record.columns[idx] is not None:
				schema = ( schema | (1 << self.num_user_columns-(idx+1)))
				self.phys_pages[idx + 4].write(record.columns[idx])
			else:
				self.phys_pages[idx + 4].write(0)
		self.phys_pages[3].write(schema)
		self.num_records += 1
		return schema

	def tail_update(self, previous_data, record: Record):
		assert len(record.columns) == self.num_user_columns

		self.phys_pages[0].write(previous_data[1]) # indirection is previous tail RID
		self.phys_pages[1].write(record.rid)
		self.phys_pages[2].write(int(time.time()))

		schema = previous_data[3]
		for index in range(self.num_user_columns):
			if record.columns[index] is not None:
				schema = (schema | (1 << self.num_user_columns - (index + 1)))
				self.phys_pages[index + 4].write(record.columns[index])
			else:
				self.phys_pages[index + 4].write(previous_data[index + 4])
		self.phys_pages[3].write(schema)
		self.num_records += 1
		return schema
