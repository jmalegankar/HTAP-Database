import time
from lstore.page import Page
from lstore.record import Record
import lstore.bufferpool as bufferpool
from threading import Lock

class BasePage:

	"""
	4 Internal columns = 4 Physical pages for internal metadata
	first page is for the indirection col
	second page is for rid col
	third page is for timestamp col
	fourth page is for schema col
	* for Tail, fifth page is for BaseRID col *
	The rest are the columns provided by the user
	"""

	__slots__ = ('num_columns', 'num_user_columns', 'path', 'num_records', 'tps', 'num_updates',
		'num_meta_columns', 'latch')

	def __init__(self, columns: int, path: str, num_records=0, tps=-1, num_updates=0):
		assert columns > 0

		if tps == -1:
			self.num_columns = columns + 5 # Total number of columns (including internal columns)
		else:
			self.num_columns = columns + 4

		self.num_user_columns = columns
		self.path = path # The location of this page in disk
		self.num_records = num_records # Number of records
		self.tps = tps # Tail-Page Sequence Number
		self.num_updates = num_updates
		self.num_meta_columns = 5 if tps == -1 else 4 # 5 is tail, 4 is base
		self.latch = Lock()

	def __getstate__(self):
		return (self.num_columns, self.num_user_columns, self.path, self.num_records, self.tps, self.num_updates, self.num_meta_columns)

	def __setstate__(self, data):
		self.num_columns = data[0]
		self.num_user_columns = data[1]
		self.path = data[2]
		self.num_records = data[3]
		self.tps = data[4]
		self.num_updates = data[5]
		self.num_meta_columns = data[6]
		self.latch = Lock()

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
		self.latch.acquire()
		tps = self.tps
		self.latch.release()
		return bufferpool.shared.get_logical_pages(self.path, self.num_columns, tps, True, column, rec_num)

	"""
	*** WARNING: CALLERS MUST UNPIN THE PAGE ***
	Get a record given page offset and column number, return value and bufferpool object
	"""

	def get_bp(self, rec_num, column):
		self.latch.acquire()
		tps = self.tps
		self.latch.release()
		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns, tps)
		return phys_pages.pages[column].get(rec_num), phys_pages

	def get_bp_only(self):
		self.latch.acquire()
		tps = self.tps
		self.latch.release()
		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns, tps)
		return phys_pages

	"""
	Quicker setter to access page set
	"""

	def set(self, rec_num, value, column):
		self.latch.acquire()
		tps = self.tps
		self.latch.release()
		return bufferpool.shared.get_logical_pages(self.path, self.num_columns, tps, True, column, rec_num, value)

	def get_and_set(self, rec_num, value, column):
		self.latch.acquire()
		tps = self.tps
		self.latch.release()
		return bufferpool.shared.get_logical_pages(self.path, self.num_columns, tps, True, column, rec_num, value, True)

	def set_and_save(self, rec_num, value, column):
		self.latch.acquire()
		tps = self.tps
		self.latch.release()

		bufferpool.shared.create_folder(self.path)
		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns, tps)
		phys_pages.pages[column].set(rec_num, value)

		for physical_page_number in range(self.num_columns):
			if phys_pages.pages[physical_page_number].dirty:
				read_version = 0 if physical_page_number < 4 else phys_pages.version
				bufferpool.shared.write_page(
					phys_pages.path,
					physical_page_number,
					phys_pages.pages[physical_page_number].data,
					read_version
				)

		phys_pages.lock.acquire()
		phys_pages.pinned -= 1 # Finished using, unpin the page
		phys_pages.lock.release()


	"""
	Get multiple columns
	rec_num: record number in integer
	ex: [0, 1, 1, 0] will get the second and third user columns
	"""

	def get_cols(self, rec_num, columns):
		self.latch.acquire()
		tps = self.tps
		self.latch.release()
		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns, tps)
		result = [None if v == 0 else phys_pages.pages[self.num_meta_columns + i].get(rec_num) for i, v in enumerate(columns)]
		phys_pages.lock.acquire()
		phys_pages.pinned -= 1 # Finished using, unpin the page
		phys_pages.lock.release()
		return result

	"""
	Get multiple columns and a column with bufferpool object
	"""
	
	def get_cols_and_col(self, rec_num, columns, column):
		self.latch.acquire()
		tps = self.tps
		self.latch.release()
		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns, tps)
		result = [None if v == 0 else phys_pages.pages[self.num_meta_columns + i].get(rec_num) for i, v in enumerate(columns)]
		additional = phys_pages.pages[column].get(rec_num)
		phys_pages.lock.acquire()
		phys_pages.pinned -= 1 # Finished using, unpin the page
		phys_pages.lock.release()
		return result, additional

	def get_user_cols(self, rec_num):
		self.latch.acquire()
		tps = self.tps
		self.latch.release()
		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns, tps)
		result =  [phys_pages.pages[self.num_meta_columns + i].get(rec_num) for i in range(self.num_user_columns)]
		phys_pages.lock.acquire()
		phys_pages.pinned -= 1 # Finished using, unpin the page
		phys_pages.lock.release()
		return result

	def get_all_cols(self, rec_num):
		self.latch.acquire()
		tps = self.tps
		self.latch.release()
		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns, tps)
		result =  [phys_pages.pages[i].get(rec_num) for i in range(self.num_columns)]
		phys_pages.lock.acquire()
		phys_pages.pinned -= 1 # Finished using, unpin the page
		phys_pages.lock.release()
		return result

	# To rebuild index, no lock, only call in the main thread
	def get_metadata_cols(self, rec_num):
		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns, self.tps)
		result = [phys_pages.pages[i].get(rec_num) for i in range(4)]
		phys_pages.pinned -= 1
		return result

	"""
	Write a record to the physical page
	PageRange should call get_next_rec_num() to generate RID page offset
	Then init Record object, call this function with that object
	PageRange should also check has_capacity(), if this page is full, PageRange should create a new base/tail page
	"""

	def write(self, offset, record: Record, from_transaction=False):
		assert len(record.columns) == self.num_user_columns
		"""
		Create new base page record
		Need to update page directory and map primary key -> RID
		"""

		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns, 0)

		# Internal columns
		phys_pages.pages[0].set(offset, 0) # indirection, default = 0
		phys_pages.pages[1].set(offset, record.rid) # rid, given by the PageRange
		if not from_transaction:
			phys_pages.pages[2].set(offset, int(time.time())) # time
		phys_pages.pages[3].set(offset, 0) # schema, default = 0

#		print(offset, record.columns)

		# User columns
		for idx in range(self.num_user_columns):
			phys_pages.pages[idx + 4].set(offset, record.columns[idx])
		
#		print(offset, record.columns)
		phys_pages.lock.acquire()
		phys_pages.pinned -= 1 # Finished using, unpin the page
		phys_pages.lock.release()


	# takes in a record and writes it
	# used to update meaning making a new tail record

	def update(self, offset, base_rid, record: Record):
		assert len(record.columns) == self.num_user_columns

		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns, 0)

		phys_pages.pages[0].set(offset, base_rid)
		phys_pages.pages[1].set(offset, record.rid)
		phys_pages.pages[2].set(offset, int(time.time()))

		schema = 0
		for idx in range(self.num_user_columns):
			if record.columns[idx] is not None:
				schema = (schema | (1 << self.num_user_columns-(idx+1)))
				phys_pages.pages[idx + 5].set(offset, record.columns[idx])
			else:
				phys_pages.pages[idx + 5].set(offset, 0)

		phys_pages.pages[3].set(offset, schema)
		phys_pages.pages[4].set(offset, base_rid)

		phys_pages.lock.acquire()
		phys_pages.pinned -= 1 # Finished using, unpin the page
		phys_pages.lock.release()
		return schema

	def tail_update(self, offset, base_rid, previous_data, record: Record):
		assert len(record.columns) == self.num_user_columns

		phys_pages = bufferpool.shared.get_logical_pages(self.path, self.num_columns, 0)

		phys_pages.pages[0].set(offset, previous_data[1]) # indirection is previous tail RID
		phys_pages.pages[1].set(offset, record.rid)
		phys_pages.pages[2].set(offset, int(time.time()))

		schema = previous_data[3]
		for index in range(self.num_user_columns):
			if record.columns[index] is not None:
				schema = (schema | (1 << self.num_user_columns - (index + 1)))
				phys_pages.pages[index + 5].set(offset, record.columns[index])
			else:
				phys_pages.pages[index + 5].set(offset, previous_data[index + 5])

		phys_pages.pages[3].set(offset, schema)
		phys_pages.pages[4].set(offset, base_rid)
		
		phys_pages.lock.acquire()
		phys_pages.pinned -= 1 # Finished using, unpin the page
		phys_pages.lock.release()
		return schema
	