from lstore.page import Page
import time

class BasePage:

	def __init__(self, columns: int):
		assert columns > 4
		self.num_columns = columns
		self.num_records = 0
		self.phys_pages = [Page() for i in range(columns)]


	def has_capacity(self):
		return self.num_records < 512


	def get(self, rec_num, column):
		return self.phys_pages[column].get(rec_num)


	def write(self, *values):
		assert len(values) == self.num_columns - 4
		"""
		Create new base page record
		Need to update page directory and get RID
		"""
		
		# Internal columns
		self.phys_pages[0].write(0) # indirection, default = ?
		self.phys_pages[1].write(0) # rid, need to calculate this
		self.phys_pages[2].write(int(time.time())) # time
		self.phys_pages[3].write(0) # schema, default = 0

		# User columns
		for idx in range(4, self.num_columns):
			self.phys_pages[idx].write(values[idx - 4])
		self.num_records += 1


	def update(self, column, value):
		pass
