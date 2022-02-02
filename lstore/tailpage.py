from lstore.page import Page

class TailPage:

	def __init__(self, columns: int):
		assert columns > 4
		self.num_columns = columns
		self.num_records = 0
		self.phys_pages = [Page() for i in range(columns)]


	def get(self):
		pass


	def write(self):
		pass


	def update(self):
		pass
		