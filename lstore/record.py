class Record:

	"""
	For query
	"""

	def __init__(self, rid, key, columns):
		self.rid = rid
		self.key = key
		self.columns = columns

	def __str__(self):
		return str(self.columns)