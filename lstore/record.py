class Record:

	"""
	For query
	"""
	
	__slots__ = 'rid', 'key', 'columns'

	def __init__(self, rid, key, columns):
		self.rid = rid
		self.key = key
		self.columns = columns

	def __str__(self):
		return 'RID: {}, key: {}, data: {}'.format(self.rid, self.key, self.columns)
	
	def __repr__(self):
		return self.__str__()

	"""
	Overload [] operator
	"""
	def __getitem__(self, key):
		return self.columns[key]
	