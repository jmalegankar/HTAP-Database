from collections import defaultdict
import time
from threading import Lock

class LockManagerLock:

	__slots__ = 'rid', 'locked_by', 'owned_by'

	def __init__(self, rid):
		self.rid = rid
		self.locked_by = set()
		# It is a shared lock if owned_by == None
		self.owned_by = None

	def __str__(self):
		string = 'LockManagerLock\n'
		if self.owned_by is None:
			string += 'RID: {}, Shared By: {}\n'.format(self.rid, self.locked_by)
		else:
			string += 'RID: {}, Owned By: {}\n'.format(self.rid, self.owned_by)
		return string

	def __repr__(self):
		return self.__str__()

	def lock(self, tid):
		if self.owned_by is None: # is shared lock
			self.locked_by.add(tid)
			return True
		else:
			return tid == self.owned_by

	def upgrade(self, tid):
		if self.owned_by is None: # is shared lock
			locked_by_len = len(self.locked_by)
			if locked_by_len > 1:
				# locked by more than 1 tid
				return False
			elif locked_by_len == 1:
				if tid in self.locked_by:
					self.locked_by.remove(tid)
				else:
					# locked by some other tid
					return False

			self.owned_by = tid
			return True
		else:
			return tid == self.owned_by

	def unlock(self, tid):
		if self.owned_by is None: # is shared lock
			if tid in self.locked_by:
				self.locked_by.remove(tid)
				return True
		else:
			if tid == self.owned_by:
				self.owned_by = None
				return True
		return False


class LockManagerDict(dict):

	def __missing__(self, key):
		self[key] = LockManagerLock(key)
		return self[key]


class LockManager:

	__slots__ = 'latch', 'locks'

	def __init__(self):
		self.start()

	def __str__(self):
		string = 'LockManager\n'
		string += '====================\n'
		for lock in self.locks:
			string += str(self.locks[lock]) + '\n'
		return string
	
	def __repr__(self):
		return self.__str__()

	"""
	Database().open() will call this function to start the lock manager
	"""

	def start(self):
		self.latch = Lock()
		self.locks = LockManagerDict()

	"""
	TID locks BASE RID
	"""

	def lock(self, tid, rid):
		self.latch.acquire()
		result = self.locks[rid].lock(tid)
		self.latch.release()
		return result

	"""
	TID unlock BASE RID
	"""

	def unlock(self, tid, rid):
		self.latch.acquire()
		result = self.locks[rid].unlock(tid)
		self.latch.release()
		return result

	"""
	TID upgrade BASE RID to X lock
	"""

	def upgrade(self, tid, rid):
		self.latch.acquire()
		result = self.locks[rid].upgrade(tid)
		self.latch.release()
		return result

shared = LockManager()


if __name__ == '__main__':
	print(shared.lock(0, 12345))
	print(shared.lock(0, 1234))
	print(shared.lock(1, 1234))
	print(shared.lock(1, 12345))
	print(shared.upgrade(2, 888))
	print(shared.upgrade(2, 12345))
	print(shared.lock(3, 123456))
	print(shared.upgrade(3, 123456))
	print(shared)