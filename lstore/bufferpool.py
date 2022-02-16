import pickle
import os
from collections import defaultdict
from lstore.config import BUFFERPOOL_SIZE
from lstore.page import Page
import time

class BufferpoolPage:

	__slots__ = 'pinned', 'pages', 'last_access', 'path'

	def __init__(self, path):
		self.pinned = 1
		self.pages = []
		self.last_access = time.time()
		self.path = path


class Bufferpool:

	__slots__ = 'path', 'logical_pages', 'bufferpool_size', 'logical_pages_directory'

	def __init__(self):
		self.start()
	
	def start(self):
		self.path = './database'
		self.logical_pages = [None] * BUFFERPOOL_SIZE # array of BufferpoolPage
		self.bufferpool_size = 0
		self.logical_pages_directory = dict() # map logical base/tail pages to index

	"""
	Set the database path
	"""

	def db_path(self, path):
		self.path = path
		self.create_folder('')

	"""
	Return True if database exists in the current path
	"""
	def db_exists(self):
		return os.path.exists(self.path + '/database.db')

	"""
	Create folder given relative path
	"""

	def create_folder(self, relative_path):
		os.makedirs(self.path + '/' + relative_path, exist_ok=True)

	"""
	Write a physical page to disk
	"""

	def write_page(self, relative_path, physical_page_number, data):
#		print('WRITE_PAGE ' + self.path + '/' + relative_path + '/' + str(physical_page_number) + '.db')
		with open(self.path + '/' + relative_path + '/' + str(physical_page_number) + '.db', 'wb', pickle.HIGHEST_PROTOCOL) as out_f:
			pickle.dump(data, out_f)

	"""
	Read a physical page from disk
	"""

	def read_page(self, relative_path, physical_page_number):
		with open(self.path + '/' + relative_path + '/' + str(physical_page_number) + '.db', 'rb', pickle.HIGHEST_PROTOCOL) as in_f:
			return pickle.load(in_f)
		return None

	"""
	Write metadata to disk
	"""

	def write_metadata(self, relative_path, data):
#		print('write_metadata at ' + relative_path)
#		print(data)
		with open(self.path + '/' + relative_path, 'wb', pickle.HIGHEST_PROTOCOL) as out_f:
			pickle.dump(data, out_f)

	"""
	Read metadata from disk
	"""

	def read_metadata(self, relative_path):
		with open(self.path + '/' + relative_path, 'rb', pickle.HIGHEST_PROTOCOL) as in_f:
			return pickle.load(in_f)
		return None

	"""
	Given path, return BufferpoolPage
	"""
	def get_logical_pages(self, path, num_columns, atomic=False, column=None, rec_num=None, set_to=None, set_and_get=False):
		try:
			if path in self.logical_pages_directory:
				# cache hit, already in bufferpool
				bufferpool_page = self.logical_pages[self.logical_pages_directory[path]]
				bufferpool_page.last_access = time.time()

				# No need to pin!
				if atomic:
					if set_to is not None:
						if set_and_get:
							return bufferpool_page.pages[column].get_and_set(rec_num, set_to)
						bufferpool_page.pages[column].set(rec_num, set_to)

						return
					return bufferpool_page.pages[column].get(rec_num)

				# TODO: M3 need to see if already pinned by transaction
				if bufferpool_page.pinned == 0:
					bufferpool_page.pinned += 1
				return bufferpool_page


			# need to bring in
			bufferpool_index = self.bufferpool_size
			if bufferpool_index == BUFFERPOOL_SIZE:
#				print('Need to kick someone out')
				# need to kick pages
				oldest_page = self.logical_pages[0]
				oldest_page_index = 0
				has_free_space = False

				for index, logical_page in enumerate(self.logical_pages):
					if logical_page.pinned <= 0: # not pinned
						has_free_space = True
						if logical_page.last_access < oldest_page.last_access:
							oldest_page_index = index
							oldest_page = logical_page
				
				if not has_free_space:
					print('ERROR: We are kicking out a pinned page!')
#					print(self.logical_pages_directory)
#					print(self.logical_pages)
					input()
				# kick out this logical page
				if oldest_page.path in self.logical_pages_directory:
					del self.logical_pages_directory[oldest_page.path]

#				print('Adios ' + oldest_page.path)
#				print('Hola ' + path)
				self.create_folder(oldest_page.path)

				# write to disk if dirty
				for physical_page_number in range(len(oldest_page.pages)):
					if oldest_page.pages[physical_page_number].dirty:
						oldest_page.pages[physical_page_number].close()
						self.write_page(oldest_page.path, physical_page_number, oldest_page.pages[physical_page_number].data)

				# we have free space now
				bufferpool_index = oldest_page_index
			else:
#				print('Welcome, no kicking ' + path)
				self.bufferpool_size += 1

			new_logical_page = BufferpoolPage(path)

			# page in database
			if os.path.isdir(self.path + '/' + path):
#				print('READ FROM FILE')
				for physical_page_number in range(num_columns):
					physical_page = Page()
					physical_page.open(self.read_page(path, physical_page_number))
					new_logical_page.pages.append(physical_page)
			else:
#				print('CREATE NEW PAGE')
				# page not in 
				new_logical_page.pages = [Page(dirty=True) for i in range(num_columns)]

			self.logical_pages[bufferpool_index] = new_logical_page
			self.logical_pages_directory[path] = bufferpool_index

			if atomic:
				self.logical_pages[bufferpool_index].pinned = 0

				if set_to is not None:
					if set_and_get:
						return self.logical_pages[bufferpool_index].pages[column].get_and_set(rec_num, set_to)
					self.logical_pages[bufferpool_index].pages[column].set(rec_num, set_to)
					return
				return self.logical_pages[bufferpool_index].pages[column].get(rec_num)
			return self.logical_pages[bufferpool_index]
		except ValueError as e:
			print(e)

	def unpin_all(self):
		pass

	def close(self):
		for page in self.logical_pages:
			if page is not None:
				self.create_folder(page.path)
				for physical_page_number in range(len(page.pages)):
#					print(page.pages[physical_page_number])
					if page.pages[physical_page_number].dirty:
						page.pages[physical_page_number].close()
						self.write_page(page.path, physical_page_number, page.pages[physical_page_number].data)


shared = Bufferpool()
